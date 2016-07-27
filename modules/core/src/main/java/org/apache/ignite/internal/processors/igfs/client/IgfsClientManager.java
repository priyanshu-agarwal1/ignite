/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.igfs.client;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsImpl;
import org.apache.ignite.internal.processors.igfs.IgfsManager;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Manager to handle IGFS client closures.
 */
public class IgfsClientManager extends IgfsManager {
    /** Outgoing operations. */
    private final Map<Long, IgfsClientOutOperation> outOps = new ConcurrentHashMap<>();

    /** Marshaller. */
    private final Marshaller marsh;

    /** RW lock for synchronization. */
    private final StripedCompositeReadWriteLock rwLock =
        new StripedCompositeReadWriteLock(Runtime.getRuntime().availableProcessors() * 2);

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new DiscoveryListener();

    /** IO message listener. */
    private final MessageListener msgLsnr = new MessageListener();

    /** Pending input operations received when manager is not started yet. */
    private final ConcurrentLinkedDeque<IgfsClientInOperation> pendingOps = new ConcurrentLinkedDeque<>();

    /** Worker to process pending requests. */
    private PendingRequestsWorker pendingWorker;

    /** Whether manager is fully started and ready to process requests. */
    private volatile boolean ready;

    /** Stopping flag. */
    private volatile boolean stopping;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public IgfsClientManager(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.config().getMarshaller();
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        ctx.io().addMessageListener(GridTopic.TOPIC_IGFS_CLI, msgLsnr);

        ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        rwLock.writeLock().lock();

        try {
            ready = true;

            if (!pendingOps.isEmpty()) {
                pendingWorker = new PendingRequestsWorker(ctx.gridName(), "igfs-client-pending-request-worker", log);

                new IgniteThread(pendingWorker).start();
            }
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        ctx.event().removeLocalEventListener(discoLsnr);

        ctx.io().removeMessageListener(GridTopic.TOPIC_IGFS_CLI, msgLsnr);

        PendingRequestsWorker pendingWorker0;

        rwLock.writeLock().lock();

        try {
            stopping = true;

            pendingWorker0 = pendingWorker;
        }
        finally {
            rwLock.writeLock().unlock();
        }

        if (pendingWorker0 != null) {
            U.cancel(pendingWorker0);
            U.join(pendingWorker0, log);
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        pendingOps.clear();
        outOps.clear();
    }

    /**
     * Execute IGFS closure.
     *
     * @param igfsCtx IGFS context.
     * @param clo Closure.
     * @return Result.
     */
    public <T> T execute(IgfsContext igfsCtx, IgfsClientAbstractCallable<T> clo) throws IgniteCheckedException {
        return execute(igfsCtx, clo, IgfsClientNodeSelectionStrategy.RANDOM);
    }

    /**
     * Execute IGFS closure.
     *
     * @param igfsCtx IGFS context.
     * @param clo Closure.
     * @param strategy Node selection strategy.
     * @return Result.
     */
    public <T> T execute(IgfsContext igfsCtx, IgfsClientAbstractCallable<T> clo,
        IgfsClientNodeSelectionStrategy strategy) throws IgniteCheckedException {
        return executeAsync(igfsCtx, clo, strategy).get();
    }

    /**
     * Execute IGFS closure asynchronously.
     *
     * @param igfsCtx IGFS context.
     * @param clo Closure.
     * @return Future.
     */
    public <T> IgniteInternalFuture<T> executeAsync(IgfsContext igfsCtx, IgfsClientAbstractCallable<T> clo) {
        return executeAsync(igfsCtx, clo, IgfsClientNodeSelectionStrategy.RANDOM);
    }

    /**
     * Execute IGFS closure asynchronously.
     *
     * @param igfsCtx IGFS context.
     * @param clo Closure.
     * @param strategy Node selection strategy.
     * @return Future.
     */
    public <T> IgniteInternalFuture<T> executeAsync(IgfsContext igfsCtx, IgfsClientAbstractCallable<T> clo,
        IgfsClientNodeSelectionStrategy strategy) {
        try {

            ClusterNode node = selectNode(igfsCtx, strategy);
        }
        catch (IgniteCheckedException e) {
            // TODO
        }

        // TODO

        return null;
    }

    /**
     * Select the most appropriate node for the task.
     *
     * @param igfsCtx IGFS context.
     * @param strategy Strategy.
     * @return Node.
     * @throws IgniteCheckedException If failed to find the node.
     */
    private ClusterNode selectNode(IgfsContext igfsCtx, IgfsClientNodeSelectionStrategy strategy)
        throws IgniteCheckedException {
        // TODO
    }

    /**
     * Create closure response.
     *
     * @param msgId Message ID.
     * @param res Response.
     * @return Response.
     */
    private IgfsClientResponse createResponse(long msgId, @Nullable Object res, @Nullable Throwable resErr) {
        try {
            if (resErr != null)
                return new IgfsClientResponse(msgId, IgfsClientResponseType.ERR, null,
                    marsh.marshal(resErr));
            else {
                if (res == null)
                    return new IgfsClientResponse(msgId, IgfsClientResponseType.NULL, null, null);
                else if (res instanceof Boolean)
                    return new IgfsClientResponse(msgId, IgfsClientResponseType.BOOL, res, null);
                else
                    return new IgfsClientResponse(msgId, IgfsClientResponseType.OBJ, null,
                        marsh.marshal(res));
            }
        }
        catch (Exception e) {
            U.error(log, "Failed to marshal IGFS closure result [msgId=" + msgId + ", res=" + res +
                ", resErr=" + resErr + ']', e);

            return new IgfsClientResponse(msgId, IgfsClientResponseType.MARSH_ERR, null, null);
        }
    }

    /**
     * Handle node leave event.
     *
     * @param nodeId Node ID.
     */
    private void onNodeLeft(UUID nodeId) {
        // TODO
    }

    /**
     * Handle request.
     *
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void onRequest(UUID nodeId, IgfsClientRequest req) {
        rwLock.readLock().lock();

        try {
            if (stopping)
                return; // Discovery listener on remote node will handle node leave.

            if (ready)
                processRequest(nodeId, req); // Normal execution flow.
            else
                // Add to pending set if manager is not operational yet.
                pendingOps.addLast(new IgfsClientInOperation(nodeId, req));
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Handle response.
     *
     * @param resp Response.
     */
    @SuppressWarnings("unchecked")
    private void onResponse(IgfsClientResponse resp) {
        rwLock.readLock().lock();

        try {
            IgfsClientOutOperation op = outOps.remove(resp.messageId());

            // Op might be null in case of concurreny local node stop or remote node stop.= discovery notification.
            if (op != null) {
                // Restore result.
                Object res = null;
                Throwable err = null;

                try {
                    switch (resp.type()) {
                        case BOOL:
                            res = resp.result();

                            break;

                        case OBJ:
                            res = marsh.unmarshal(resp.resultBytes(), U.resolveClassLoader(ctx.config()));

                            break;

                        case ERR:
                            err = marsh.unmarshal(resp.resultBytes(), U.resolveClassLoader(ctx.config()));

                            break;

                        case MARSH_ERR:
                            err = new IgfsException("Failed to marshal IGFS task result on remote node " +
                                "(see remote node logs for more information) [nodeId + " + op.nodeId() + ']');

                            break;

                        default:
                            assert resp.type() == IgfsClientResponseType.NULL;
                    }
                }
                catch (Exception e) {
                    // Something went wrong during unmarshalling.
                    err = new IgfsException("Failed to unmarshal IGFS task result." , e);
                }

                op.future().onDone(res, err);
            }
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Actual request processing. Happens inside appropriate thread pool.
     *
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void processRequest(UUID nodeId, IgfsClientRequest req) {
        IgfsClientResponse resp;

        try {
            IgfsClientAbstractCallable target = req.target();

            IgfsImpl igfs = (IgfsImpl) ctx.igfs().igfs(target.igfsName());

            if (igfs == null)
                throw new IgfsException("IGFS with the given name is not configured on the node: " + target.igfsName());

            Object res = target.call0(igfs.context());

            resp = createResponse(req.messageId(), res, null);
        }
        catch (Exception e) {
            // Wrap exception.
            resp = createResponse(req.messageId(), null, e);
        }

        // Send response.
        try {
            ctx.io().send(nodeId, GridTopic.TOPIC_IGFS_CLI, resp, GridIoPolicy.PUBLIC_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send IGFS client response [nodeId=" + nodeId +
                ", msgId=" + req.messageId() + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientManager.class, this);
    }

    /**
     * Message listener.
     */
    private class MessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            assert nodeId != null;
            assert msg != null;

            if (msg instanceof IgfsClientRequest)
                onRequest(nodeId, (IgfsClientRequest)msg);
            else if (msg instanceof IgfsClientResponse)
                onResponse((IgfsClientResponse)msg);
            else
                U.error(log, "IGFS client message listener received unknown message: " + msg);
        }
    }

    /**
     * Discovery listener.
     */
    private class DiscoveryListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            switch (evt.type()) {
                case EVT_NODE_LEFT:
                case EVT_NODE_FAILED:
                    DiscoveryEvent evt0 = (DiscoveryEvent) evt;

                    onNodeLeft(evt0.eventNode().id());

                    break;

                default:
                    assert false : "Unknown event: " + evt;
            }
        }
    }

    /**
     * Pending requests worker.
     */
    private class PendingRequestsWorker extends GridWorker {
        /**
         * Consturctor.
         *
         * @param gridName Grid name.
         * @param name WOrker name.
         * @param log Logger.
         */
        public PendingRequestsWorker(@Nullable String gridName, String name, IgniteLogger log) {
            super(gridName, name, log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            IgfsClientInOperation inOp;

            while ((inOp = pendingOps.pollFirst()) != null)
                processRequest(inOp.nodeId(), inOp.request());
        }
    }
}
