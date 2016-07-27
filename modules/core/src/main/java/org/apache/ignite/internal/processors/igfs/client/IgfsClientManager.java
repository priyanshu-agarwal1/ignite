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
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsImpl;
import org.apache.ignite.internal.processors.igfs.IgfsManager;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Manager to handle IGFS client closures.
 */
public class IgfsClientManager extends IgfsManager {
    /** Pending input operations received when manager is not started yet. */
    private final ConcurrentLinkedDeque<IgfsClientRequest> pending = new ConcurrentLinkedDeque<>();

    /** Outgoing operations. */
    private final Map<Long, IgfsClientOutOperation> outOps = new ConcurrentHashMap<>();

    /** Marshaller. */
    private final Marshaller marsh;

    /** Whether manager is fully started and ready to process requests. */
    private volatile boolean ready;

    /** Stopping flag. */
    private volatile boolean stopping;

    /** RW lock for synchronization. */
    private final StripedCompositeReadWriteLock rwLock =
        new StripedCompositeReadWriteLock(Runtime.getRuntime().availableProcessors() * 2);

    /** IO message listener. */
    private final MessageListener msgLsnr = new MessageListener();

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

        // TODO: Discovery listener.
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        // TODO: Set ready flag.
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        ctx.io().removeMessageListener(GridTopic.TOPIC_IGFS_CLI, msgLsnr);

        // TODO: Discovery listener.

        // TODO: Set stopping flag
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        // TODO: Cleanup everything.
    }

    /**
     * Execute IGFS closure.
     *
     * @param igfsCtx IGFS context.
     * @param clo Closure.
     * @return Result.
     */
    public <T> T execute(IgfsContext igfsCtx, IgfsClientAbstractCallable<T> clo) throws IgniteCheckedException {
        return executeAsync(igfsCtx, clo).get();
    }

    /**
     * Execute IGFS closure asynchronously.
     *
     * @param igfsCtx IGFS context.
     * @param clo Closure.
     * @return Future.
     */
    public <T> IgniteInternalFuture<T> executeAsync(IgfsContext igfsCtx, IgfsClientAbstractCallable<T> clo) {

        // TODO

        return null;
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
     * @param req Request.
     */
    private void onRequest(IgfsClientRequest req) {
        rwLock.readLock().lock();

        try {
            if (stopping)
                return; // Discovery listener on remote node will handle node leave.

            if (ready)
                processRequest(req); // Normal execution flow.
            else
                pending.add(req); // Add to pending set if manager is not fully started yet.
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
     * @param req Request.
     */
    private void processRequest(IgfsClientRequest req) {
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
            ctx.io().send(req.nodeId(), GridTopic.TOPIC_IGFS_CLI, resp, GridIoPolicy.PUBLIC_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send IGFS client response [nodeId=" + req.nodeId() +
                ", msgId=" + req.messageId() + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientManager.class, this);
    }

    /**
     * Handles job execution requests.
     */
    private class MessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            assert nodeId != null;
            assert msg != null;

            if (msg instanceof IgfsClientRequest)
                onRequest((IgfsClientRequest)msg);
            else if (msg instanceof IgfsClientResponse)
                onResponse((IgfsClientResponse)msg);
            else
                U.error(log, "IGFS client message listener received unknown message: " + msg);
        }
    }
}
