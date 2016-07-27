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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsManager;
import org.apache.ignite.internal.util.GridStripedSpinBusyLock;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Manager to handle IGFS client closures.
 */
public class IgfsClientClosureManager extends IgfsManager {
    /** Pending closures received when manager is not started yet. */
    private final ConcurrentLinkedDeque startupClos = new ConcurrentLinkedDeque();

    /** Marshaller. */
    private final Marshaller marsh;

    /** Stopping flag. */
    private volatile boolean stopping;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public IgfsClientClosureManager(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.config().getMarshaller();
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        // TODO
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
     * @param marsh Marshaller.
     * @return Response.
     */
    public IgfsClientClosureResponse createResponse(long msgId, @Nullable Object res, @Nullable Throwable resErr,
        Marshaller marsh) {
        try {

            if (resErr != null)
                return new IgfsClientClosureResponse(msgId, IgfsClientClosureResponseType.ERR, null,
                    marsh.marshal(resErr));
            else {
                if (res == null)
                    return new IgfsClientClosureResponse(msgId, IgfsClientClosureResponseType.NULL, null, null);
                else if (res instanceof Boolean)
                    return new IgfsClientClosureResponse(msgId, IgfsClientClosureResponseType.BOOL, res, null);
                else
                    return new IgfsClientClosureResponse(msgId, IgfsClientClosureResponseType.OBJ, null,
                        marsh.marshal(res));
            }
        }
        catch (Exception e) {
            U.error(log, "Failed to marshal IGFS closure result [msgId=" + msgId + ", res=" + res +
                ", resErr=" + resErr + ']', e);

            return new IgfsClientClosureResponse(msgId, IgfsClientClosureResponseType.MARSH_ERR, null, null);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientClosureManager.class, this);
    }
}
