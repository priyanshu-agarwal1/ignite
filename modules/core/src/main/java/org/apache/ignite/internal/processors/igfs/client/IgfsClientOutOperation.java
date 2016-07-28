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

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * IGFS client closure outgoing operation descriptor.
 */
public class IgfsClientOutOperation {
    /** IGFS context. */
    private final IgfsContext igfsCtx;

    /** Target operation. */
    private final IgfsClientAbstractCallable target;

    /** Node selection strategy. */
    private final IgfsClientNodeSelectionStrategy strategy;

    /** Future completed when operation is ready. */
    private final GridFutureAdapter fut;

    /** Target node ID. */
    private UUID nodeId;

    /**
     * Constructor.
     *
     * @param igfsCtx IGFS context.
     * @param target Target operation.
     * @param strategy Node selection strategy.
     * @param fut Future completed when operation is ready.
     */
    public IgfsClientOutOperation(IgfsContext igfsCtx, IgfsClientAbstractCallable target,
        IgfsClientNodeSelectionStrategy strategy, GridFutureAdapter fut) {
        this.igfsCtx = igfsCtx;
        this.target = target;
        this.strategy = strategy;
        this.fut = fut;
    }

    /**
     * @return Target node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return IGFS context.
     */
    public IgfsContext igfsContext() {
        return igfsCtx;
    }

    /**
     * @return Target operation.
     */
    public IgfsClientAbstractCallable target() {
        return target;
    }

    /**
     * @return Node selection strategy.
     */
    public IgfsClientNodeSelectionStrategy strategy() {
        return strategy;
    }

    /**
     * @return Future completed when operation is ready.
     */
    public GridFutureAdapter future() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientOutOperation.class, this);
    }
}
