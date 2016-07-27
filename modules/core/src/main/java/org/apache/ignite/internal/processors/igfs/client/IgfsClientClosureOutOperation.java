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
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * IGFS client closure outgoing opeartion descriptor.
 */
public class IgfsClientClosureOutOperation {
    /** Target node ID. */
    private final UUID nodeId;

    /** Target operation. */
    private final IgfsClientAbstractCallable target;

    /** Future completed when operation is ready. */
    private final IgniteInternalFuture fut;

    /**
     * Constructor.
     *
     * @param nodeId Target node ID.
     * @param target Target operation.
     * @param fut Future completed when operation is ready.
     */
    public IgfsClientClosureOutOperation(UUID nodeId, IgfsClientAbstractCallable target, IgniteInternalFuture fut) {
        this.nodeId = nodeId;
        this.target = target;
        this.fut = fut;
    }

    /**
     * @return Target node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Target operation.
     */
    public IgfsClientAbstractCallable target() {
        return target;
    }

    /**
     * @return Future completed when operation is ready.
     */
    public IgniteInternalFuture future() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientClosureOutOperation.class, this);
    }
}
