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

import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * IGFS client closure incoming operation descriptor.
 */
public class IgfsClientInOperation {
    /** Target node ID. */
    private final UUID nodeId;

    /** Request. */
    private final IgfsClientRequest req;

    /**
     * Constructor.
     *
     * @param nodeId Target node ID.
     * @param req Request.
     */
    public IgfsClientInOperation(UUID nodeId, IgfsClientRequest req) {
        this.nodeId = nodeId;
        this.req = req;
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
    public IgfsClientRequest request() {
        return req;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientInOperation.class, this);
    }
}
