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

import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * IGFS client closure execute request.
 */
public class IgfsClientClosureRequest implements Message {
    /** Base fields (all except of target) count. */
    private static final byte BASE_FIELDS_CNT = 2;

    /** Originating node ID. */
    private UUID nodeId;

    /** Message ID. */
    private long msgId;

    /** Target callable. */
    private IgfsClientAbstractCallable target;

    /**
     * Default constructor.
     */
    public IgfsClientClosureRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param nodeId Originating node ID.
     * @param msgId Message ID.
     * @param target Target callable.
     */
    public IgfsClientClosureRequest(UUID nodeId, long msgId, IgfsClientAbstractCallable target) {
        assert nodeId != null;
        assert target != null;

        this.nodeId = nodeId;
        this.msgId = msgId;
        this.target = target;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Message ID.
     */
    public long messageId() {
        return msgId;
    }

    /**
     * @return Target callable.
     */
    public IgfsClientAbstractCallable target() {
        return target;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -27;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return (byte)(BASE_FIELDS_CNT + target.fieldsCount());
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        byte fieldsCount = fieldsCount();

        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("msgId", msgId))
                    return false;

                writer.incrementState();

            default:
                while (writer.state() < fieldsCount) {
                    if (!target.writeTo(writer, writer.state() - BASE_FIELDS_CNT))
                        return false;

                    writer.incrementState();
                }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }
}
