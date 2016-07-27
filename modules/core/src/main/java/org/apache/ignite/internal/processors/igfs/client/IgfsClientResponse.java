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

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * IGFS client closure execute response.
 */
public class IgfsClientResponse implements Message {
    /** Message ID. */
    private long msgId;

    /** Response type. */
    private IgfsClientResponseType typ;

    /** Result. */
    @GridToStringInclude
    private Object res;

    /** Result bytes. */
    @GridToStringExclude
    private byte[] resBytes;

    /**
     * Default constructor.
     */
    public IgfsClientResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param msgId Message ID.
     * @param typ Type.
     * @param res Result.
     * @param resBytes Result bytes.
     */
    public IgfsClientResponse(long msgId, IgfsClientResponseType typ, @Nullable Object res,
        @Nullable byte[] resBytes) {
        this.msgId = msgId;
        this.typ = typ;
        this.res = res;
        this.resBytes = resBytes;
    }

    /**
     * @return Message ID.
     */
    public long messageId() {
        return msgId;
    }

    /**
     * @return Type.
     */
    public IgfsClientResponseType type() {
        return typ;
    }

    /**
     * @return Result.
     */
    @Nullable public Object result() {
        return res;
    }

    /**
     * @return Result bytes.
     */
    @Nullable public byte[] resultBytes() {
        return resBytes;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -28;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return (byte)(typ == IgfsClientResponseType.NULL || typ == IgfsClientResponseType.MARSH_ERR ?
            2 : 3);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        // TODO

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        // TODO

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientResponse.class, this);
    }
}
