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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.processors.igfs.client.meta.IgfsClientMetaIdsForPathCallable;
import org.apache.ignite.internal.processors.igfs.client.meta.IgfsClientMetaInfoForPathCallable;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract callable for IGFS tasks initiated on client node and passed to data node.
 */
public abstract class IgfsClientAbstractCallable<T> implements IgniteCallable<T>, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Base fields count. */
    private static final byte BASE_FIELDS_CNT = 2;

    /** Type ID. */
    private short typeId;

    /** IGFS name. */
    private String igfsName;

    /** Path for operation. */
    protected IgfsPath path;

    /** Injected instance. */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Create callable for the given type ID.
     *
     * @param typeId Type ID.
     * @return Callable.
     */
    public static IgfsClientAbstractCallable callableForTypeId(short typeId) {
        switch (typeId) {
            case IgfsClientMetaIdsForPathCallable.TYPE_ID:
                return new IgfsClientMetaIdsForPathCallable();

            case IgfsClientMetaInfoForPathCallable.TYPE_ID:
                return new IgfsClientMetaInfoForPathCallable();

            case IgfsClientAffinityCallable.TYPE_ID:
                return new IgfsClientAffinityCallable();

            case IgfsClientDeleteCallable.TYPE_ID:
                return new IgfsClientDeleteCallable();

            case IgfsClientExistsCallable.TYPE_ID:
                return new IgfsClientExistsCallable();

            case IgfsClientInfoCallable.TYPE_ID:
                return new IgfsClientInfoCallable();

            case IgfsClientListFilesCallable.TYPE_ID:
                return new IgfsClientListFilesCallable();

            case IgfsClientListPathsCallable.TYPE_ID:
                return new IgfsClientListPathsCallable();

            case IgfsClientMkdirsCallable.TYPE_ID:
                return new IgfsClientMkdirsCallable();

            case IgfsClientRenameCallable.TYPE_ID:
                return new IgfsClientRenameCallable();

            case IgfsClientSetTimesCallable.TYPE_ID:
                return new IgfsClientUpdateCallable();

            case IgfsClientSizeCallable.TYPE_ID:
                return new IgfsClientSizeCallable();

            case IgfsClientSummaryCallable.TYPE_ID:
                return new IgfsClientSummaryCallable();

            case IgfsClientUpdateCallable.TYPE_ID:
                return new IgfsClientUpdateCallable();

            default:
                throw new IgniteException("Unsupported IGFS callable type ID: " + typeId);
        }
    }

    /**
     * Default constructor.
     */
    protected IgfsClientAbstractCallable() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param typeId Type ID.
     * @param igfsName IGFS name.
     * @param path Path.
     */
    protected IgfsClientAbstractCallable(short typeId, @Nullable String igfsName, @Nullable IgfsPath path) {
        this.typeId = typeId;
        this.igfsName = igfsName;
        this.path = path;
    }

    /** {@inheritDoc} */
    @Override public final T call() throws Exception {
        assert ignite != null;

        IgfsEx igfs = (IgfsEx)ignite.fileSystem(igfsName);

        return call0(igfs.context());
    }

    /**
     * Execute task.
     *
     * @param ctx IGFS ocntext.
     * @return Result.
     * @throws Exception If failed.
     */
    protected abstract T call0(IgfsContext ctx) throws Exception;

    /** {@inheritDoc} */
    @Override public final void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeString(igfsName);
        IgfsUtils.writePath(rawWriter, path);

        writeBinary0(rawWriter);
    }

    /** {@inheritDoc} */
    @Override public final void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader rawReader = reader.rawReader();

        igfsName = rawReader.readString();
        path = IgfsUtils.readPath(rawReader);

        readBinary0(rawReader);
    }

    /**
     * Write binary.
     *
     * @param rawWriter Raw writer.
     */
    protected void writeBinary0(BinaryRawWriter rawWriter) {
        // No-op.
    }

    /**
     * Read binary.
     *
     * @param rawReader Raw reader.
     */
    protected void readBinary0(BinaryRawReader rawReader) {
        // No-op.
    }

    /**
     * @return Type ID.
     */
    public short typeId() {
        return typeId;
    }

    /**
     * @return Fields count.
     */
    public final byte fieldsCount() {
        return (byte)(BASE_FIELDS_CNT + fieldsCount0());
    }

    /**
     * @return Additional fields count of concrete class.
     */
    protected byte fieldsCount0() {
        return 0;
    }

    /**
     * Write callable to writer.
     *
     * @param writer Writer.
     * @param fieldId Field ID.
     * @return Result.
     */
    public final boolean writeTo(MessageWriter writer, int fieldId) {
        switch (fieldId) {
            case 0:
                return writer.writeString("igfsName", igfsName);

            case 1:
                return writer.writeString("path", path.toString());

            default:
                return writeTo0(writer, fieldId - BASE_FIELDS_CNT);
        }
    }

    /**
     * Write callable to writer (for child classes).
     *
     * @param writer Writer.
     * @param fieldId Field ID.
     * @return Result.
     */
    protected boolean writeTo0(MessageWriter writer, int fieldId) {
        assert false : "Should not be called.";

        return true;
    }

    /**
     * Read callable content from reader.
     *
     * @param reader Reader.
     * @param fieldId Field ID.
     */
    public final void readFrom(MessageReader reader, int fieldId) {
        switch (fieldId) {
            case 0:
                igfsName = reader.readString("igfsName");

            case 1:
                String pathStr = reader.readString("path");

                if (reader.isLastRead())
                    path = new IgfsPath(pathStr);

            default:
                readFrom0(reader, fieldId - BASE_FIELDS_CNT);
        }
    }

    /**
     * Read callable content from reader (for child classes).
     *
     * @param reader Reader.
     * @param fieldId Field ID.
     */
    protected void readFrom0(MessageReader reader, int fieldId) {
        assert false : "Should not be called.";
    }
}
