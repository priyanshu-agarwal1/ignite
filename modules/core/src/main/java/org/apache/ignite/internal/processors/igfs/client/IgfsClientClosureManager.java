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
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.igfs.IgfsManager;
import org.apache.ignite.internal.util.GridStripedSpinBusyLock;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.Marshaller;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Manager to handle IGFS client closures.
 */
public class IgfsClientClosureManager extends IgfsManager {
    /** Busy lock. */
    private final GridStripedSpinBusyLock busyLock = new GridStripedSpinBusyLock();

    /** Pending closures received when manager is not started yet. */
    private final ConcurrentLinkedDeque startupClos = new ConcurrentLinkedDeque();

    /** Kernal context. */
    private GridKernalContext ctx;

    /** IGFS configuration. */
    private FileSystemConfiguration igfsCfg;

    /** Marshaller. */
    private Marshaller marsh;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        ctx = igfsCtx.kernalContext();

        igfsCfg = igfsCtx.configuration();

        marsh = ctx.config().getMarshaller();

        ctx.discovery().setCustomEventListener();
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
     * Execute callable.
     *
     * @param clo Closure.
     * @return Result.
     */
    public <T> T execute(IgfsClientAbstractCallable<T> clo) {

    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientClosureManager.class, this);
    }

    private class DiscoveryListener implements GridLocalEventListener {

    }
}
