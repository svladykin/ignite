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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Task that collect cache metrics from all nodes.
 */
@GridInternal
public class VisorCacheConfigurationCollectorTask
    extends VisorOneNodeTask<Collection<IgniteUuid>, Map<IgniteUuid, VisorCacheConfiguration>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheConfigurationCollectorJob job(Collection<IgniteUuid> arg) {
        return new VisorCacheConfigurationCollectorJob(arg, debug);
    }
}
