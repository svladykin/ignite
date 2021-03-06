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

package org.apache.ignite.internal.platform.memory;

import static org.apache.ignite.internal.platform.memory.PlatformMemoryUtils.*;

/**
 * Interop pooled memory chunk.
 */
public class PlatformPooledMemory extends PlatformAbstractMemory {
    /** Owning memory pool. */
    private final PlatformMemoryPool pool;

    /**
     * Constructor.
     *
     * @param pool Owning memory pool.
     * @param memPtr Cross-platform memory pointer.
     */
    public PlatformPooledMemory(PlatformMemoryPool pool, long memPtr) {
        super(memPtr);

        assert isPooled(memPtr);
        assert isAcquired(memPtr);

        this.pool = pool;
    }

    /** {@inheritDoc} */
    @Override public void reallocate(int cap) {
        assert isAcquired(memPtr);

        // Try doubling capacity to avoid excessive allocations.
        int doubledCap = PlatformMemoryUtils.capacity(memPtr) << 1;

        if (doubledCap > cap)
            cap = doubledCap;

        pool.reallocate(memPtr, cap);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        assert isAcquired(memPtr);

        pool.release(memPtr); // Return to the pool.
    }
}
