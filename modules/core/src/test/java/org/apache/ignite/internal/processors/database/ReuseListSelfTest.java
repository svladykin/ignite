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

package org.apache.ignite.internal.processors.database;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.processors.cache.database.DataStructure.randomInt;
import static org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBags.newBag;
import static org.apache.ignite.internal.processors.database.BPlusTreeSelfTest.CPUS;
import static org.apache.ignite.internal.processors.database.BPlusTreeSelfTest.MB;
import static org.apache.ignite.internal.processors.database.BPlusTreeSelfTest.PAGE_SIZE;

/**
 *
 */
public class ReuseListSelfTest extends GridCommonAbstractTest {
    /** */
    protected static final int CACHE_ID = 100500;

    /** */
    protected PageMemory pageMem;

    /** */
    protected ReuseList reuseList;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        long seed = System.nanoTime();

        X.println("Test seed: " + seed + "L; // ");

        DataStructure.rnd = new Random(seed);

        pageMem = createPageMemory();

        reuseList = createReuseList(pageMem);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        DataStructure.rnd = null;

        for (int i = 0; i < 10; i++) {
            if (acquiredPages() != 0) {
                System.out.println("!!!");
                U.sleep(10);
            }
        }

        assertEquals(0, acquiredPages());

        pageMem.stop();
    }

    /**
     * @return Number of acquired pages.
     */
    protected long acquiredPages() {
        return ((PageMemoryNoStoreImpl)pageMem).acquiredPages();
    }

    /**
     * @return Page memory.
     */
    protected PageMemory createPageMemory() throws Exception {
        long[] sizes = new long[CPUS];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / CPUS;

        PageMemory pageMem = new PageMemoryNoStoreImpl(log, new UnsafeMemoryProvider(sizes), null, PAGE_SIZE);

        pageMem.start();

        return pageMem;
    }

    /**
     * @param pageMem Page memory.
     * @return Reuse list.
     * @throws IgniteCheckedException If failed.
     */
    protected ReuseList createReuseList(PageMemory pageMem)
        throws IgniteCheckedException {
        return new ReuseListImpl(CACHE_ID, "test-reuse-list", pageMem, null, 0L, true);
    }

    /**
     * @return Allocated page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected long allocatePage() throws IgniteCheckedException {
        return pageMem.allocatePage(CACHE_ID, INDEX_PARTITION, FLAG_IDX);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testReuseListOps() throws IgniteCheckedException {
        int cnt = 10_000;

        Set<Long> set = new HashSet<>(cnt);
        ReuseBag bag = newBag(cnt);

        for (int i = 0; i < cnt; i++) {
            long x = allocatePage();

            assertTrue(set.add(effectivePageId(x)));
            bag.addPage(x);
        }

        assertEquals(cnt, bag.size());

        reuseList.addForRecycle(bag);

        assertEquals(0, bag.size());
        assertEquals(cnt, reuseList.recycledPagesCount());

        for (int i = 0 ; i < 10_000; i++) {
            for (int j = 0, end = 1 + randomInt(11_000); j < end; j++) {
                long x = reuseList.pollRecycledPage();

                if (x == 0L)
                    break;

                bag.addPage(x);
            }

            reuseList.addForRecycle(bag);

            assertEquals(0, bag.size());
        }

        assertEquals(cnt, reuseList.recycledPagesCount());

        long x;

        while ((x = reuseList.pollRecycledPage()) != 0L)
            assertTrue(set.remove(effectivePageId(x)));

        assertTrue("Size: " + set.size(), set.isEmpty());
    }
}
