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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

import static org.apache.ignite.internal.processors.cache.database.DataStructure.randomInt;
import static org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBags.singlePageBag;

/**
 *
 */
public class FreeListSelfTest extends ReuseListSelfTest {
    /**
     */
    public void testFreeListOps() throws IgniteCheckedException {
        Map<Long, TestRow> rows = new HashMap<>();

        for (int i = 0; i < 100_000; i++) {
            switch (randomInt(4)) {
                case 0:
                    TestRow row = newRow();

//                    X.println("insert: " + row.data.length);

                    freeList().insertDataRow(row);

                    assertTrue(row.link != 0L);
                    assertNull(rows.put(row.link, row));

                    break;

                case 1:
                    Iterator<Long> it = rows.keySet().iterator();

                    if (it.hasNext()) {
                        long link = it.next();

                        it.remove();

//                        X.println("remove: " + U.hexLong(link));

                        freeList().removeDataRowByLink(link);
                    }

                    break;

                case 2:
//                    X.println("poll");

                    reuseList.pollRecycledPage();

                    break;

                case 3:
//                    X.println("recycle");

                    reuseList.addForRecycle(singlePageBag(allocatePage()));

                    break;

                default:
                    fail();
            }
        }

        // TODO add contents check
    }

    private TestRow newRow() {
        TestRow row = new TestRow();

        row.data = new byte[1 + (randomInt(2) == 1 ? randomInt(25) : randomInt(1000))];

        DataStructure.rnd.nextBytes(row.data);

        return row;
    }

    /**
     * @return Free list.
     */
    private FreeList freeList() {
        return (FreeListImpl)reuseList;
    }

    /** {@inheritDoc} */
    @Override protected ReuseList createReuseList(PageMemory pageMem) throws IgniteCheckedException {
        return new FreeListImpl(CACHE_ID, "test-free-list", pageMem, null, null, 0L, true) {
            @Override protected int getRowSize(CacheDataRow row) throws IgniteCheckedException {
                return row(row).data.length;
            }

            @Override protected int addRow(
                Page page,
                ByteBuffer buf,
                DataPageIO io,
                CacheDataRow row,
                int rowSize
            ) throws IgniteCheckedException {
                row(row).link = io.addRow(buf, row(row).data);

                return rowSize;
            }

            @Override protected int addRowFragment(
                Page page,
                ByteBuffer buf,
                DataPageIO io,
                CacheDataRow row,
                int written,
                int rowSize
            ) throws IgniteCheckedException {
                byte[] r = row(row).data;

                int payloadSize = io.payloadSize(buf, written, r.length);
                byte[] payload = Arrays.copyOfRange(r, written, written + payloadSize);

                row(row).link = io.addRowFragment(buf, payload, row.link());

                return written + payloadSize;
            }
        };
    }

    /**
     * @param row Row.
     * @return Test row.
     */
    private static TestRow row(CacheDataRow row) {
        return (TestRow)row;
    }

    /**
     * Test row.
     */
    private static class TestRow implements CacheDataRow {
        /** */
        long link;

        /** */
        byte[] data;

        /** {@inheritDoc} */
        @Override public KeyCacheObject key() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public CacheObject value() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion version() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public long expireTime() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return link;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            assert link != 0L;

            this.link = link;
        }
    }
}
