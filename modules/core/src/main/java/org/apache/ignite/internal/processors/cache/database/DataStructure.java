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

package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.RecycleRecord;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.isWalDeltaRecordNeeded;

/**
 * Base class for all the data structures based on {@link PageMemory}.
 */
public abstract class DataStructure implements PageLockListener {
    /** For tests. */
    public static Random rnd;

    /** */
    private final String name;

    /** */
    protected final int cacheId;

    /** */
    protected final PageMemory pageMem;

    /** */
    protected final IgniteWriteAheadLogManager wal;

    /** */
    protected ReuseList reuseList;

    /**
     * @param name Name.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     */
    public DataStructure(
        String name,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal
    ) {
        assert pageMem != null;
        assert !F.isEmpty(name);

        this.name = name;
        this.cacheId = cacheId;
        this.pageMem = pageMem;
        this.wal = wal;
    }

    /**
     * @return Data structure name.
     */
    public final String getName() {
        return name;
    }

    /**
     * @return Cache ID.
     */
    public final int getCacheId() {
        return cacheId;
    }

    /**
     * @param max Max.
     * @return Random value from {@code 0} (inclusive) to the given max value (exclusive).
     */
    public static int randomInt(int max) {
        Random rnd0 = rnd != null ? rnd : ThreadLocalRandom.current();

        return rnd0.nextInt(max);
    }

    /**
     * @return Allocated page.
     * @throws IgniteCheckedException If failed.
     */
    protected final long allocatePage() throws IgniteCheckedException {
        long pageId = 0L;

        if (reuseList != null)
            pageId = reuseList.pollRecycledPage();

        if (pageId == 0L)
            pageId = allocatePageNoReuse();

        assert pageId != 0;

        return pageId;
    }

    /**
     * @return Page ID of newly allocated page.
     * @throws IgniteCheckedException If failed.
     */
    protected long allocatePageNoReuse() throws IgniteCheckedException {
        return pageMem.allocatePage(cacheId, PageIdAllocator.INDEX_PARTITION, FLAG_IDX);
    }

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    protected final Page page(long pageId) throws IgniteCheckedException {
        return pageMem.page(cacheId, pageId);
    }

    /**
     * @param page Updated page.
     * @return {@code true} If we need to make a delta WAL record for the change in this page.
     */
    protected final boolean needWalDeltaRecord(Page page) {
        return isWalDeltaRecordNeeded(wal, page);
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param buf Buffer.
     * @return Recycled page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected final long recyclePage(long pageId, Page page, ByteBuffer buf) throws IgniteCheckedException {
        // Rotate page ID to avoid concurrency issues with reused pages.
        pageId = PageIdUtils.rotatePageId(pageId);

        // Update page ID inside of the buffer, Page.id() will always return the original page ID.
        PageIO.setPageId(buf, pageId);

        if (needWalDeltaRecord(page))
            wal.log(new RecycleRecord(cacheId, page.id(), pageId));

        return pageId;
    }

    /**
     * @param page Page.
     * @return Buffer.
     */
    protected final ByteBuffer tryWriteLock(Page page) {
        return PageHandler.writeLock(page, this, true);
    }


    /**
     * @param page Page.
     * @return Buffer.
     */
    protected final ByteBuffer writeLock(Page page) {
        return PageHandler.writeLock(page, this, false);
    }

    /**
     * @param page Page.
     * @param buf Buffer.
     * @param dirty Dirty page.
     */
    protected final void writeUnlock(Page page, ByteBuffer buf, boolean dirty) {
        PageHandler.writeUnlock(page, buf, this, dirty);
    }

    /**
     * @param page Page.
     * @return Buffer.
     */
    protected final ByteBuffer readLock(Page page) {
        return PageHandler.readLock(page, this);
    }

    /**
     * @param page Page.
     * @param buf Buffer.
     */
    protected final void readUnlock(Page page, ByteBuffer buf) {
        PageHandler.readUnlock(page, buf, this);
    }

    /** {@inheritDoc} */
    @Override public void onBeforeWriteLock(Page page) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onWriteLock(Page page, ByteBuffer buf) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onWriteUnlock(Page page, ByteBuffer buf) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onBeforeReadLock(Page page) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReadLock(Page page, ByteBuffer buf) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReadUnlock(Page page, ByteBuffer buf) {
        // No-op.
    }
}
