package org.apache.ignite.internal.processors.blockchain.trie.index;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.blockchain.trie.TrieEntry;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;

public class TrieIndex extends BPlusTree<TrieEntry, TrieEntry> {
    /**
     * @param name Trie index name.
     * @param cacheId Cache id.
     * @param pageMem Page memory.
     * @param wal WAL.
     * @param globalRmvId Global remove id.
     * @param metaPageId Meta page id.
     * @param reuseList Reuse list.
     * @throws IgniteCheckedException If failed.
     */
    public TrieIndex(
        String name,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        long metaPageId,
        ReuseList reuseList
    ) throws IgniteCheckedException {
        super(name, cacheId, pageMem, wal, globalRmvId, metaPageId, reuseList);
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<TrieEntry> io, long pageAddr, int idx, TrieEntry row) throws IgniteCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected TrieEntry getRow(BPlusIO<TrieEntry> io, long pageAddr, int idx, Object x) throws IgniteCheckedException {
        return null;
    }
}
