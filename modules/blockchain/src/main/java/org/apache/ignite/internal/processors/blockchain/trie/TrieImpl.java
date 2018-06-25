package org.apache.ignite.internal.processors.blockchain.trie;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.blockchain.trie.index.TrieIndex;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;

import static java.util.Objects.requireNonNull;

/**
 */
public class TrieImpl implements Trie<byte[], byte[]> {
    /** */
    private final TrieIndex idx;

    /** */
    private final int part;

    /**
     * @param part Partition.
     * @param name Trie name,
     * @param cacheId Cache id. TODO ???
     * @param pageMem Page memory.
     * @param wal WAL.
     * @param globalRmvId Global remove id.
     * @param metaPageId Met page id.
     * @param reuseList Reuse list.
     */
    public TrieImpl(
        int part,
        String name,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        long metaPageId,
        ReuseList reuseList
    ) throws IgniteCheckedException {
        this.part = part;
        this.idx = new TrieIndex(name, cacheId, pageMem, wal, globalRmvId, metaPageId, reuseList);
    }

    @Override
    public boolean put(byte[] key, byte[] val) {
        requireNonNull(key);
        requireNonNull(val);

        return false;
    }

    @Override
    public byte[] get(byte[] key) {
        requireNonNull(key);

        return null;
    }

    @Override
    public boolean remove(byte[] key) {
        requireNonNull(key);

        return false;
    }
}
