package org.apache.ignite.internal.processors.blockchain.trie.index.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.blockchain.trie.TrieEntry;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;

public class TrieIndexInnerIO extends BPlusInnerIO<TrieEntry> implements TrieIndexIO {
    /**
     * @param type      Page type.
     * @param ver       Page format version.
     * @param prefixLen Prefix length for this trie.
     */
    protected TrieIndexInnerIO(int type, int ver, int prefixLen) {
        super(type, ver, false, prefixLen);
    }

    @Override
    public void storeByOffset(long pageAddr, int off, TrieEntry row) throws IgniteCheckedException {
        TrieIndexIO.storeByOffset(this, pageAddr, off, row);
    }

    @Override
    public void store(long dstPageAddr, int dstIdx, BPlusIO<TrieEntry> srcIo, long srcPageAddr,
        int srcIdx) throws IgniteCheckedException {
        TrieIndexIO.store(this, dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
    }

    @Override
    public TrieEntry getLookupRow(BPlusTree<TrieEntry,?> tree, long pageAddr, int idx) throws IgniteCheckedException {
        return TrieIndexIO.getLookupRow(this, tree, pageAddr, idx);
    }
}
