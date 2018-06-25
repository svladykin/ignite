package org.apache.ignite.internal.processors.blockchain.trie.index.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.blockchain.trie.TrieEntry;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIOInterface;

public interface TrieIndexIO extends BPlusIOInterface {
    /**
     * @return Prefix length.
     */
    default int getPrefixLength() {
        int prefixLen = getItemSize();

        if (isLeaf())
            prefixLen -= 8;

        return prefixLen;
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index of item.
     * @return Prefix.
     */
    default byte[] getPrefix(long pageAddr, int idx) {
        assert idx < getCount(pageAddr) : idx;

        return PageUtils.getBytes(pageAddr, offset(idx), getPrefixLength());
    }

    static void storeByOffset(TrieIndexIO io, long pageAddr, int off, TrieEntry row) throws IgniteCheckedException {
    }

    static void store(TrieIndexIO io, long dstPageAddr, int dstIdx, BPlusIO<TrieEntry> srcIo, long srcPageAddr,
        int srcIdx) throws IgniteCheckedException {
    }

    static TrieEntry getLookupRow(TrieIndexIO io, BPlusTree<TrieEntry,?> tree, long pageAddr, int idx) throws IgniteCheckedException {
        return null;
    }
}
