package org.apache.ignite.internal.processors.blockchain.trie;

import org.apache.ignite.internal.processors.cache.persistence.Storable;

public class TrieEntry implements Storable {
    /** */
    private long link;

    /** */
    private int part = -1;

    /** */
    private byte[] key;

    /** {@inheritDoc} */
    @Override public void link(long link) {
        this.link = link;
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return link;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /**
     * @param part Partition.
     */
    public void partition(int part) {
        this.part = part;
    }

    /**
     * @param key Key.
     */
    public void key(byte[] key) {
        this.key = key;
    }

    /**
     * @return Key.
     */
    public byte[] key() {
        return key;
    }
}
