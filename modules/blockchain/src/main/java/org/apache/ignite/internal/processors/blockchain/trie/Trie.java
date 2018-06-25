package org.apache.ignite.internal.processors.blockchain.trie;

public interface Trie<K,V> {
    /**
     *
     * @param key Key.
     * @return The respective value or {@code null} if no matching key was found.
     */
    V get(K key);

    /**
     * Puts the key and value pair into this trie if the given key does not exist already.
     *
     * @param key Key.
     * @param val Value.
     * @return {@code true} If such a key did not exist in the trie and was successfully added,
     *          {@code false} if the given key is a duplicate of an existing one and no trie modification was made.
     */
    boolean put(K key, V val);

    /**
     * Removes the respective key-value pair if the matching key found.
     *
     * @param key Key.
     * @return {@code true} If the matching key found and successfully removed.
     */
    boolean remove(K key);
}
