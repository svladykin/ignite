package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;

public interface BPlusIOInterface {

    /**
     * @return Item size in bytes.
     */
    int getItemSize();

    /**
     * @param pageAddr Page address.
     * @return Items count in the page.
     */
    int getCount(long pageAddr);

    /**
     * @return {@code true} If we can get the full row from this page using
     * method {@link BPlusTree#getRow(BPlusIO, long, int)}.
     * Must always be {@code true} for leaf pages.
     */
    boolean canGetRow();

    /**
     * @return {@code true} if it is a leaf page.
     */
    boolean isLeaf();

    /**
     * @param pageAddr Page address.
     * @param pageSize Page size.
     * @return Max items count.
     */
    int getMaxCount(long pageAddr, int pageSize);

    /**
     * @param idx Index of element.
     * @return Offset from byte buffer begin in bytes.
     */
    public abstract int offset(int idx);

    /**
     * Copy items from source page to destination page.
     * Both pages must be of the same type and the same version.
     *
     * @param srcPageAddr Source page address.
     * @param dstPageAddr Destination page address.
     * @param srcIdx Source begin index.
     * @param dstIdx Destination begin index.
     * @param cnt Items count.
     * @param cpLeft Copy leftmost link (makes sense only for inner pages).
     * @throws IgniteCheckedException If failed.
     */
    public abstract void copyItems(long srcPageAddr, long dstPageAddr, int srcIdx, int dstIdx, int cnt, boolean cpLeft)
        throws IgniteCheckedException;
}
