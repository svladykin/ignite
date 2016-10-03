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

package org.apache.ignite.internal.processors.cache.database.tree.reuse;

import java.io.Externalizable;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Reuse bags.
 */
public class ReuseBags {
    /**
     * @param bag Bag.
     * @return {@code true} If it is a single page reuse bag.
     */
    public static boolean isSinglePageBag(ReuseBag bag) {
        return bag.getClass() == SinglePageBag.class;
    }

    /**
     * @param pageId Page ID.
     * @return Single page bag (does not allow adding new pages).
     */
    public static ReuseBag singlePageBag(long pageId) {
        return new SinglePageBag(pageId);
    }

    /**
     * @param cap Initial capacity.
     * @return New reuse bag.
     */
    public static ReuseBag newBag(int cap) {
        return new LongListReuseBag(cap);
    }

    /**
     *
     */
    private ReuseBags() {
        // No-op.
    }

    /**
     * Single page reuse bag.
     */
    private static final class SinglePageBag implements ReuseBag {
        /** */
        private long pageId;

        /**
         * @param pageId Page ID.
         */
        SinglePageBag(long pageId) {
            this.pageId = pageId;
        }

        /** {@inheritDoc} */
        @Override public void addFreePage(long pageId) {
            throw new IllegalStateException("Should never be called.");
        }

        /** {@inheritDoc} */
        @Override public long pollFreePage() {
            long res = pageId;

            pageId = 0L;

            return res;
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return pageId == 0L ? 0 : 1;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SinglePageBag.class, this);
        }
    }

    /**
     * Multi-page reuse bag.
     */
    private static final class LongListReuseBag extends GridLongList implements ReuseBag {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Default constructor for {@link Externalizable}.
         */
        public LongListReuseBag() {
            // No-op.
        }

        /**
         * @param cap Initial capacity.
         */
        public LongListReuseBag(int cap) {
            super(cap);
        }

        /** {@inheritDoc} */
        @Override public void addFreePage(long pageId) {
            add(pageId);
        }

        /** {@inheritDoc} */
        @Override public long pollFreePage() {
            return isEmpty() ? 0L : remove();
        }
    }
}
