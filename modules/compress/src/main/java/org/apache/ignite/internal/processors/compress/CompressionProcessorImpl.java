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

package org.apache.ignite.internal.processors.compress;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CompactablePageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.xerial.snappy.Snappy;

import static org.apache.ignite.configuration.DiskPageCompression.SKIP_GARBAGE;
import static org.apache.ignite.internal.util.GridUnsafe.NATIVE_BYTE_ORDER;

/**
 * Compression processor.
 */
public class CompressionProcessorImpl extends CompressionProcessor {
    /** */
    static boolean testMode = false;

    /** A bit more than max page size. */
    private static final int THREAD_LOCAL_BUF_SIZE = 18 * 1024;

    /** */
    private final ThreadLocalByteBuffer tmp = new ThreadLocalByteBuffer(THREAD_LOCAL_BUF_SIZE);

    /**
     * @param ctx Kernal context.
     */
    public CompressionProcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param cap Capacity.
     * @return Direct byte buffer.
     */
    static ByteBuffer allocateDirectBuffer(int cap) {
        return ByteBuffer.allocateDirect(cap).order(NATIVE_BYTE_ORDER);
    }

    /** {@inheritDoc} */
    @Override public void checkPageCompressionSupported(Path storagePath, int pageSize) throws IgniteCheckedException {
        if (testMode)
            return;

        if (!U.isLinux())
            throw new IgniteCheckedException("Currently page compression is supported only for Linux.");

        FileSystemUtils.checkSupported();

        int fsBlockSize = FileSystemUtils.getFileSystemBlockSize(storagePath);

        if (fsBlockSize <= 0)
            throw new IgniteCheckedException("Failed to get file system block size: " + storagePath);

        if (!U.isPow2(fsBlockSize))
            throw new IgniteCheckedException("Storage block size must be power of 2: " + fsBlockSize);

        if (pageSize < fsBlockSize * 2) {
            throw new IgniteCheckedException("Page size (now configured to " + pageSize + " bytes) " +
                "must be at least 2 times larger than the underlying storage block size (detected to be " + fsBlockSize +
                " bytes at '" + storagePath + "') for page compression.");
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer compressPage(
        ByteBuffer page,
        int pageSize,
        int blockSize,
        DiskPageCompression compression,
        int compressLevel
    ) throws IgniteCheckedException {
        assert compression != null;
        assert U.isPow2(pageSize): pageSize;
        assert U.isPow2(blockSize): blockSize;
        assert page.position() == 0 && page.limit() == pageSize;

        PageIO io = PageIO.getPageIO(page);

        if (!(io instanceof CompactablePageIO))
            return page;

        ByteBuffer compactPage = tmp.get();

        // Drop the garbage from the page.
        ((CompactablePageIO)io).compactPage(page, compactPage, pageSize);
        page.clear();

        int compactSize = compactPage.limit();

        assert compactSize <= pageSize: compactSize;

        // If no need to compress further or configured just to skip garbage.
        if (compactSize < blockSize || compression == SKIP_GARBAGE)
            return setCompactionInfo(compactPage, compactSize);

        ByteBuffer compressedPage = doCompressPage(compression, compactPage, compactSize, compressLevel);

        assert compressedPage.position() == 0;
        int compressedSize = compressedPage.limit();

        int freeCompactBlocks = (pageSize - compactSize) / blockSize;
        int freeCompressedBlocks = (pageSize - compressedSize) / blockSize;

        if (freeCompactBlocks >= freeCompressedBlocks) {
            if (freeCompactBlocks == 0)
                return page; // No blocks will be released.

            return setCompactionInfo(compactPage, compactSize);
        }

        return setCompressionInfo(compressedPage, compression, compressedSize, compactSize);
    }

    /**
     * @param page Page.
     * @param compactSize Compacted page size.
     * @return The given page.
     */
    private static ByteBuffer setCompactionInfo(ByteBuffer page, int compactSize) {
        return setCompressionInfo(page, SKIP_GARBAGE, compactSize, compactSize);
    }

    /**
     * @param page Page.
     * @param compression Compression algorithm.
     * @param compressedSize Compressed size.
     * @param compactedSize Compact size.
     * @return The given page.
     */
    private static ByteBuffer setCompressionInfo(ByteBuffer page, DiskPageCompression compression, int compressedSize, int compactedSize) {
        assert compressedSize >= 0 && compressedSize <= Short.MAX_VALUE: compressedSize;
        assert compactedSize >= 0 && compactedSize <= Short.MAX_VALUE: compactedSize;

        PageIO.setCompressionType(page, getCompressionType(compression));
        PageIO.setCompressedSize(page, (short)compressedSize);
        PageIO.setCompactedSize(page, (short)compactedSize);

        return page;
    }

    /**
     * @param compression Compression algorithm.
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    private ByteBuffer doCompressPage(DiskPageCompression compression, ByteBuffer compactPage, int compactSize, int compressLevel) {
        switch (compression) {
            case ZSTD:
                return compressPageZstd(compactPage, compactSize, compressLevel);

            case LZ4:
                return compressPageLz4(compactPage, compactSize, compressLevel);

            case SNAPPY:
                return compressPageSnappy(compactPage, compactSize);
        }
        throw new IllegalStateException("Unsupported compression: " + compression);
    }

    /**
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    private ByteBuffer compressPageLz4(ByteBuffer compactPage, int compactSize, int compressLevel) {
        LZ4Compressor compressor = Lz4.getCompressor(compressLevel);

        ByteBuffer compressedPage = allocateDirectBuffer(PageIO.COMMON_HEADER_END +
            compressor.maxCompressedLength(compactSize - PageIO.COMMON_HEADER_END));

        copyPageHeader(compactPage, compressedPage, compactSize);
        compressor.compress(compactPage, compressedPage);

        compactPage.flip();
        compressedPage.flip();

        return compressedPage;
    }

    /**
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    private ByteBuffer compressPageZstd(ByteBuffer compactPage, int compactSize, int compressLevel) {
        ByteBuffer compressedPage = allocateDirectBuffer((int)(PageIO.COMMON_HEADER_END +
            Zstd.compressBound(compactSize - PageIO.COMMON_HEADER_END)));

        copyPageHeader(compactPage, compressedPage, compactSize);
        Zstd.compress(compressedPage, compactPage, compressLevel);

        compactPage.flip();
        compressedPage.flip();

        return compressedPage;
    }

    /**
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @return Compressed page.
     */
    private ByteBuffer compressPageSnappy(ByteBuffer compactPage, int compactSize) {
        ByteBuffer compressedPage = allocateDirectBuffer(PageIO.COMMON_HEADER_END +
            Snappy.maxCompressedLength(compactSize - PageIO.COMMON_HEADER_END));

        copyPageHeader(compactPage, compressedPage, compactSize);

        try {
            int compressedSize = Snappy.compress(compactPage, compressedPage);
            assert compressedPage.limit() == PageIO.COMMON_HEADER_END + compressedSize;
        }
        catch (IOException e) {
            throw new IgniteException("Failed to compress page with Snappy.", e);
        }

        compactPage.position(0);
        compressedPage.position(0);

        return compressedPage;
    }

    /**
     * @param compactPage Compacted page.
     * @param compressedPage Compressed page.
     * @param compactSize Compacted page size.
     */
    private static void copyPageHeader(ByteBuffer compactPage, ByteBuffer compressedPage, int compactSize) {
        compactPage.limit(PageIO.COMMON_HEADER_END);
        compressedPage.put(compactPage);
        compactPage.limit(compactSize);
    }

    /**
     * @param compression Compression.
     * @return Level.
     */
    private static byte getCompressionType(DiskPageCompression compression) {
        if (compression == null)
            return UNCOMPRESSED_PAGE;

        switch (compression) {
            case ZSTD:
                return ZSTD_COMPRESSED_PAGE;

            case LZ4:
                return LZ4_COMPRESSED_PAGE;

            case SNAPPY:
                return SNAPPY_COMPRESSED_PAGE;

            case SKIP_GARBAGE:
                return COMPACTED_PAGE;
        }
        throw new IllegalStateException("Unexpected compression: " + compression);
    }

    /** {@inheritDoc} */
    @Override public void decompressPage(ByteBuffer page, int pageSize) throws IgniteCheckedException {
        assert page.capacity() == pageSize;

        byte compressType = PageIO.getCompressionType(page);

        if (compressType == UNCOMPRESSED_PAGE)
            return; // Nothing to do.

        short compressedSize = PageIO.getCompressedSize(page);
        short compactSize = PageIO.getCompactedSize(page);

        assert compactSize <= pageSize && compactSize >= compressedSize;

        if (compressType != COMPACTED_PAGE) {
            ByteBuffer dst = tmp.get();

            // Position on a part that needs to be decompressed.
            page.limit(compressedSize)
                .position(PageIO.COMMON_HEADER_END);

            // LZ4 needs this limit to be exact.
            dst.limit(compactSize - PageIO.COMMON_HEADER_END);

            if (compressType == ZSTD_COMPRESSED_PAGE) {
                Zstd.decompress(dst, page);
                dst.flip();
            }
            else if (compressType == LZ4_COMPRESSED_PAGE) {
                Lz4.decompress(page, dst);
                dst.flip();
            }
            else if (compressType == SNAPPY_COMPRESSED_PAGE) {
                try {
                    Snappy.uncompress(page, dst);
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            } else
                throw new IllegalStateException("Unknown compression: " + compressType);

            page.position(PageIO.COMMON_HEADER_END).limit(compactSize);
            page.put(dst).flip();
            assert page.limit() == compactSize;
        }
        else
            page.position(0).limit(compactSize);

        CompactablePageIO io = PageIO.getPageIO(page);

        io.restorePage(page, pageSize);

        setCompressionInfo(page, null, 0, 0);
    }

    /** */
    static class Lz4 {
        /** */
        static final LZ4Factory factory = LZ4Factory.fastestInstance();

        /** */
        static final LZ4FastDecompressor decompressor = factory.fastDecompressor();

        /** */
        static final LZ4Compressor fastCompressor = factory.fastCompressor();

        /**
         * @param level Compression level.
         * @return Compressor.
         */
        static LZ4Compressor getCompressor(int level) {
            assert level >= 0 && level <= 17: level;
            return level == 0 ? fastCompressor : factory.highCompressor(level);
        }

        /**
         * @param page Page.
         * @param dst Destination buffer.
         */
        static void decompress(ByteBuffer page, ByteBuffer dst) {
            decompressor.decompress(page, dst);
        }
    }

    /**
     */
    static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
        /** */
        final int size;

        /**
         * @param size Size.
         */
        ThreadLocalByteBuffer(int size) {
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override protected ByteBuffer initialValue() {
            return allocateDirectBuffer(size);
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer get() {
            ByteBuffer buf = super.get();
            buf.clear();
            return buf;
        }
    }
}