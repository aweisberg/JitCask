//Copyright 2012 Ariel Weisberg
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package com.afewmoreamps;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import com.google.common.util.concurrent.ListenableFutureTask;

class MiniCask implements Iterable<CaskEntry> {
    private final File m_path;
    /*
     * Kept around for truncation at the end
     */
    private final FileChannel m_outChannel;
    private final MappedByteBuffer m_buffer;
    private final int HEADER_SIZE = 15;//8-byte timestamp, 1-byte flags, 2-byte key length, 4-byte value length
    private final ByteBuffer m_headerBytes = ByteBuffer.allocate(HEADER_SIZE);
    private final AtomicInteger m_fileLength;
    final int m_fileId;
    private final CRC32 m_crc = new CRC32();
    private final int m_maxValidValueSize;


    public MiniCask(
            File path,
            int id,
            int maxValidValueSize) throws IOException {
        m_maxValidValueSize = maxValidValueSize;
        m_path = path;
        m_fileId = id;
        final boolean existed = path.exists();
        RandomAccessFile ras = new RandomAccessFile(m_path, "rw");
        m_outChannel = ras.getChannel();
        if (!existed) {
            m_buffer = m_outChannel.map(MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
            m_fileLength = new AtomicInteger();
        } else {
            m_buffer = m_outChannel.map(MapMode.READ_ONLY, 0, m_outChannel.size());
            m_fileLength = new AtomicInteger((int)m_outChannel.size());
            m_outChannel.close();
        }
    }

    /*
     * If value is null this is adding a tombstone,
     * The value CRC MUST include the bytes of the value length even though they
     * will be rematerialized into the header before the CRC. This allows us to avoid storing the value length
     * in the KeyDir saving 4 bytes. Crazy? Probably. Saving a few more bytes elsewhere as well, it all adds up
     */
    public boolean addEntry(long timestamp, byte key[], byte value[], int valueCRC, boolean compressedValue, KeyDir keyDir)
            throws IOException {
        if (value != null && value.length > m_maxValidValueSize) {
            throw new IOException("Value length " +
                    value.length +
                    " is > max valid value length " + m_maxValidValueSize);
        }
        if (m_buffer.remaining() < HEADER_SIZE + key.length + (value != null ? value.length + 4 : 0)) {
            m_outChannel.truncate(m_buffer.position() + 1);
            m_outChannel.close();
            return false;
        }
        final byte flags = compressedValue ? (byte)1 : (byte)0;
        m_crc.reset();
        m_headerBytes.clear();
        m_headerBytes.putLong(timestamp);
        m_headerBytes.put(flags);

        final short keyLength = (short)key.length;
        m_headerBytes.putShort(keyLength);

        /*
         * This is the length being recorded,
         * may be -1 for a tombstone. There is no value CRC in this scenario.
         * The value length is always part of the header CRC in addition to the value CRC
         */
        final int valueLength = (value != null ? value.length : -1);
        m_headerBytes.putInt(valueLength);
        m_crc.update(m_headerBytes.array());
        m_crc.update(key);
        final int headerCRC = (int)m_crc.getValue();

        m_buffer.putInt(headerCRC);
        m_buffer.put(m_headerBytes.array(), 0, HEADER_SIZE - 4);
        m_buffer.put(key);

        /*
         * Didn't calculate the CRC this way, but put the value length
         * here so it can be retrieved on the same cache line as the value.
         * Will have to move it back into the right spot
         * to calculate the CRC on retrieval, but didn't want to call update
         * on the CRC again since it is a JNI call. Premature optimization?
         * You bet. The double JNI call penalty is stilled paid on the read end,
         * but reads scale out and writes don't.
         */
        final int valuePosition = m_buffer.position();
        m_buffer.putInt(valueLength);
        if (value != null) {
            m_buffer.putInt(valueCRC);
            m_buffer.put(value);
        }

        if (valueLength != -1) {
            /*
             * Record the new position for this value of the key
             */
            ByteBuffer keyDirEntryBytes = ByteBuffer.allocate(KDEntry.SIZE + key.length);
            System.arraycopy(key,0, keyDirEntryBytes.array(), 0, key.length);
            keyDirEntryBytes.position(key.length);
            KDEntry.toBytes(keyDirEntryBytes, m_fileId, valuePosition, timestamp, flags);
            keyDir.put(keyDirEntryBytes.array());
        } else {
            /*
             * Remove the value from the keydir now that
             * the tombstone has been created
             */
            keyDir.remove(KeyDir.decorateKey(key));
        }

        m_fileLength.lazySet(m_buffer.position());
        return true;
    }

    /**
     * If the value was compressed, return the compressed bytes and a ListenableFutureTask
     * that will decompress the value if run.
     * @param position
     * @param length
     * @param wasCompressed
     * @return
     * @throws IOException
     */
    public Object[] getValue(int position, final boolean wasCompressed) throws IOException {
        if (m_fileLength == null) {
            assert(position < m_buffer.limit());
        } else {
            assert(position < m_fileLength.get());
        }
        ByteBuffer dup = m_buffer.asReadOnlyBuffer();
        dup.position(position);

        ByteBuffer lengthBytes = ByteBuffer.allocate(4);
        dup.get(lengthBytes.array());
        final int length = lengthBytes.getInt();
        final int originalCRC = dup.getInt();

        if (length < 0 || length > 1024 * 1024 * 100) {
            throw new IOException(
                    "Length (" + length + ") is probably not a valid value, retrieved from " +
                    m_path + " position " + position);
        }
        final byte valueBytes[] = new byte[length];
        dup.get(valueBytes);

        CRC32 crc = new CRC32();
        crc.update(lengthBytes.array());
        crc.update(valueBytes);
        final int actualCRC = (int)crc.getValue();

        if (actualCRC != originalCRC) {
            System.out.println("Actual length was " + length);
            throw new IOException("CRC mismatch in record");
        }

        return new Object[] {
                    wasCompressed ? valueBytes : null, //If it was never compressed,
                                                       //don't return the valueBytes as the compressed version
                    ListenableFutureTask.create(new Callable<byte[]>() {
                        @Override
                        public byte[] call() throws Exception {
                            if (wasCompressed) {
                                return org.xerial.snappy.Snappy.uncompress(valueBytes);
                            } else {
                                return valueBytes;
                            }
                        }
                    })};
    }

    @Override
    public Iterator<CaskEntry> iterator() {
        final ByteBuffer view = m_buffer.asReadOnlyBuffer();
        view.position(0);
        view.limit(m_fileLength.get());

        return new Iterator<CaskEntry>() {
            private CaskEntry nextEntry = getNextEntry();

            private CaskEntry getNextEntry() {
                CaskEntry newEntry = null;
                while (newEntry == null) {
                    if (view.remaining() < HEADER_SIZE + 4) {
                        return null;
                    }
                    final long startPosition = view.position();
                    /*
                     * The last 4-bytes of the header (the value length) are not in the right place.
                     * Will put them there manually later
                     */
                    final ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE + 4);
                    final byte headerBytes[] = header.array();
                    view.get(headerBytes, 0, HEADER_SIZE);

                    final int originalHeaderCRC = header.getInt();
                    final long timestamp = header.getLong();
                    final byte flags = header.get();

                    /*
                     * Need to know the key size to find the position
                     * of the value size
                     */
                    final short keySize = header.getShort();
                    if (keySize < 0) {
                        throw new RuntimeException(
                                "Length (" + keySize + ") is probably not a valid key, retrieved from " +
                                m_path + " position " + startPosition);
                    }


                    final byte keyBytes[] = new byte[keySize + KDEntry.SIZE];
                    view.get(keyBytes, 0, keySize);

                    //Skip the key and the CRC for the value to get the length of the value
                    //which was moved forward on put
                    view.get(headerBytes, HEADER_SIZE, 4);
                    final int valueSize = header.getInt();
                    int actualValueSize = 0;
                    if (valueSize >= 0) {
                        actualValueSize = valueSize;
                    }

                    final CRC32 crc = new CRC32();
                    crc.update(headerBytes, 4, HEADER_SIZE);

                    crc.update(keyBytes, 0, keySize);
                    final int actualHeaderCRC = (int)crc.getValue();
                    if (actualHeaderCRC != originalHeaderCRC) {
                        System.err.println("Had a corrupt entry header in " + m_path + " at position " + startPosition);
                        continue;
                    }

                    if (valueSize == -1) {
                        //Tombstone
                        newEntry = new CaskEntry(
                                MiniCask.this,
                                timestamp,
                                flags,
                                keyBytes,
                                -1,
                                -1,
                                null,
                                null);
                        break;
                    }

                    final byte valueBytes[] = new byte[actualValueSize];
                    final int valuePosition = view.position() - 4;
                    final int originalValueCRC = view.getInt();
                    view.get(valueBytes);
                    crc.reset();
                    crc.update(headerBytes, headerBytes.length - 4, 4);
                    crc.update(valueBytes);
                    final int actualValueCRC = (int)crc.getValue();
                    if (actualValueCRC != originalValueCRC) {
                        System.err.println("Had a corrupt entry value in " + m_path + " at position " + startPosition);
                        continue;
                    }

                    FutureTask<byte[]> uncompressedValue;
                    if ((flags & 1) != 0) {
                        uncompressedValue = new FutureTask<byte[]>(new Callable<byte[]>() {
                            @Override
                            public byte[] call() throws Exception {
                                return org.xerial.snappy.Snappy.uncompress(valueBytes);
                            }
                        });
                    } else {
                        uncompressedValue = new FutureTask<byte[]>(new Callable<byte[]>() {
                            @Override
                            public byte[] call() throws Exception {
                                return valueBytes;
                            }
                        });
                    }

                    newEntry =
                        new CaskEntry(
                            MiniCask.this,
                            timestamp,
                            flags,
                            keyBytes,
                            valuePosition,
                            valueBytes.length,
                            valueBytes,
                            uncompressedValue);
                }
                assert(newEntry != null);
                return newEntry;
            }

            @Override
            public boolean hasNext() {
                return nextEntry != null ? true : false;
            }

            @Override
            public CaskEntry next() {
                if (nextEntry == null) {
                    throw new NoSuchElementException();
                }
                CaskEntry retval = nextEntry;
                nextEntry = getNextEntry();
                return retval;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    public void close() throws IOException {
        if (m_outChannel.isOpen()) {
            try {
                m_outChannel.force(false);
            } finally {
                try {
                    m_buffer.force();
                } finally {
                    try {
                        m_outChannel.truncate(m_buffer.position());
                    } finally {
                        m_outChannel.close();
                    }
                }
            }
        }
    }

    public void sync() throws IOException {
        /*
         * *Cough*
         * So msync updates metadata? I think, maybe? God forbid people actually doc this stuff
         * Force isn't guaranteed to take care of modifications to the page cache
         * through the MappedByteBuffer, but my research says that on Linux it is.
         *
         * I wouldn't rely on this without doing a few power plug a crash safety tests
         */
        m_outChannel.force(false);
    }

}