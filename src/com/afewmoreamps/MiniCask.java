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

class MiniCask implements Iterable<CaskEntry> {
    private final File m_path;
    /*
     * Kept around for truncation at the end
     */
    private final FileChannel m_outChannel;
    private final MappedByteBuffer m_buffer;
    private final int HEADER_SIZE = 15;//8-byte timestamp, 1-byte flags, 2-byte key length, 4-byte value length
    private final ByteBuffer m_headerBytes = ByteBuffer.allocate(HEADER_SIZE);
    private final ByteBuffer m_keyDirEntryBytes = ByteBuffer.allocate(KDEntry.SIZE);
    private final AtomicInteger m_fileLength;
    final int m_fileId;
    private final CRC32 m_crc = new CRC32();


    public MiniCask(File path, int id) throws IOException {
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
     * If value is null this is adding a tombstone
     */
    public boolean addEntry(long timestamp, byte key[], byte value[], int valueCRC, boolean compressedValue, KeyDir keyDir)
            throws IOException {
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
         * may be -1 for a tombstone
         */
        final int valueLength = (value != null ? value.length : -1);
        m_headerBytes.putInt(valueLength);
        m_crc.update(m_headerBytes.array());
        m_crc.update(key);
        final int headerCRC = (int)m_crc.getValue();

        m_buffer.putInt(headerCRC);
        m_buffer.putLong(timestamp);
        m_buffer.put(flags);
        m_buffer.putShort(keyLength);
        m_buffer.putInt(valueLength);
        m_buffer.put(key);
        final int valuePosition = m_buffer.position();
        if (value != null) {
            m_buffer.putInt(valueCRC);
            m_buffer.put(value);
        }

        /*
         * This is the size of the entire record and not just the payload
         * associated with the key
         */
        final int valueSize = m_buffer.position() - valuePosition - 4;//-4 don't include crc

        if (valueLength != -1) {
            /*
             * Record the new position for this value of the key
             */
            KDEntry.toBytes(m_keyDirEntryBytes, m_fileId, valueSize, valuePosition, timestamp, flags);
            byte kdbytes[] = new byte[KDEntry.SIZE];
            System.arraycopy(m_keyDirEntryBytes.array(), 0, kdbytes, 0, KDEntry.SIZE);
            keyDir.put(key, kdbytes);
        } else {
            /*
             * Remove the value from the keydir now that
             * the tombstone has been created
             */
            keyDir.remove(key);
        }

        m_fileLength.lazySet(m_buffer.position());
        return true;
    }

    public byte[] getValue(int position, int length, boolean decompress) throws IOException {
        if (m_fileLength == null) {
            assert(position < m_buffer.limit());
        } else {
            assert(position < m_fileLength.get());
        }
        ByteBuffer dup = m_buffer.asReadOnlyBuffer();
        dup.position(position);

        final int originalCRC = dup.getInt();
        byte valueBytes[] = new byte[length];
        dup.get(valueBytes);

        CRC32 crc = new CRC32();
        crc.update(valueBytes);
        final int actualCRC = (int)crc.getValue();

        if (actualCRC != originalCRC) {
            System.out.println("Actual length was " + length);
            throw new IOException("CRC mismatch in record");
        }

        if (decompress) {
            valueBytes = org.xerial.snappy.Snappy.uncompress(valueBytes);
        }
        return valueBytes;
    }

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
                    ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE + 4);
                    view.get(header.array());

                    final int originalHeaderCRC = header.getInt();
                    final long timestamp = header.getLong();
                    final byte flags = header.get();
                    final short keySize = header.getShort();
                    final int valueSize = header.getInt();
                    int actualValueSize = 0;
                    if (valueSize >= 0) {
                        actualValueSize = valueSize;
                    }

                    final CRC32 crc = new CRC32();
                    crc.update(header.array(), 4, HEADER_SIZE);
                    if (view.remaining() < keySize + actualValueSize) {
                        return null;
                    }
                    if (keySize < 0) {
                        System.out.println("Uhoh");
                    }
                    final byte keyBytes[] = new byte[keySize];
                    view.get(keyBytes);
                    crc.update(keyBytes);
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
                    final int valuePosition = view.position();
                    final int originalValueCRC = view.getInt();
                    view.get(valueBytes);
                    crc.reset();
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

}