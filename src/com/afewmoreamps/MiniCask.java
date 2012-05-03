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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

class MiniCask implements Iterable<CaskEntry> {
    private final File m_path;
    /*
     * Kept around for truncation at the end
     */
    private final FileChannel m_outChannel;
    private final MappedByteBuffer m_buffer;
    static final int HEADER_SIZE = 15;//8-byte timestamp, 1-byte flags, 2-byte key length, 4-byte value length
    private final AtomicInteger m_fileLength;
    final int m_fileId;
    private final int m_maxValidValueSize;
    private HintCaskOutput m_hintCaskOutput;
    private HintCaskInput m_hintCaskInput;

    public MiniCask(
            File path,
            int id,
            int maxValidValueSize) throws IOException {
        m_maxValidValueSize = maxValidValueSize;
        m_path = new File(path, id + ".minicask");
        final boolean existed = m_path.exists();
        m_fileId = id;
        File hintCaskPath = new File(path, id + ".hintcask");
        if (hintCaskPath.exists()) {
            if (!existed) {
                throw new IOException("Can't have a hint file without a cask file");
            }
            m_hintCaskInput = new HintCaskInput(hintCaskPath);
        } else {
            m_hintCaskOutput = new HintCaskOutput(hintCaskPath);
        }

        RandomAccessFile ras = new RandomAccessFile(m_path, "rw");
        m_outChannel = ras.getChannel();
        if (!existed) {
            m_buffer = m_outChannel.map(MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
            m_buffer.order(ByteOrder.nativeOrder());
            m_fileLength = new AtomicInteger();
        } else {
            m_buffer = m_outChannel.map(MapMode.READ_ONLY, 0, m_outChannel.size());
            m_fileLength = new AtomicInteger((int)m_outChannel.size());
            m_buffer.order(ByteOrder.nativeOrder());
            m_outChannel.close();
        }
    }

    public boolean addEntry(
            int crc,
            byte entry[],
            byte key[],
            byte flags,
            long timestamp,
            boolean isTombstone,
            KeyDir keyDir) throws IOException {
        if (m_buffer.remaining() < entry.length) {
            m_outChannel.truncate(m_buffer.position() + 1);//Forgot why the +1? Maybe I should remove it? Not a good sign
            m_outChannel.close();
            try {
                m_hintCaskOutput.close();
            } catch (InterruptedException e) {
                throw new IOException(e);
            } finally {
                m_hintCaskOutput = null;
            }
            return false;
        }

        final int valuePosition = m_buffer.position();
        m_buffer.putInt(crc);
        m_buffer.putInt(entry.length);
        m_buffer.put(entry);

        if (!isTombstone) {
            /*
             * Record the new position for this value of the key
             */
            ByteBuffer keyDirEntryBytes =
                ByteBuffer.allocate(KDEntry.SIZE + key.length).order(ByteOrder.nativeOrder());
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
    public ByteBuffer getValue(int position) throws IOException {
        if (m_fileLength == null) {
            assert(position < m_buffer.limit());
        } else {
            assert(position < m_fileLength.get());
        }
        ByteBuffer dup = m_buffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());
        dup.position(position);

        final int originalCRC = dup.getInt();
        final int entryLength = dup.getInt();

        if (entryLength < 0 || entryLength > m_maxValidValueSize) {
            throw new IOException(
                    "Length (" + entryLength + ") is probably not a valid value, retrieved from " +
                    m_path + " position " + position);
        }
        final byte entryBytes[] = new byte[entryLength];
        dup.get(entryBytes);
        CRC32 crc = new CRC32();
        crc.update(entryBytes);
        final int actualCRC = (int)crc.getValue();

        if (actualCRC != originalCRC) {
            throw new IOException("CRC mismatch in record, retrieved from " +
                    m_path + " position " + position);
        }

        ByteBuffer entry =
            ByteBuffer.wrap(org.xerial.snappy.Snappy.uncompress(entryBytes)).order(ByteOrder.nativeOrder());
        entry.position(9);
        final short keySize = entry.getShort();
        final int valueSize = entry.getInt();
        entry.position(entry.position() + keySize);
        assert(entry.remaining() == valueSize);

        return entry.slice();
    }

    @Override
    public Iterator<CaskEntry> iterator() {
        final ByteBuffer view = m_buffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());
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
                    final int entryStartPosition = view.position();
                    final int originalCRC = view.getInt();
                    final int entryLength = view.getInt();
                    if (entryLength < 0 || entryLength > m_maxValidValueSize) {
                        throw new RuntimeException("Invalid value size " + entryLength);
                    }
                    byte compressedEntryBytes[] = new byte[entryLength];
                    view.get(compressedEntryBytes);
                    CRC32 crc = new CRC32();
                    crc.update(compressedEntryBytes);

                    final int actualCRC = (int)crc.getValue();
                    if (actualCRC != originalCRC) {
                        System.err.println("Had a corrupt entry in " + m_path + " at position " + entryStartPosition);
                        continue;
                    }

                    ByteBuffer entry;
                    try {
                        entry = ByteBuffer.wrap(
                                org.xerial.snappy.Snappy.uncompress(compressedEntryBytes)).
                                    order(ByteOrder.nativeOrder());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    final long timestamp = entry.getLong();
                    final byte flags = entry.get();

                    /*
                     * Need to know the key size to find the position
                     * of the value size
                     */
                    final short keySize = entry.getShort();
                    if (keySize < 0 || keySize > m_maxValidValueSize) {
                        throw new RuntimeException(
                                "Length (" + keySize + ") is probably not a valid key, retrieved from " +
                                m_path + " position " + entryStartPosition);
                    }

                    final int valueSize = entry.getInt();
                    if (valueSize < -1 || valueSize > m_maxValidValueSize) {
                        throw new RuntimeException(
                                "Length (" + keySize + ") is probably not a valid value, retrieved from " +
                                m_path + " position " + entryStartPosition);
                    }

                    entry.limit(entry.position() + keySize);
                    final ByteBuffer key = entry.slice();
                    entry.limit(entry.capacity());
                    entry.position(entry.position() + keySize);

                    if (valueSize == -1) {
                        //Tombstone
                        newEntry = new CaskEntry(
                                MiniCask.this,
                                timestamp,
                                flags,
                                key,
                                -1,
                                -1);
                        break;
                    }

                    newEntry =
                        new CaskEntry(
                            MiniCask.this,
                            timestamp,
                            flags,
                            key,
                            entryStartPosition,
                            entryLength + 4);
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

    public static Object[] constructEntry(byte[] key, byte[] value,
            long timestamp) throws IOException {
        ByteBuffer buildBuf =
            ByteBuffer.allocate(
                    key.length +
                    (value == null ? 0 : value.length) +
                    HEADER_SIZE).order(ByteOrder.nativeOrder());
        buildBuf.putLong(timestamp);
        buildBuf.put((byte)0);
        buildBuf.putShort((short)key.length);
        buildBuf.putInt(value == null ? -1 : value.length);
        buildBuf.put(key);
        if (value != null) {
            buildBuf.put(value);
        }

        byte compressedBytes[] = org.xerial.snappy.Snappy.compress(buildBuf.array());

        CRC32 crc = new CRC32();
        crc.update(compressedBytes);
        return new Object[] { (int)crc.getValue(), compressedBytes };
    }

}