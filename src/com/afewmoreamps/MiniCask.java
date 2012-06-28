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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

class MiniCask implements Iterable<CaskEntry> {

    private int m_activeKeys;
    private int m_deletedKeys;

    private final File m_path;
    /*
     * Kept around for truncation at the end
     */
    private final FileChannel m_outChannel;
    private final MappedByteBuffer m_buffer;
    static final int HEADER_SIZE = 8;
    private final AtomicInteger m_fileLength;
    final int m_fileId;
    final long m_timestamp;
    private final int m_maxValidValueSize;
    private HintCaskOutput m_hintCaskOutput;
    private HintCaskInput m_hintCaskInput;

    public MiniCask(
            File path,
            int id,
            Long timestamp,
            int maxValidValueSize) throws IOException {
        m_maxValidValueSize = maxValidValueSize;
        String filename = id + "-" + timestamp;
        m_timestamp = timestamp;
        m_path = new File(path, filename + ".minicask");
        final boolean existed = m_path.exists();
        m_fileId = id;
        File hintCaskPath = new File(path, filename + ".hintcask");
        if (hintCaskPath.exists()) {
            if (!existed) {
                throw new IOException("Can't have a hint file without a cask file");
            }
            m_hintCaskInput = new HintCaskInput(hintCaskPath, this);
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
            byte entry[],
            byte keyHash[],
            boolean isTombstone,
            KeyDir keyDir) throws IOException {
        if (m_buffer.remaining() < entry.length) {
            m_outChannel.truncate(m_buffer.position() + 1);//Forgot why the +1? Maybe I should remove it? Not a good sign
            m_outChannel.close();
            try {
                m_hintCaskOutput.close();
            } finally {
                m_hintCaskOutput = null;
            }
            return false;
        }

        final int valuePosition = m_buffer.position();
        m_buffer.put(entry);

        if (!isTombstone) {
            m_hintCaskOutput.addHint(keyHash, valuePosition);
            /*
             * Record the new position for this value of the key
             */
            ByteBuffer keyDirEntryBytes =
                ByteBuffer.allocate(KDEntry.SIZE).order(ByteOrder.nativeOrder());
            System.arraycopy(keyHash,0, keyDirEntryBytes.array(), 0, 20);
            keyDirEntryBytes.position(20);
            KDEntry.toBytes(keyDirEntryBytes, m_fileId, valuePosition);
            keyDir.put(keyDirEntryBytes.array());
        } else {
            m_hintCaskOutput.addHint(keyHash, -1);
            /*
             * Remove the value from the keydir now that
             * the tombstone has been created
             */
            keyDir.remove(KeyDir.decorateKeyHash(keyHash));
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

        dup.position(dup.position());
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

        final int keySize = entry.getInt();
        final int valueSize = entry.getInt();
        entry.position(entry.position() + keySize);
        if (valueSize == -1) {
            throw new IOException("KeyDir should not point to a tombstone");
        }
        assert(entry.remaining() == valueSize);

        return entry.slice();
    }

    /*
     * Get an iterator that populates cask entries with just enough info to reload the
     * key dir.
     */
    public Iterator<CaskEntry> getReloadIterator() throws IOException {
        if (m_hintCaskInput.validateChecksum()) {
            return m_hintCaskInput.hintIterator();
        } else {
            return iterator();
        }
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
                    //Need to CRC the length as well
                    final int compressedLength = view.getInt();
                    byte compressedBytes[] = new byte[compressedLength ];
                    view.get(compressedBytes);

                    final CRC32 crc = new CRC32();
                    crc.update(compressedBytes);

                    final int actualCRC = (int)crc.getValue();
                    if (actualCRC != originalCRC) {
                        throw new RuntimeException("CRC mismatch");
                    }

                    ByteBuffer entry;
                    try {
                        entry = ByteBuffer.wrap(
                                org.xerial.snappy.Snappy.uncompress(compressedBytes)).
                                    order(ByteOrder.nativeOrder());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    /*
                     * Need to know the key size to find the position
                     * of the value size
                     */
                    final int keySize = entry.getInt();
                    if (keySize < -1 || keySize > m_maxValidValueSize) {
                        throw new RuntimeException(
                                "Length (" + keySize + ") is probably not a valid key, retrieved from " +
                                m_path + " position " + entryStartPosition);
                    }

                    final int valueSize = entry.getInt();
                    if (valueSize < -1 || valueSize > m_maxValidValueSize) {
                        throw new RuntimeException(
                                "Length (" + valueSize + ") is probably not a valid value, retrieved from " +
                                m_path + " position " + entryStartPosition);
                    }

                    entry.limit(entry.position() + (keySize == -1 ? 20 : keySize));
                    final ByteBuffer key = entry.slice();
                    entry.limit(entry.capacity());
                    entry.position(entry.position() + keySize);

                    MessageDigest md;
                    try {
                        md = MessageDigest.getInstance("SHA-1");
                    } catch (NoSuchAlgorithmException e) {
                        throw new AssertionError(e);
                    }
                    md.update(key.array(), key.arrayOffset(), key.remaining());
                    if (valueSize == -1) {
                        //Tombstone
                        newEntry = new CaskEntry(
                                MiniCask.this,
                                md.digest(),
                                -1,
                                null,
                                null);
                        break;
                    }

                    newEntry =
                        new CaskEntry(
                            MiniCask.this,
                            md.digest(),
                            entryStartPosition,
                            key,
                            entry.slice());
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
        if (m_hintCaskOutput != null) {
            m_hintCaskOutput.close();
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

    public static Object[] constructEntry(byte[] key, byte[] value) throws IOException {
        assert(key != null);

        byte keyHash[] = null;
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
            keyHash = md.digest(key);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }

        final int keyLength = key.length;
        final int valueLength = value.length;

        ByteBuffer forCompression = ByteBuffer.allocate(8 + keyLength + valueLength).order(ByteOrder.nativeOrder());
        forCompression.putInt(keyLength);
        forCompression.putInt(valueLength);
        forCompression.put(key);
        forCompression.put(value);

        final byte compressedBytes[] = org.xerial.snappy.Snappy.compress(forCompression.array());
        ByteBuffer finalBuf =
                ByteBuffer.allocate(HEADER_SIZE + compressedBytes.length).order(ByteOrder.nativeOrder());

        finalBuf.position(4);
        finalBuf.putInt(compressedBytes.length);
        finalBuf.put(compressedBytes);

        final CRC32 crc = new CRC32();
        crc.update(finalBuf.array(), 8, finalBuf.capacity() - 8);
        finalBuf.putInt(0, (int)crc.getValue());

        return new Object[] { finalBuf.array(), keyHash };
    }

    public static byte[] constructTombstoneEntry(byte keyHash[], int fileId, long timestamp) {
        /*
         * A tombstone contains the usual CRC and length prefix (which is -1 to sigil a tombstone,
         * as well as the 20 byte hash, 4 byte fileid and 8 byte timestamp for the version of the file that
         * contains the deleted entry. The tombstone must remain as long as that version of the file is present
         */
        ByteBuffer entryBuf = ByteBuffer.allocate(8 + 20 + 4 + 8);
        entryBuf.position(4);
        entryBuf.putInt(-1);
        entryBuf.put(keyHash);
        entryBuf.putLong(timestamp);

        final CRC32 crc = new CRC32();
        crc.update(entryBuf.array(), 4, entryBuf.capacity() - 4);
        entryBuf.putInt(0, (int)crc.getValue());

        return entryBuf.array();
    }

}