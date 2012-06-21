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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

class HintCaskOutput {
    private final ListeningExecutorService m_thread;
    private final FileChannel m_fc;
    private final CRC32 m_crc = new CRC32();
    private final CRC32 m_allCRC = new CRC32();
    static final int ENTRY_SIZE = (20 + 4 + 1 + 4);

    private final ByteBuffer m_buffer =
            ByteBuffer.allocate(ENTRY_SIZE).order(ByteOrder.nativeOrder());

    /*
     * Room for 64 key value pairs
     */
    private final ByteBuffer m_outBuffer =
            ByteBuffer.allocateDirect((ENTRY_SIZE) * 64).order(ByteOrder.nativeOrder());

    private byte m_keyHashes[][] = new byte[64][];
    private int m_positions[] = new int[64];
    private byte m_flags[] = new byte[64];
    private int m_pendingMetadataIndex;

    HintCaskOutput(final File path) throws IOException {
        if (path.exists()) {
            throw new IOException(path + " already exists");
        }
        FileOutputStream fos = new FileOutputStream(path);
        m_fc = fos.getChannel();
        m_fc.position(4);

        final ThreadFactory tf = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(
                        null,
                        r,
                        "HintCask[" + path + "] write thread",
                        1024 * 256);
                t.setDaemon(true);
                return t;
            }

        };
        m_thread = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(tf));
    }

    void addHint(byte keyHash[], int position, byte flag) {
        m_keyHashes[m_pendingMetadataIndex] = keyHash;
        m_flags[m_pendingMetadataIndex] = flag;
        m_positions[m_pendingMetadataIndex++] = position;
        if (m_pendingMetadataIndex >= m_keyHashes.length) {
            flushHints(m_pendingMetadataIndex, m_keyHashes, m_positions, m_flags);
        }
    }

    private void flushHints(
            final int numKeys,
            final byte keyHashes[][],
            final int positions[],
            final byte flags[]) {
        if (keyHashes.length != positions.length ||
                keyHashes == null || positions == null) {
            throw new IllegalArgumentException();
        }
        m_thread.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    m_outBuffer.clear();
                    for (int ii = 0; ii < numKeys; ii++) {
                        m_buffer.clear();
                        m_buffer.position(4);
                        assert(
                                positions[ii] >= 0 ||
                                positions[ii] == -1);//-1 value is tombstone sentinel
                        m_buffer.putInt( positions[ii]);
                        m_buffer.put(flags[ii]);
                        m_buffer.put(keyHashes[ii]);
                        m_crc.reset();
                        m_crc.update(m_buffer.array(), 4, m_buffer.position() - 4);
                        m_buffer.putInt(0, (int)m_crc.getValue());
                        m_allCRC.update(m_buffer.array(), 0, 4);
                        m_buffer.flip();
                        m_outBuffer.put(m_buffer);
                    }
                    m_outBuffer.flip();

                    while (m_outBuffer.hasRemaining()) {
                        m_fc.write(m_outBuffer);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    //Can't tolerate failing to write hints
                    System.exit(-1);
                }
            }
        });
        m_keyHashes = new byte[64][];
        m_positions = new int[64];
        m_flags = new byte[64];
        m_pendingMetadataIndex = 0;
    }

    void close() throws  IOException {
        flushHints(m_pendingMetadataIndex, m_keyHashes, m_positions, m_flags);
        m_thread.shutdown();
        try {
            m_thread.awaitTermination(356, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        m_fc.force(false);
        ByteBuffer allCRCBuf = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());
        allCRCBuf.putInt(0, (int)m_allCRC.getValue());
        while (allCRCBuf.hasRemaining()) {
            m_fc.write( allCRCBuf, allCRCBuf.position());
        }
        m_fc.force(false);
        m_fc.close();
    }
}
