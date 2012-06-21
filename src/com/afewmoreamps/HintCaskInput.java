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

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import com.afewmoreamps.util.DirectMemoryUtils;
import com.afewmoreamps.util.PrefetchingInputStream;
import com.google.common.util.concurrent.ListenableFutureTask;

class HintCaskInput {
    private final FileInputStream m_fis;
    private final FileChannel m_channel;
    private final File m_path;
    private PrefetchingInputStream m_pis;//used when retrieving the actual keys
    private final MiniCask m_miniCask;

    HintCaskInput(final File path, MiniCask mc) throws IOException {
        this.m_path = path;
        if (!path.exists()) {
            throw new IOException(path + " does not exist");
        }
        if (!path.canRead()) {
            throw new IOException(path + " is not readable");
        }
        m_fis = new FileInputStream(path);
        m_channel = m_fis.getChannel();
        m_miniCask = mc;
    }

    static final int ENTRY_WITH_CHECKSUM_SIZE = HintCaskOutput.ENTRY_SIZE;

    boolean validateChecksum() throws IOException {
        PrefetchingInputStream pis = new PrefetchingInputStream(m_channel, 1024 * 1024 * 16, 3);
        try {
            ByteBuffer allCRCBytes = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());
            int readAllCRCBytes = 0;
            while (readAllCRCBytes < 4) {
                final int readThisTime = pis.read(allCRCBytes.array(), readAllCRCBytes, 4 - readAllCRCBytes);
                if (readThisTime == -1) return false;
                readAllCRCBytes += readThisTime;
            }
            final int allCRCExpected = allCRCBytes.getInt();
            final CRC32 allCRC = new CRC32();
            ByteBuffer entryBuffer = ByteBuffer.allocate(ENTRY_WITH_CHECKSUM_SIZE).order(ByteOrder.nativeOrder());

            while (true) {
                entryBuffer.clear();
                int read = 0;

                while (read < ENTRY_WITH_CHECKSUM_SIZE) {
                    final int readThisTime = pis.read(entryBuffer.array(), read, entryBuffer.capacity() - read);
                    if (readThisTime == -1) break;
                    read += readThisTime;
                }

                if (read == 0 || read < ENTRY_WITH_CHECKSUM_SIZE) {
                    final int actualAllCRC = (int)allCRC.getValue();
                    if (actualAllCRC != allCRCExpected) {
                        m_channel.close();
                        return false;
                    }
                    return true;
                }
                final int expectedCRC = entryBuffer.getInt();
                final CRC32 crc = new CRC32();
                crc.update(entryBuffer.array(), 4, entryBuffer.capacity() - 4);
                final int actualCRC = (int)crc.getValue();

                if (expectedCRC != actualCRC) {
                    m_channel.close();
                    return false;
                }
                allCRC.update(entryBuffer.array(), 0, 4);
            }
        } finally {
            pis.close();
        }
    }

    Iterator<CaskEntry> hintIterator() throws IOException {
        m_channel.position(0);
        m_pis = new PrefetchingInputStream(m_channel, 1024 * 1024 * 16, 3);
        int read = 0;
        while (read < 4) {
            int readThisTime = m_pis.read(new byte[4 - read]);
            if (readThisTime == -1) {
                throw new IOException();
            }
            read += readThisTime;
        }
        return new Iterator<CaskEntry>() {
            private final ByteBuffer entryBuffer =
                    ByteBuffer.allocate(ENTRY_WITH_CHECKSUM_SIZE).order(ByteOrder.nativeOrder());

            @Override
            public boolean hasNext() {
                try {
                    int m_read = 0;

                    while (m_read < ENTRY_WITH_CHECKSUM_SIZE) {
                        final int readThisTime = m_pis.read(entryBuffer.array(), m_read, entryBuffer.capacity() - m_read);
                        if (readThisTime == -1) break;
                        m_read += readThisTime;
                    }

                    if (m_read == 0 || m_read < ENTRY_WITH_CHECKSUM_SIZE) {
                        return false;
                    } else {
                        return true;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            @Override
            public CaskEntry next() {
                entryBuffer.position(4);
                final int valuePosition = entryBuffer.getInt();
                final byte flags = entryBuffer.get();
                final byte keyHash[] = new byte[20];
                entryBuffer.get(keyHash);
                final CaskEntry entry = new CaskEntry(
                        m_miniCask,
                        flags,
                        keyHash,
                        valuePosition,
                        null,
                        null);
                return entry;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }


}
