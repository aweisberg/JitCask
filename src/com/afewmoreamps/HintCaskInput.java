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
import java.nio.channels.FileChannel;
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
import com.google.common.util.concurrent.ListenableFutureTask;

class HintCaskInput {
    private final FileInputStream m_fis;
    private final FileChannel m_channel;
    private final File m_path;

    HintCaskInput(final File path) throws IOException {
        this.m_path = path;
        if (!path.exists()) {
            throw new IOException(path + " does not exist");
        }
        if (!path.canRead()) {
            throw new IOException(path + " is not readable");
        }
        m_fis = new FileInputStream(path);
        m_channel = m_fis.getChannel();
    }

    boolean validateChecksum() throws IOException, InterruptedException {
        ByteBuffer expectedCRCBytes = ByteBuffer.allocate(4);

        while (expectedCRCBytes.hasRemaining()) {
            int read = m_channel.read(expectedCRCBytes);
            if (read == -1) {
                throw new EOFException("Unexpected EOF reading checksum of " + m_path);
            }
        }
        expectedCRCBytes.flip();

        final int expectedCRC = expectedCRCBytes.getInt();

        final CRC32 crc = new CRC32();


        try {
            ExecutorService es = Executors.newSingleThreadExecutor();
            try {

                for (ListenableFutureTask<ByteBuffer> task : readTasks) {
                    ByteBuffer buffer = null;
                    try {
                        buffer = task.get();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }

                    byte nextCRCBytes[] = new byte[1024 * 32];
                    while (buffer.hasRemaining()) {
                        if (nextCRCBytes.length > buffer.remaining()) {
                            nextCRCBytes = new byte[buffer.remaining()];
                        }
                        crc.update(nextCRCBytes);
                        availableBuffers.offer(buffer);
                    }
                }

                if (((int)crc.getValue()) != expectedCRC) {
                    return false;
                }
                return true;
            } finally {

            }
        } finally {

        }
    }

}
