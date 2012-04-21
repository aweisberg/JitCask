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
import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.afewmoreamps.util.CRCSnappyOutputStream;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

class HintCaskOutput {
    private final ListeningExecutorService m_thread;
    private final ByteBuffer m_hintHeader = ByteBuffer.allocate(14);
    private final CRCSnappyOutputStream m_outputStream;

    HintCaskOutput(final File path) throws IOException {
        if (path.exists()) {
            throw new IOException(path + " already exists");
        }
        FileOutputStream fos = new FileOutputStream(path);
        FileChannel channel = fos.getChannel();
        m_outputStream = new CRCSnappyOutputStream(fos, channel);

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

    private boolean assertNoNulls(byte values[][]) {
        for (int ii = 0; ii < values.length; ii++) {
            if (values[ii] == null) {
                return false;
            }
        }
        return true;
    }

    void addHints(final byte keys[][], final int positions[], final long timestamps[]) {
        if (keys.length != positions.length ||
                timestamps.length  != keys.length || timestamps == null || keys == null || positions == null) {
            throw new IllegalArgumentException();
        }
        assert(assertNoNulls(keys));
        m_thread.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int ii = 0; ii < keys.length; ii++) {
                        m_hintHeader.clear();
                        assert(
                                positions[ii] > 0 ||
                                positions[ii] == Integer.MIN_VALUE);//Min value is tombstone sentinel
                        m_hintHeader.putInt( 0, positions[ii]);
                        m_hintHeader.putLong( 4, timestamps[ii]);
                        m_hintHeader.putShort( 12, (short)keys[ii].length);
                        m_outputStream.write(m_hintHeader.array());
                        m_outputStream.write(keys[ii]);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    //Can't tolerate failing to write hints
                    System.exit(-1);
                }
            }
        });
    }

    void close() throws InterruptedException, IOException {
        m_thread.shutdown();
        m_thread.awaitTermination(356, TimeUnit.DAYS);
        m_outputStream.close();
    }
}
