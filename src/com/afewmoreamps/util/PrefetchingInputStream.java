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
package com.afewmoreamps.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFutureTask;

public class PrefetchingInputStream extends InputStream {

    private final ByteBuffer buffers[];
    private final ExecutorService es = Executors.newSingleThreadScheduledExecutor();
    private final LinkedBlockingQueue<ByteBuffer> availableBuffers =
        new LinkedBlockingQueue<ByteBuffer>();
    private final List<ListenableFutureTask<ByteBuffer>> readTasks =
        new LinkedList<ListenableFutureTask<ByteBuffer>>();

    private final Iterator<ListenableFutureTask<ByteBuffer>> m_taskIterator;
    private ByteBuffer currentBuffer;

    public PrefetchingInputStream(
            final FileChannel channel,
            final int prefetchSize,
            final int prefetchCount) throws IOException {
        buffers = new ByteBuffer[prefetchCount];
        for (int ii = 0; ii < prefetchCount; ii++) {
            buffers[ii] = ByteBuffer.allocateDirect(prefetchSize);
            availableBuffers.offer(buffers[ii]);
        }

        int currentReadPosition = (int)channel.position();
        final long channelSize = channel.size();
        while (currentReadPosition != channelSize) {
            final int readStartingAt = currentReadPosition;
            final int limit = (int)Math.min(currentReadPosition + prefetchSize, channelSize);
            currentReadPosition += limit;
            readTasks.add(ListenableFutureTask.create(new Callable<ByteBuffer>() {
                @Override
                public ByteBuffer call() throws Exception {
                    final ByteBuffer buffer = availableBuffers.take();
                    buffer.clear();
                    buffer.limit(limit);
                    while (buffer.hasRemaining()) {
                        int read = channel.read(buffer, readStartingAt);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    buffer.flip();
                    return buffer;
                }
            }));
        }
        es.execute(new Runnable() {
            @Override
            public void run() {
                for (ListenableFutureTask<?> task : readTasks) {
                    task.run();
                    try {
                        try {
                            task.get();
                        } catch (InterruptedException e) {
                            return;
                        }
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof InterruptedException) {
                            return;
                        }
                    }
                }
            }
        });
        m_taskIterator = readTasks.iterator();
    }

    @Override
    public int read() throws IOException {
        if (currentBuffer == null || !currentBuffer.hasRemaining()) {
            if (m_taskIterator.hasNext()) {
                try {
                    currentBuffer = m_taskIterator.next().get();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            } else {
                return -1;
            }
        }
        assert(currentBuffer.hasRemaining());
        return currentBuffer.get();
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int fetched = 0;
        while (fetched < len) {
            if (currentBuffer == null || !currentBuffer.hasRemaining()) {
                if (m_taskIterator.hasNext()) {
                    try {
                        currentBuffer = m_taskIterator.next().get();
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } else {
                    return fetched == 0 ? -1 : fetched;
                }
            }
            int nextFetch = Math.min(currentBuffer.remaining(), len - fetched);
            currentBuffer.get(b, off + fetched, nextFetch);
            fetched += nextFetch;
        }
        assert(fetched == len);
        return fetched;
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            es.shutdownNow();
            es.awaitTermination(365, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        for (ByteBuffer buffer : buffers) {
            try {
                DirectMemoryUtils.destroyDirectByteBuffer(buffer);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
