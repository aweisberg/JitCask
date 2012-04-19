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
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import com.afewmoreamps.KeyDir.SubKeyDir;
import com.afewmoreamps.util.COWMap;
import com.google.common.util.concurrent.*;

class JitCaskImpl implements JitCask, Iterable<CaskEntry> {

    public static class CaskConfig {
        private final File caskPath;
        public int syncInterval = 100;
        public boolean compressByDefault = true;
        public boolean syncByDefault = true;

        public CaskConfig(File caskPath) {
            this.caskPath = caskPath;
        }
    }

    /**
     * If this flag is set the value supplied put will be compressed for storage
     */
    public static final int PUTFLAG_COMPRESS = 1;

    /**
     * If this flag is set then put will not return until
     * the data is fsynced
     */
    public static final int PUTFLAG_SYNC = 1 << 1;

    /**
     * Constant with the recommended default of compress and sync
     */
    public static final int PUTFLAG_COMPRESS_AND_SYNC = PUTFLAG_COMPRESS | PUTFLAG_SYNC;

    private final File m_caskPath;

    private final ListeningExecutorService m_writeThread;
    private final ListeningExecutorService m_compressionThreads;
    private final ListeningExecutorService m_readThreads;
    private final ListeningScheduledExecutorService m_syncThread;

    private final KeyDir m_keyDir = new KeyDir(READ_QUEUE_DEPTH);

    private final COWMap<Integer, MiniCask> m_miniCasks = new COWMap<Integer, MiniCask>();

    private MiniCask m_outCask;

    private int m_nextMiniCaskIndex = 0;

    private final boolean m_compressByDefault;
    private final boolean m_syncByDefault;
    private final int m_syncInterval;

    private static final int READ_QUEUE_DEPTH = 64;
    private final Semaphore m_maxOutstandingReads = new Semaphore(READ_QUEUE_DEPTH);
    private final Semaphore m_maxOutstandingWrites = new Semaphore(64);
    private final ConcurrentLinkedQueue<MiniCask> m_finishedCasksPendingSync =
        new ConcurrentLinkedQueue<MiniCask>();

    public JitCaskImpl(CaskConfig config) throws IOException {
        m_syncByDefault = config.syncByDefault;
        m_syncInterval = config.syncInterval;
        m_caskPath = config.caskPath;
        m_compressByDefault = config.compressByDefault;

        if (!m_caskPath.exists()) {
            throw new IOException("Path " + m_caskPath + " does not exist");
        }
        if (!m_caskPath.isDirectory()) {
            throw new IOException("Path " + m_caskPath + " exists but is not a directory");
        }
        if (!m_caskPath.canRead()) {
            throw new IOException("Path " + m_caskPath + " is not readable");
        }
        if (!m_caskPath.canWrite()) {
            throw new IOException("Path " + m_caskPath + " is not writable");
        }
        if (!m_caskPath.canExecute()) {
            throw new IOException("Path " + m_caskPath + " is not executable");
        }

        ThreadFactory tf = new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread( r, "JitCask[" + m_caskPath + "] Write Thread");
                t.setDaemon(true);
                return t;
            }

        };
        m_writeThread = MoreExecutors.listeningDecorator(
                new ThreadPoolExecutor(
                        1,
                        1,
                        0,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        tf));

        tf = new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread( r, "JitCask[" + m_caskPath + "] Sync Thread");
                t.setDaemon(true);
                return t;
            }

        };
        m_syncThread = MoreExecutors.listeningDecorator(
                new ScheduledThreadPoolExecutor(
                1,
                tf));

        tf = new ThreadFactory() {
            private final AtomicInteger m_counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(
                        null,
                        r,
                        "JitCask[" + m_caskPath + "] Read Thread " + m_counter.incrementAndGet(),
                        1024 * 256);
                t.setDaemon(true);
                return t;
            }

        };
        m_readThreads = MoreExecutors.listeningDecorator(
                new ThreadPoolExecutor(
                        READ_QUEUE_DEPTH,
                        READ_QUEUE_DEPTH,
                        0,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        tf,
                        new ThreadPoolExecutor.AbortPolicy()));

        tf = new ThreadFactory() {
            private final AtomicInteger m_counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(
                        null,
                        r,
                        "JitCask[" + m_caskPath + "] Compression Thread " + m_counter.incrementAndGet(),
                        1024 * 256);
                t.setDaemon(true);
                return t;
            }

        };

        final int availableProcs = Runtime.getRuntime().availableProcessors() / 2;
        m_compressionThreads =
                MoreExecutors.listeningDecorator(
                        new ThreadPoolExecutor(
                                availableProcs,
                                availableProcs,
                                0,
                                TimeUnit.MILLISECONDS,
                                new LinkedBlockingQueue<Runnable>(availableProcs),
                                tf,
                                new ThreadPoolExecutor.CallerRunsPolicy()));
    }

    @Override
    public synchronized void open() throws IOException {
        if (m_readThreads.isShutdown()) {
            throw new IOException("Can't reuse a closed JitCask");
        }
        reloadJitCask();
        m_outCask = new MiniCask(new File(m_caskPath, m_nextMiniCaskIndex + ".minicask"), m_nextMiniCaskIndex);
        m_miniCasks.put(m_nextMiniCaskIndex, m_outCask);
        m_nextMiniCaskIndex++;

        m_syncThread.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                /*
                 * Catch throwable since we don't ever want to stop syncing.
                 */
                try {
                    final long  start = System.currentTimeMillis();
                    sync();
                    final long delta = System.currentTimeMillis() - start;
                    if (delta > m_syncInterval) {
                        System.err.println("Missed sync interval by " + delta);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }, 0, m_syncInterval, TimeUnit.MILLISECONDS);
    }

    private void sync() throws IOException {
        MiniCask cask;
        while ((cask = m_finishedCasksPendingSync.poll()) != null) {
            cask.sync();
        }
        m_outCask.sync();
    }

    private void reloadJitCask() throws IOException {
        int highestIndex = -1;
        for (File f : m_caskPath.listFiles()) {
            if (!f.getName().endsWith(".minicask")) {
                throw new IOException("Unrecognized file " + f + " found in cask directory");
            }
            int caskIndex = Integer.valueOf(f.getName().substring(0, f.getName().length() - 9));
            highestIndex = Math.max(caskIndex, highestIndex);
            MiniCask cask = new MiniCask(f, caskIndex);
            m_miniCasks.put(caskIndex, cask);
        }
        m_nextMiniCaskIndex = highestIndex + 1;

        for (CaskEntry ce : this) {
            final SubKeyDir subKeyDir = m_keyDir.getSubKeyDir(ce.key);
            byte entryBytes[] = subKeyDir.m_keys.get(ce.key);
            if (entryBytes == null) {
                subKeyDir.m_keys.put(
                        ce.key,
                        KDEntry.toBytes(
                                ce.miniCask.m_fileId,
                                ce.valueLength,
                                ce.valuePosition,
                                ce.timestamp,
                                ce.flags));
                continue;
            }

            KDEntry existingEntry = new KDEntry(entryBytes);
            if (existingEntry.timestamp < ce.timestamp) {
                subKeyDir.m_keys.put(
                        ce.key,
                        KDEntry.toBytes(
                                ce.miniCask.m_fileId,
                                ce.valueLength,
                                ce.valuePosition,
                                ce.timestamp,
                                ce.flags));
            }
        }

        /*
         * Remove all the tombstones from memory because the merge thread might have moved stuff out of order?
         * Really want to see if I can make merging not change the order of stuff
         */
        for (SubKeyDir subKeyDir : m_keyDir.m_subKeyDirs.values()) {
            Iterator<Map.Entry<byte[], byte[]>> iter = subKeyDir.m_keys.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iter.next();
                KDEntry kdEntry = new KDEntry(entry.getValue());
                if (kdEntry.valuePos == -1) {
                    iter.remove();
                }
            }
        }
    }

    @Override
    public ListenableFuture<byte[]> get(final byte[] key) {
        try {
            m_maxOutstandingReads.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return m_readThreads.submit(new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                try {
                    while (true) {
                        KDEntry entry = m_keyDir.get(key);
                        if (entry == null) {
                            return null;
                        }

                        final MiniCask mc = m_miniCasks.get(entry.fileId);
                        /*
                         * Race condition, file was deleted after looking up entry.
                         * Loop again and find out the state of the key from the KeyDir
                         * again.
                         */
                        if (mc == null) {
                            continue;
                        }

                        return mc.getValue(entry.valuePos, entry.valueSize, entry.flags == 1);
                    }
                } finally {
                    m_maxOutstandingReads.release();
                }
            }
        });
    }

    @Override
    public ListenableFuture<?> put(byte[] key, byte[] value) {
        return put(
                key,
                value,
                (m_compressByDefault ? PUTFLAG_COMPRESS : 0) | (m_syncByDefault ? PUTFLAG_SYNC : 0));
    }

    @Override
    public ListenableFuture<?> put(final byte[] key, final byte[] value,
            int flags) {
        final boolean compressValue = (flags & PUTFLAG_COMPRESS) != 0;
        final boolean waitForSync = (flags & PUTFLAG_SYNC) != 0;
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        ListenableFuture<Object[]> compressedValueTemp;
        if (compressValue) {
            compressedValueTemp = m_compressionThreads.submit(new Callable<Object[]>() {
                    @Override
                    public Object[] call() throws Exception {
                        final byte compressed[] = org.xerial.snappy.Snappy.compress(value);
                        final CRC32 crc = new CRC32();
                        crc.update(compressed);
                        return new Object[] { compressed, (int)crc.getValue() };
                    }
            });
        } else {
            compressedValueTemp = ListenableFutureTask.create(new Callable<Object[]>() {
                @Override
                public Object[] call() throws Exception {
                    final CRC32 crc = new CRC32();
                    crc.update(value);
                    return new Object[] { value, (int)crc.getValue() };
                }
            });
            ((ListenableFutureTask<Object[]>)compressedValueTemp).run();
        }
        final ListenableFuture<Object[]> compressedValue = compressedValueTemp;

        try {
            m_maxOutstandingWrites.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ListenableFuture<?> result = m_writeThread.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                final Object compressionResults[] = compressedValue.get();
                final byte compressionResult[] = (byte[])compressionResults[0];
                final int crc = (Integer)compressionResults[1];

                /*
                 * If compression didn't improve things, don't apply it
                 */
                if (compressValue && compressionResult.length >= value.length) {
                    CRC32 crcCalc = new CRC32();
                    crcCalc.update(value);
                    putImpl(key, value, (int)crcCalc.getValue(), false);
                } else {
                    putImpl(key, compressionResult, crc, compressValue);
                }
                return null;
            }
        });

        result.addListener(new Runnable() {
            @Override
            public void run() {
                m_maxOutstandingWrites.release();
            }
        },
        MoreExecutors.sameThreadExecutor());

        return result;
    }

    private void putImpl(byte key[], byte value[], int valueCRC, boolean compressedValue) throws IOException {
        final long timestamp = getNextTimestamp();
        if (!m_outCask.addEntry(timestamp, key, value, valueCRC, compressedValue, m_keyDir)) {
            m_outCask = new MiniCask(new File(m_caskPath, m_nextMiniCaskIndex + ".minicask"), m_nextMiniCaskIndex);
            m_miniCasks.put(m_nextMiniCaskIndex, m_outCask);
            m_nextMiniCaskIndex++;
            if (!m_outCask.addEntry(timestamp, key, value, valueCRC, compressedValue, m_keyDir)) {
                throw new IOException("Unable to place value in an empty bitcask, should never happen");
            }
        }
    }

    private static final long COUNTER_BITS = 6;
    private static final long TIMESTAMP_BITS = 57;
    private static final long TIMESTAMP_MAX_VALUE = (1L << TIMESTAMP_BITS) - 1L;
    private static final long COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;

    private long lastReturnedTime = System.currentTimeMillis();
    private long counter = 0;

    private long getNextTimestamp() {
        final long now = System.currentTimeMillis();

        if (now < lastReturnedTime) {
            if (now < (lastReturnedTime - 10000)) {
                System.err.println(
                        "Time traveled backwards more then 10 seconds from " +
                                lastReturnedTime + " to " + now);
                System.exit(-1);
            }
            while (now <= lastReturnedTime) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (now > lastReturnedTime) {
            counter = 0;
            lastReturnedTime = counter;
        } else {
            counter++;
            if (counter > COUNTER_MAX_VALUE) {
                while (now <= lastReturnedTime) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                counter = 0;
                lastReturnedTime = 0;
            }
        }
        return makeTimestampFromComponents(now, counter);
    }

    public static long makeTimestampFromComponents(long time, long counter) {
        long timestamp = time << COUNTER_BITS;
        assert(time < TIMESTAMP_MAX_VALUE);
        assert(counter < COUNTER_MAX_VALUE);
        timestamp |= counter;
        return timestamp;
    }

    public static long getCounterFromTimestamp(long timestamp) {
        return timestamp & COUNTER_MAX_VALUE;
    }

    public static long getTimeFromTimestamp(long timestamp) {
        return timestamp >> COUNTER_BITS;
    }

    @Override
    public ListenableFuture<?> remove(final byte[] key) {
        try {
            m_maxOutstandingWrites.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ListenableFuture<?> result = m_writeThread.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                putImpl(key, null, 0, true);
                return null;
            }
        });

        result.addListener(new Runnable() {
            @Override
            public void run() {
                m_maxOutstandingWrites.release();
            }
        },
        MoreExecutors.sameThreadExecutor());
        return result;
    }

    @Override
    public synchronized void close() throws IOException {
        m_readThreads.shutdown();
        m_writeThread.shutdown();
        try {
            m_readThreads.awaitTermination(365, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            m_writeThread.awaitTermination(365, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            for (MiniCask mc : m_miniCasks.values()) {
                try {
                    mc.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            m_miniCasks.clear();
            /*
             * Would really love those stupid fucking buffers to get unmapped
             * you ivory tower jackasses
             */
            for (int ii = 0; ii < 10; ii++) {
                System.gc();
            }
        }
    }

    @Override
    public Iterator<CaskEntry> iterator() {
        final Map<Integer, MiniCask> casks = m_miniCasks.get();

        if (casks.isEmpty()) {
            return new Iterator<CaskEntry>() {

                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public CaskEntry next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

            };
        }

        return new Iterator<CaskEntry>() {

            Iterator<MiniCask> caskIterator = casks.values().iterator();
            MiniCask currentCask = caskIterator.next();
            Iterator<CaskEntry> entryIterator = currentCask.iterator();

            @Override
            public boolean hasNext() {
                if (caskIterator == null) {
                    return false;
                }
                while (!entryIterator.hasNext()) {
                    if (caskIterator.hasNext()) {
                        currentCask = caskIterator.next();
                        entryIterator = currentCask.iterator();
                    } else {
                        caskIterator = null;
                        return false;
                    }
                }
                return true;
            }

            @Override
            public CaskEntry next() {
                if (caskIterator == null) {
                    throw new NoSuchElementException();
                }
                assert(entryIterator.hasNext());
                return entryIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

}
