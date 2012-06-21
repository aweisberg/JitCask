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
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.afewmoreamps.KeyDir.SubKeyDir;
import com.afewmoreamps.util.COWMap;
import com.afewmoreamps.util.COWSortedMap;
import com.afewmoreamps.util.SettableFuture;
import com.google.common.util.concurrent.*;

class JitCaskImpl implements JitCask, Iterable<CaskEntry> {

    /**
     * If this flag is set then put will not return until
     * the data is fsynced
     */
    public static final int PUTFLAG_SYNC = 1 << 1;

    private final File m_caskPath;

    private final ListeningExecutorService m_writeThread;
    private final ListeningExecutorService m_compressionThreads;
    private final ListeningExecutorService m_readThreads;
    private final ListeningScheduledExecutorService m_syncThread;

    private ScheduledFuture<?> m_syncTaskRunner;
    private ListenableFutureTask<Long> m_nextSyncTask;

    private final KeyDir m_keyDir = new KeyDir(READ_QUEUE_DEPTH);

    private final COWSortedMap<Integer, MiniCask> m_miniCasks = new COWSortedMap<Integer, MiniCask>();

    private MiniCask m_outCask;

    private int m_nextMiniCaskIndex = 0;

    private final boolean m_syncByDefault;
    private final int m_syncInterval;
    private final int m_maxValidValueSize;

    private static final int READ_QUEUE_DEPTH = 64;
    private final Semaphore m_maxOutstandingReads = new Semaphore(READ_QUEUE_DEPTH);
    private final Semaphore m_maxOutstandingWrites = new Semaphore(64);
    private final ConcurrentLinkedQueue<MiniCask> m_finishedCasksPendingSync =
        new ConcurrentLinkedQueue<MiniCask>();

    public JitCaskImpl(CaskConfig config) throws IOException {
        m_syncByDefault = config.syncByDefault;
        m_syncInterval = config.syncInterval;
        m_caskPath = config.caskPath;
        m_maxValidValueSize = config.maxValidValueSize;

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
        m_outCask = new MiniCask(
                m_caskPath,
                m_nextMiniCaskIndex,
                m_maxValidValueSize);
        m_miniCasks.put(m_nextMiniCaskIndex, m_outCask);
        m_nextMiniCaskIndex++;

        m_syncTaskRunner = m_syncThread.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ListenableFutureTask<Long> currentSyncTask = m_nextSyncTask;
                    m_nextSyncTask = ListenableFutureTask.create(new Callable<Long>() {
                        @Override
                        public Long call() {
                            /*
                             * Catch throwable since we don't ever want to stop syncing.
                             */
                            try {
                                final long  start = System.currentTimeMillis();
                                sync();
                                final long end = System.currentTimeMillis();
                                final long delta = System.currentTimeMillis() - start;
                                if (delta > m_syncInterval) {
                                    System.err.println("Missed sync interval by " + delta);
                                }
                                return end;
                            } catch (Throwable t) {
                                t.printStackTrace();
                                return Long.MIN_VALUE;
                            }
                        }
                    });
                    if (currentSyncTask != null) {
                        currentSyncTask.run();
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
            if (f.getName().endsWith(".hintcask")) continue;
            if (!f.getName().endsWith(".minicask")) {
                throw new IOException("Unrecognized file " + f + " found in cask directory");
            }
            int caskIndex = Integer.valueOf(f.getName().substring(0, f.getName().length() - 9));
            highestIndex = Math.max(caskIndex, highestIndex);
            MiniCask cask =
                new MiniCask(
                        f.getParentFile(),
                        caskIndex,
                        m_maxValidValueSize);
            m_miniCasks.put(caskIndex, cask);
        }
        m_nextMiniCaskIndex = highestIndex + 1;

        for (MiniCask miniCask : m_miniCasks.values()) {
            final Iterator<CaskEntry> iter = miniCask.getReloadIterator();
            while (iter.hasNext()) {
                final CaskEntry ce = iter.next();
                final byte keyHashBytes[] = new byte[KDEntry.SIZE];
                System.arraycopy(ce.keyHash, 0, keyHashBytes, 0, 20);
                final SubKeyDir subKeyDir = m_keyDir.getSubKeyDir(keyHashBytes);

                if (ce.valuePosition == -1) {
                    subKeyDir.m_keys.remove(keyHashBytes);
                    continue;
                }

                KDEntry.toBytes(
                        keyHashBytes,
                        ce.miniCask.m_fileId,
                        ce.valuePosition,
                        ce.flags);
                subKeyDir.m_keys.put(
                        keyHashBytes,
                        keyHashBytes);
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
    public ListenableFuture<GetResult> get(final byte[] key) {
        final long start = System.currentTimeMillis();

        try {
            m_maxOutstandingReads.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return m_readThreads.submit(new Callable<GetResult>() {
            @Override
            public GetResult call() throws Exception {
                try {
                    while (true) {
                        MessageDigest md = MessageDigest.getInstance("SHA-1");
                        byte keyHash[] = md.digest(key);
                        final byte decoratedKey[] = KeyDir.decorateKeyHash(keyHash);
                        KDEntry entry = m_keyDir.get(decoratedKey);
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

                        /*
                         * Potentially compressed and uncompressed value, or just the uncompressed
                         * value wrapped in a ListenableFutureTask
                         */
                        ByteBuffer value = mc.getValue(entry.valuePos);
                        return new GetResult(
                                key,
                                value,
                                (int)(System.currentTimeMillis() - start));
                    }
                } finally {
                    m_maxOutstandingReads.release();
                }
            }
        });
    }

    @Override
    public ListenableFuture<PutResult> put(byte[] key, byte[] value) {
        return put(
                key,
                value,
                (m_syncByDefault ? PUTFLAG_SYNC : 0));
    }

    @Override
    public ListenableFuture<PutResult> put(final byte[] key, final byte[] value,
            int flags) {
        if (key == null || value == null) {
            throw new IllegalArgumentException();
        }

        final int uncompressedSize =
                key.length + value.length + MiniCask.HEADER_SIZE + 8;//8 is the two length prefixes in the compressed entry
        /*
         * Record when the put started
         */
        final long start = System.currentTimeMillis();

        /*
         * Parse out the flag for whether the future should be set
         * when the data is synced or whether it should be set
         * as soon as the data is written to the file descriptor
         */
        final boolean waitForSync = (flags & PUTFLAG_SYNC) != 0;

        /*
         * This is the return value that will be set with the result
         * or any exceptions thrown during the put
         */
        final SettableFuture<PutResult> retval = SettableFuture.create();

        /*
         * If compression is requested, attempt to compress the value
         * and generate the CRC in a separate thread pool before submitting
         * to the single write thread. This allows parallelism for what is potentially
         * the more CPU intensive part of a write. Can't have more write
         * threads so best to scale out as far as possible before giving it work.
         */
        final ListenableFuture<Object[]> assembledEntryFuture =
            m_compressionThreads.submit(new Callable<Object[]>() {
                @Override
                public Object[] call() throws Exception {
                    return MiniCask.constructEntry(key, value);
                }
        });

        /*
         * Limit the maximum number of outstanding writes
         * to avoid OOM in naive benchmarks/applications
         */
        try {
            m_maxOutstandingWrites.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        retval.addListener(new Runnable() {
            @Override
            public void run() {
                m_maxOutstandingWrites.release();
            }
        },
        MoreExecutors.sameThreadExecutor());

        /*
         * Submit the write to the single write thread
         * which will write the kv pair to the current file and
         * upsert it into the keydir. If sync was not requested
         * then the retval future will be set as soon as the write
         * thread writes the new kv pair to the memory mapped file (page cache).
         * Otherwise it adds a listener to the next sync task that will set the value
         * once sync has been performed.
         */
        m_writeThread.execute(new Runnable() {
            @Override
            public void run() {
                /*
                 * Retrieve the compression results, forwarding any exceptions
                 * to the retval future.
                 */
                Object assembledEntry[];
                try {
                    assembledEntry = assembledEntryFuture.get();
                } catch (Throwable t) {
                    retval.setException(t);
                    return;
                }

                final int crc = ((Integer)assembledEntry[0]).intValue();
                final byte entryBytes[] = (byte[])assembledEntry[1];
                final byte keyHash[] = (byte[])assembledEntry[2];

                try {
                    putImpl( crc, entryBytes, keyHash, false);
                } catch (Throwable t) {
                    retval.setException(t);
                    return;
                }

                /*
                 * If the put requested waiting for sync then don't set the retval future
                 * immediately. Add a listener for the next sync task that will do it
                 * once the data is really durable.
                 *
                 * Otherwise set it immediately and use the current time to reflect the latency
                 * of the put
                 */
                if (waitForSync) {
                    final ListenableFuture<Long> syncTask = m_nextSyncTask;
                    syncTask.addListener( new Runnable() {
                        @Override
                        public void run() {
                            try {
                                retval.set(
                                        new PutResult(
                                                uncompressedSize,
                                                entryBytes.length + MiniCask.HEADER_SIZE,
                                                (int)(syncTask.get() - start)));
                            } catch (Throwable t) {
                                retval.setException(t);
                                return;
                            }
                        }
                    }, MoreExecutors.sameThreadExecutor());
                } else {
                    retval.set(
                        new PutResult(
                            uncompressedSize,//WHat goes into the entry, 4 is the CRC
                            entryBytes.length + MiniCask.HEADER_SIZE,
                             (int)(System.currentTimeMillis() - start)));
                }
            }
        });

        return retval;
    }

    private void putImpl(
            int crc,
            byte entry[],
            byte keyHash[],
            boolean isTombstone) throws IOException {
        assert(keyHash.length == 20);
        if (!m_outCask.addEntry(crc, entry, keyHash, (byte)0, isTombstone, m_keyDir)) {
            m_outCask = new MiniCask(
                    new File(m_caskPath, m_nextMiniCaskIndex + ".minicask"),
                    m_nextMiniCaskIndex,
                    m_maxValidValueSize);
            m_miniCasks.put(m_nextMiniCaskIndex, m_outCask);
            m_nextMiniCaskIndex++;
            if (!m_outCask.addEntry(crc, entry, keyHash, (byte)0, isTombstone, m_keyDir)) {
                throw new IOException("Unable to place value in an empty bitcask, should never happen");
            }
        }
    }

    @Override
    public ListenableFuture<RemoveResult> remove(final byte key[]) {
        return remove(key, (m_syncByDefault ? PUTFLAG_SYNC : 0));
    }

    @Override
    public ListenableFuture<RemoveResult> remove(final byte[] key, int flags) {
        if (key == null) {
            throw new IllegalArgumentException();
        }

        /*
         * Parse out the flag for whether the future should be set
         * when the data is synced or whether it should be set
         * as soon as the data is written to the file descriptor
         */
        final boolean waitForSync = (flags & PUTFLAG_SYNC) != 0;
        final long start = System.currentTimeMillis();

        try {
            m_maxOutstandingWrites.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final SettableFuture<RemoveResult> retval = SettableFuture.create();
        retval.addListener(new Runnable() {
            @Override
            public void run() {
                m_maxOutstandingWrites.release();
            }
        },
        MoreExecutors.sameThreadExecutor());

        Object assembledEntryTemp[];
        try {
            assembledEntryTemp = MiniCask.constructEntry(key, null);
        } catch (IOException e) {
            retval.setException(e);
            return retval;
        }
        final Object assembledEntry[] = assembledEntryTemp;

        m_writeThread.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    putImpl(((Integer)assembledEntry[0]).intValue(), (byte[])assembledEntry[1], (byte[])assembledEntry[2], true);
                } catch (Throwable t) {
                    retval.setException(t);
                    return;
                }
                if (waitForSync) {
                    final ListenableFuture<Long> syncTask = m_nextSyncTask;
                    syncTask.addListener(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                retval.set(
                                        new RemoveResult(
                                                (int)(syncTask.get() - start)));
                            } catch (Throwable t) {
                                retval.setException(t);
                                return;
                            }
                        }
                    }, MoreExecutors.sameThreadExecutor());
                } else {
                    retval.set(
                            new RemoveResult(
                                    (int)(System.currentTimeMillis() - start)));
                }
            }
        });
        return retval;
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
        m_syncTaskRunner.cancel(false);

        m_syncThread.shutdown();
        try {
            m_syncThread.awaitTermination(365, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ListenableFutureTask<Long> syncTask = m_nextSyncTask;
        m_nextSyncTask = null;

        /*
         * Very cheesy hack to make sure the reference to m_nextSyncTask it was leaked is done being used.
         * I don't think it can actually be leaked because the write thread is shutdown
         * and it is the only one that is supposed to register listeners
         */
        try {
            Thread.sleep(200);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /*
         * Run the last sync task to make sure any dangling listeners are synced and notified
         */
        syncTask.run();

        /*
         * Now close all the files
         */
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
             * Would really love those buffers to get unmapped
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
