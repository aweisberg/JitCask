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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

import com.afewmoreamps.util.COWMap;
import com.google.common.util.concurrent.*;

class JitCaskImpl implements JitCask {

    private final File m_caskPath;

    private final ListeningExecutorService m_writeThread;
    private final ListeningExecutorService m_compressionThreads;
    private final ListeningExecutorService m_readThreads;

    private static class CaskEntry {
        final MiniCask miniCask;
        final long timestamp;
        final byte flags;
        final byte key[];
        final int valuePosition;
        final int valueLength;
        final byte[] valueCompressed;
        private final FutureTask<byte[]> valueUncompressed;

        public CaskEntry(
                MiniCask miniCask,
                long timestamp,
                byte flags,
                byte key[],
                int valuePosition,
                int valueLength,
                byte[] valueCompressed,
                FutureTask<byte[]> valueUncompressed) {
            this.miniCask = miniCask;
            this.timestamp = timestamp;
            this.flags = flags;
            this.key = key;
            this.valuePosition = valuePosition;
            this.valueLength = valueLength;
            this.valueCompressed = valueCompressed;
            this.valueUncompressed = valueUncompressed;
        }

        public byte[] getValueUncompressed() {
            valueUncompressed.run();
            try {
                return valueUncompressed.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class MiniCask implements Iterable<CaskEntry> {
        /*
         * Read locks are shared by readers and appenders. The write lock is for deleting the file
         * from the merge thread. It must acquire the write lock on a file before deleting it.
         * The locks aren't necessary if there is no merge thread since files are never mutated/removed/closed
         * without one.
         */
        private final ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock(true);
        private final File m_path;
        private final FileChannel m_outChannel;
        private final MappedByteBuffer m_buffer;
        private final int HEADER_SIZE = 15;//8-byte timestamp, 2-byte key length, 4-byte value length, 1-byte flags
        private final ByteBuffer headerBytes = ByteBuffer.allocate(HEADER_SIZE);


        public MiniCask(File path) throws IOException {
            m_path = path;
            RandomAccessFile ras = new RandomAccessFile(m_path, "rw");
            m_outChannel = ras.getChannel();
            m_buffer = m_outChannel.map(MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
        }

        public boolean addEntry(long timestamp, byte key[], byte value[], boolean compressedValue) throws IOException {
            assert(m_lock.getReadHoldCount() == 1);
            if (m_buffer.remaining() < HEADER_SIZE + key.length + value.length) {
                return false;
            }
            final CRC32 crc = new CRC32();
            headerBytes.clear();
            headerBytes.putLong(timestamp);

            /*
             * Compression bit in flags byte
             */
            if (compressedValue) {
                headerBytes.put((byte)1);
            } else {
                headerBytes.put((byte)0);
            }

            final short keyLength = (short)key.length;
            headerBytes.putShort(keyLength);
            headerBytes.putInt(value.length);
            crc.update(headerBytes.array());
            crc.update(key);
            crc.update(value);
            final int crcResult = (int)crc.getValue();

            m_buffer.putInt(crcResult);
            m_buffer.putLong(timestamp);
            m_buffer.putShort(keyLength);
            m_buffer.putInt(value.length);
            m_buffer.put(key);
            m_buffer.put(value);
            return true;
        }

        public byte[] getEntry(int position, int length, boolean decompress) throws IOException {
            assert(m_lock.getReadHoldCount() == 1);
            assert(position < m_outChannel.size());
            if (!m_outChannel.isOpen()) {
                throw new IOException("closed");
            }
            byte outBytes[] = new byte[length];
            ByteBuffer dup = m_buffer.asReadOnlyBuffer();
            dup.position(position);
            dup.get(outBytes);
            if (decompress) {
                outBytes = org.xerial.snappy.Snappy.uncompress(outBytes);
            }
            return outBytes;
        }

        public Iterator<CaskEntry> iterator() {
            assert(m_lock.getReadHoldCount() == 1);
            final ByteBuffer view = m_buffer.asReadOnlyBuffer();
            try {
                view.limit((int)m_outChannel.size());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
                        header.put(view);
                        header.flip();
                        assert(header.limit() == header.capacity());
                        final int originalCRC = header.getInt();
                        final long timestamp = header.getLong();
                        final byte flags = header.get();
                        final short keySize = header.getShort();
                        final int valueSize = header.getInt();
                        final CRC32 crc = new CRC32();
                        crc.update(header.array(), 4, HEADER_SIZE);
                        if (view.remaining() < keySize + valueSize) {
                            return null;
                        }
                        final byte keyBytes[] = new byte[keySize];
                        final byte valueBytes[] = new byte[valueSize];
                        view.get(keyBytes);
                        final int valuePosition = view.position();
                        view.get(valueBytes);
                        crc.update(keyBytes);
                        crc.update(valueBytes);
                        final int currentCRC = (int)crc.getValue();

                        if (currentCRC != originalCRC) {
                            System.err.println("Had a corrupt entry in " + m_path + " at position " + startPosition);
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


    }

    private final COWMap<Integer, MiniCask> m_miniCasks = new COWMap<Integer, MiniCask>();

    private MiniCask m_outCask;

    private int m_nextMiniCaskIndex = 0;

    private final boolean m_compressByDefault;

    private final Semaphore m_maxOutstandingWrites = new Semaphore(64);

    public JitCaskImpl(File caskPath, boolean compressByDefault) throws IOException {
        m_caskPath = caskPath;
        m_compressByDefault = compressByDefault;
        if (!caskPath.exists()) {
            throw new IOException("Path " + caskPath + " does not exist");
        }
        if (caskPath.isDirectory()) {
            throw new IOException("Path " + caskPath + " exists but is not a directory");
        }
        if (!caskPath.canRead()) {
            throw new IOException("Path " + caskPath + " is not readable");
        }
        if (!caskPath.canWrite()) {
            throw new IOException("Path " + caskPath + " is not writable");
        }
        if (!caskPath.canExecute()) {
            throw new IOException("Path " + caskPath + " is not executable");
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
                        new LinkedBlockingQueue<Runnable>(96),
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
                        32,
                        32,
                        0,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(128),
                        tf,
                        new ThreadPoolExecutor.CallerRunsPolicy()));

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
                                new LinkedBlockingQueue<Runnable>(availableProcs * 2),
                                tf,
                                new ThreadPoolExecutor.CallerRunsPolicy()));
    }

    @Override
    public void open() throws IOException {
        m_outCask = new MiniCask(new File(m_caskPath, m_nextMiniCaskIndex + ".bitcask"));
        m_nextMiniCaskIndex++;
        m_writeThread.execute(new Runnable() {
            @Override
            public void run() {
                m_outCask.m_lock.readLock().lock();
            }
        });
        m_miniCasks.put(m_nextMiniCaskIndex, m_outCask);
    }

    @Override
    public ListenableFuture<byte[]> get(byte[] key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListenableFuture<?> put(byte[] key, byte[] value) {
        return put (key, value, m_compressByDefault);
    }

    @Override
    public ListenableFuture<?> put(final byte[] key, final byte[] value,
            final boolean compressValue) {
        ListenableFuture<byte[]> compressedValueTemp;
        if (compressValue) {
            compressedValueTemp = m_compressionThreads.submit(new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        return org.xerial.snappy.Snappy.compress(value);
                    }
            });
        } else {
            compressedValueTemp = ListenableFutureTask.create(new Callable<byte[]>() {
                @Override
                public byte[] call() throws Exception {
                    return value;
                }
            });
            ((ListenableFutureTask<byte[]>)compressedValueTemp).run();
        }
        final ListenableFuture<byte[]> compressedValue = compressedValueTemp;

        try {
            m_maxOutstandingWrites.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ListenableFuture<Object> result = m_writeThread.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                putImpl(key, compressedValue.get(), compressValue);
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

    private void putImpl(byte key[], byte value[], boolean compressedValue) throws IOException {
        final long timestamp = getNextTimestamp();
        if (!m_outCask.addEntry(timestamp, key, value, compressedValue)) {
            m_outCask.m_lock.readLock().unlock();
            assert(m_outCask.m_lock.getReadHoldCount() == 0);
            m_outCask = new MiniCask(new File(m_caskPath, m_nextMiniCaskIndex + ".bitcask"));
            m_nextMiniCaskIndex++;
            m_outCask.m_lock.readLock().lock();
            m_miniCasks.put(m_nextMiniCaskIndex, m_outCask);
            if (!m_outCask.addEntry(timestamp, key, value, compressedValue)) {
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
    public ListenableFuture<?> remove(byte[] key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListenableFuture<byte[]> getAndRemove(byte[] key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sync() throws IOException {
        // TODO Auto-generated method stub

    }

}
