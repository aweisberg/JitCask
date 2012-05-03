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

import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.primitives.KeyDirUnsignedBytesComparator;

/*
 * Keys and values are the same object.
 * The first bytes are the key, the last KDEntry.SIZE bytes are the actual data.
 * If you are using real keys then you must decorate/undecorate.
 */
class KeyDir {

    static class SubKeyDir {
        private final ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock(true);
        final TreeMap<byte[], byte[]> m_keys =
                new TreeMap<byte[], byte[]>( new KeyDirUnsignedBytesComparator());
    }

    final HashMap<Integer, SubKeyDir> m_subKeyDirs = new HashMap<Integer, SubKeyDir>();
    private final int m_numPartitions;

    KeyDir(int partitions) {
        if (partitions < 1) {
            throw new IllegalArgumentException();
        }
        m_numPartitions = partitions;
        for (int ii = 0; ii < partitions; ii++) {
            m_subKeyDirs.put(ii, new SubKeyDir());
        }
    }

    private int hashByteBuffer(byte key[]) {
        int hash = 1;
        for (int ii = key.length - (1 + KDEntry.SIZE); ii >= 0; ii--)
            hash = 31 * hash + key[ii];
        return hash;
    }

    /*
     * Leaked to avoid locking for reload
     */
    SubKeyDir getSubKeyDir(byte key[]) {
        return m_subKeyDirs.get(Math.abs(hashByteBuffer(key)) % m_numPartitions);
    }

//    private boolean keysAreEqual(byte a[], byte b[]) {
//        if (a.length != b.length) {
//            return false;
//        }
//        for (int ii = 0; ii < a.length - KDEntry.SIZE; ii++) {
//            if (a[ii] != b[ii]) {
//                return false;
//            }
//        }
//        return true;
//    }

    /*
     * Does identity for expected.
     * Returns null on success and the unexpected value on failure
     */
//    KDEntry compareAndSet(byte expected[], byte update[]) {
//        final SubKeyDir subKeyDir = getSubKeyDir(key);
//        assert(update.length == expected.length);
//        assert(subKeyDir.m_lock.getWriteHoldCount() == 0);
//        assert(keysAreEqual(expected, update);
//
//        subKeyDir.m_lock.writeLock().lock();
//        byte found[] = null;
//        try {
//            found = subKeyDir.m_keys.get(update);
//            if (found == expected) {
//                if (found != null) {
//
//                }
//                subKeyDir.m_keys.put(update, update);
//                return null;
//            }
//        } finally {
//            subKeyDir.m_lock.writeLock().unlock();
//        }
//
//        if (found == null) {
//            return new KDEntry();
//        } else {
//            return new KDEntry(found);
//        }
//    }

    /*
     * I think that compareAndSet is the only thing actually needed since
     * the merge worker will be moving the storage location of keys
     */
//    /*
//     * Does identity for expected.
//     * Returns null on success and the unexpected value on failure
//     */
//    public KDEntry compareAndRemove(byte key[],  byte expected[]) {
//        m_lock.writeLock().lock();
//        byte found[] = null;
//        try {
//            found = m_keys.get(key);
//            if (found == expected) {
//                m_keys.remove(key);
//                return null;
//            }
//        } finally {
//            m_lock.writeLock().unlock();
//        }
//        return new KDEntry(found);
//    }
//
//    /*
//     * Does identity for expected.
//     * Returns null on success and the unexpected value on failure
//     */
//    public KDEntry putIfAbsent(byte key[], byte value[]) {
//        m_lock.writeLock().lock();
//        byte found[] = null;
//        try {
//            found = m_keys.get(key);
//            if (found != null) {
//                m_keys.put(key, value);
//                return null;
//            }
//        } finally {
//            m_lock.writeLock().unlock();
//        }
//        return new KDEntry(found);
//    }

    KDEntry get(byte key[]) {
        final SubKeyDir subKeyDir = getSubKeyDir(key);
        assert(subKeyDir.m_lock.getReadHoldCount() == 0);
        byte entry[] = null;
        subKeyDir.m_lock.readLock().lock();
        try {
            entry = subKeyDir.m_keys.get(key);
        } finally {
            subKeyDir.m_lock.readLock().unlock();
        }
        if (entry != null) {
            return new KDEntry(entry);
        } else {
            return null;
        }
    }

    void remove(byte key[]) {
        final SubKeyDir subKeyDir = getSubKeyDir(key);
        assert(subKeyDir.m_lock.getWriteHoldCount() == 0);
        subKeyDir.m_lock.writeLock().lock();
        try {
            subKeyDir.m_keys.remove(key);
        } finally {
            subKeyDir.m_lock.writeLock().unlock();
        }
    }

    void put(byte key[]) {
        final SubKeyDir subKeyDir = getSubKeyDir(key);
        assert(subKeyDir.m_lock.getWriteHoldCount() == 0);
        subKeyDir.m_lock.writeLock().lock();
        try {
            /*
             * Internally a new Map.Entry is not constructed by put
             * so the only way to really use just one byte array as key/value
             * is to remove the old one and then put the new one
             */
            subKeyDir.m_keys.remove(key);
            subKeyDir.m_keys.put(key, key);
        } finally {
            subKeyDir.m_lock.writeLock().unlock();
        }
    }

    static byte[] decorateKey(byte key[]) {
        return Arrays.copyOf(key, key.length + KDEntry.SIZE);
    }

    static byte[] undecorateKey(byte key[]) {
        return Arrays.copyOf(key, key.length - KDEntry.SIZE);
    }
}
