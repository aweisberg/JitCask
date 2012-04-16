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

import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import sun.security.util.ByteArrayLexOrder;


class KeyDir {

    static class SubKeyDir {
        private final ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock(true);
        final TreeMap<byte[], byte[]> m_keys =
                new TreeMap<byte[], byte[]>(new ByteArrayLexOrder());
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

    private int hashByteArray(byte key[]) {
        int hash = 1;
        for (int ii = key.length - 1; ii >= 0; ii--)
            hash = 31 * hash + (int)key[ii];
        return hash;
    }

    /*
     * Leaked to avoid locking for reload
     */
    SubKeyDir getSubKeyDir(byte key[]) {
        return m_subKeyDirs.get(Math.abs(hashByteArray(key)) % m_numPartitions);
    }

    /*
     * Does identity for expected.
     * Returns null on success and the unexpected value on failure
     */
    KDEntry compareAndSet(byte key[], byte expected[], byte update[]) {
        final SubKeyDir subKeyDir = getSubKeyDir(key);
        assert(subKeyDir.m_lock.getWriteHoldCount() == 0);

        subKeyDir.m_lock.writeLock().lock();
        byte found[] = null;
        try {
            found = subKeyDir.m_keys.get(key);
            if (found == expected) {
                subKeyDir.m_keys.put(key, update);
                return null;
            }
        } finally {
            subKeyDir.m_lock.writeLock().unlock();
        }

        if (found == null) {
            return new KDEntry();
        } else {
            return new KDEntry(found);
        }
    }

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
        subKeyDir.m_lock.readLock().lock();
        try {
            byte entry[] = subKeyDir.m_keys.get(key);
            if (entry != null) {
                return new KDEntry(entry);
            } else {
                return null;
            }
        } finally {
            subKeyDir.m_lock.readLock().unlock();
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

    void put(byte key[], byte value[]) {
        final SubKeyDir subKeyDir = getSubKeyDir(key);
        assert(subKeyDir.m_lock.getWriteHoldCount() == 0);
        subKeyDir.m_lock.writeLock().lock();
        try {
            subKeyDir.m_keys.put(key, value);
        } finally {
            subKeyDir.m_lock.writeLock().unlock();
        }
    }

}
