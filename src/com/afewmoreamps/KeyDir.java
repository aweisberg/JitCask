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

import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import sun.security.util.ByteArrayLexOrder;


public class KeyDir {
    final ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock(true);
    private final TreeMap<byte[], byte[]> m_keys =
            new TreeMap<byte[], byte[]>(new ByteArrayLexOrder());

    /*
     * Does identity for expected.
     * Returns null on success and the unexpected value on failure
     */
    public KDEntry compareAndSet(byte key[], byte expected[], byte update[]) {
        assert(m_lock.getWriteHoldCount() == 0);
        m_lock.writeLock().lock();
        byte found[] = null;
        try {
            found = m_keys.get(key);
            if (found == expected) {
                m_keys.put(key, update);
                return null;
            }
        } finally {
            m_lock.writeLock().unlock();
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

    public KDEntry get(byte key[]) {
        assert(m_lock.getReadHoldCount() == 0);
        m_lock.readLock().lock();
        try {
            byte entry[] = m_keys.get(key);
            if (entry != null) {
                return new KDEntry(entry);
            } else {
                return null;
            }
        } finally {
            m_lock.readLock().unlock();
        }
    }

    public void remove(byte key[]) {
        assert(m_lock.getWriteHoldCount() == 0);
        m_lock.writeLock().lock();
        try {
            m_keys.remove(key);
        } finally {
            m_lock.writeLock().unlock();
        }
    }

    public void put(byte key[], byte value[]) {
        assert(m_lock.getWriteHoldCount() == 0);
        m_lock.writeLock().lock();
        try {
            m_keys.put(key, value);
        } finally {
            m_lock.writeLock().unlock();
        }
    }

    /**
     * Leak the keydir at startup to bypass locking
     * since it is single threaded
     * @return
     */
    public TreeMap<byte[], byte[]> leakKeyDir() {
        return m_keys;
    }
}
