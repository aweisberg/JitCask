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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Key set, value set, and entry set are all immutable as are their iterators.
 * Otherwise behaves as you would expect.
 */
public class COWMap<K, V> implements Map<K, V> {
    private final AtomicReference<ImmutableMap<K, V>> m_map;

    public COWMap() {
        m_map = new AtomicReference<ImmutableMap<K, V>>(new Builder<K, V>().build());
    }

    public COWMap(Map<K, V> map) {
        if (map == null) {
            throw new IllegalArgumentException("Wrapped map cannot be null");
        }
        m_map = new AtomicReference<ImmutableMap<K, V>>(new Builder<K, V>().putAll(map).build());
    }

    @Override
    public int size() {
        return m_map.get().size();
    }

    public Map<K, V> get() {
        return m_map.get();
    }

    @Override
    public boolean isEmpty() {
        return m_map.get().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return m_map.get().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return m_map.get().containsValue(value);
    }

    @Override
    public V get(Object key) {
        return m_map.get().get(key);
    }

    @Override
    public V put(K key, V value) {
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            Builder<K, V> builder = new Builder<K, V>();
            V oldValue = null;
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    oldValue = entry.getValue();
                    builder.put(key, value);
                } else {
                    builder.put(entry);
                }
            }
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return oldValue;
            }
        }
    }

    @Override
    public V remove(Object key) {
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            Builder<K, V> builder = new Builder<K, V>();
            V oldValue = null;
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    oldValue = entry.getValue();
                } else {
                    builder.put(entry);
                }
            }
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original,copy)) {
                return oldValue;
            }
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        while (true) {
            ImmutableMap<K, V> original = m_map.get();
            Builder<K, V> builder = new Builder<K, V>();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (!m.containsKey(entry.getKey())) {
                    builder.put(entry);
                }
            }
            builder.putAll(m);
            ImmutableMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return;
            }
        }
    }

    @Override
    public void clear() {
        m_map.set(new Builder<K, V>().build());
    }

    @Override
    public Set<K> keySet() {
        return m_map.get().keySet();
    }

    @Override
    public Collection<V> values() {
        return m_map.get().values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return m_map.get().entrySet();
    }
}
