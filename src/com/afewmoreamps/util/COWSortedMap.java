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

import java.util.Map;
import java.util.NavigableMap;

import com.google.common.collect.ForwardingNavigableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedMap.Builder;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Key set, value set, and entry set are all immutable as are their iterators.
 * Otherwise behaves as you would expect.
 */
public class COWSortedMap<K extends Comparable<K>, V> extends ForwardingNavigableMap<K, V> implements NavigableMap<K, V> {
    private final AtomicReference<ImmutableSortedMap<K, V>> m_map;

    public COWSortedMap() {
        m_map = new AtomicReference<ImmutableSortedMap<K, V>>(ImmutableSortedMap.<K, V>naturalOrder().build());
    }

    public COWSortedMap(Map<K, V> map) {
        if (map == null) {
            throw new IllegalArgumentException("Wrapped map cannot be null");
        }
        m_map = new AtomicReference<ImmutableSortedMap<K, V>>(ImmutableSortedMap.<K, V>naturalOrder().putAll(map).build());
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
            ImmutableSortedMap<K, V> original = m_map.get();
            Builder<K, V> builder = ImmutableSortedMap.<K, V>naturalOrder();
            V oldValue = null;
            boolean replaced = false;
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    oldValue = entry.getValue();
                    builder.put(key, value);
                    replaced = true;
                } else {
                    builder.put(entry);
                }
            }
            if (!replaced) {
                builder.put(key, value);
            }
            ImmutableSortedMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return oldValue;
            }
        }
    }

    @Override
    public V remove(Object key) {
        while (true) {
            ImmutableSortedMap<K, V> original = m_map.get();
            Builder<K, V> builder = ImmutableSortedMap.<K, V>naturalOrder();
            V oldValue = null;
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (entry.getKey().equals(key)) {
                    oldValue = entry.getValue();
                } else {
                    builder.put(entry);
                }
            }
            ImmutableSortedMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original,copy)) {
                return oldValue;
            }
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        while (true) {
            ImmutableSortedMap<K, V> original = m_map.get();
            Builder<K, V> builder = ImmutableSortedMap.<K, V>naturalOrder();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (!m.containsKey(entry.getKey())) {
                    builder.put(entry);
                }
            }
            builder.putAll(m);
            ImmutableSortedMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original, copy)) {
                return;
            }
        }
    }

    @Override
    public void clear() {
        m_map.set(ImmutableSortedMap.<K, V>naturalOrder().build());
    }

    @Override
    public java.util.Map.Entry<K, V> pollFirstEntry() {
        while (true) {
            ImmutableSortedMap<K, V> original = m_map.get();
            Builder<K, V> builder = ImmutableSortedMap.<K, V>naturalOrder();
            final Map.Entry<K, V> firstEntry = original.firstEntry();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (!entry.equals(firstEntry)) {
                    builder.put(entry);
                }
            }
            ImmutableSortedMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original,copy)) {
                return firstEntry;
            }
        }
    }

    @Override
    public java.util.Map.Entry<K, V> pollLastEntry() {
        while (true) {
            ImmutableSortedMap<K, V> original = m_map.get();
            Builder<K, V> builder = ImmutableSortedMap.<K, V>naturalOrder();
            final Map.Entry<K, V> firstEntry = original.firstEntry();
            for (Map.Entry<K, V> entry : original.entrySet()) {
                if (!entry.equals(firstEntry)) {
                    builder.put(entry);
                }
            }
            ImmutableSortedMap<K, V> copy = builder.build();
            if (m_map.compareAndSet(original,copy)) {
                return firstEntry;
            }
        }
    }

    @Override
    protected NavigableMap<K, V> delegate() {
        return m_map.get();
    }
}
