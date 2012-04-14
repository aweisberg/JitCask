package com.afewmoreamps.util;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Key set, value set, and entry set are all immutable as are their iterators.
 * Otherwise behaves as you would expect.
 */
public class COWMap<K, V> implements Map<K, V> {
    private final AtomicReference<Map<K, V>> m_map;

    public COWMap() {
        m_map = new AtomicReference<Map<K, V>>(Collections.unmodifiableMap(new HashMap<K, V>()));
    }

    public COWMap(Map<K, V> map) {
        if (map == null) {
            throw new IllegalArgumentException("Wrapped map cannot be null");
        }
        m_map = new AtomicReference<Map<K, V>>(Collections.unmodifiableMap(map));
    }

    @Override
    public int size() {
        return m_map.get().size();
    }

    public Map<? extends K, ? extends V> get() {
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
            Map<K, V> original = m_map.get();
            HashMap<K, V> copy = new HashMap<K, V>(m_map.get());
            V oldValue = (V)copy.put(key, value);
            if (m_map.compareAndSet(original, Collections.unmodifiableMap(copy))) {
                return oldValue;
            }
        }
    }

    @Override
    public V remove(Object key) {
        while (true) {
            Map<K, V> original = m_map.get();
            HashMap<K, V> copy = new HashMap<K, V>(m_map.get());
            V oldValue = (V)copy.remove(key);
            if (m_map.compareAndSet(original, Collections.unmodifiableMap(copy))) {
                return oldValue;
            }
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        while (true) {
            Map<K, V> original = m_map.get();
            HashMap<K, V> copy = new HashMap<K, V>(m_map.get());
            copy.putAll(m);
            if (m_map.compareAndSet(original, Collections.unmodifiableMap(copy))) {
                return;
            }
        }
    }

    @Override
    public void clear() {
        while (true) {
            Map<K, V> original = m_map.get();
            HashMap<K, V> replacement = new HashMap<K, V>();
            if (m_map.compareAndSet(original, Collections.unmodifiableMap(replacement))) {
                return;
            }
        }
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
