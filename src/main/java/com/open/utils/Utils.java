package com.open.utils;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 2/11/13
 * Time: 4:36 PM
 */
public class Utils {

    @SuppressWarnings("unchecked")
    public static <K, V, V1 extends V> Map<V1, Integer> frequency(Collection<Map<K, V>> collection, K key) {
        Map<V1, Integer> freq = new HashMap<>();
        for (Map<K, V> map : collection) {
            V1 value = (V1) map.get(key);
            if (value != null) {
                if (freq.containsKey(value)) {
                    freq.put(value, freq.get(value) + 1);
                } else freq.put(value, 1);
            }
        }
        return freq;
    }

    @SuppressWarnings("unchecked")
    public static <K, V, V1 extends V> Map<V1, Pair<Integer, List<Map<K, V>>>> frequencyWV(Collection<Map<K, V>> collection, K key) {
        Map<V1, Pair<Integer, List<Map<K, V>>>> freq = new HashMap<>();
        for (Map<K, V> map : collection) {
            V1 value = (V1) map.get(key);
            if (value != null) {
                if (freq.containsKey(value)) {
                    Pair<Integer, List<Map<K, V>>> v = freq.get(value);
                    v.getItem2().add(map);
                    freq.put(value, Pair.pair(v.getItem1() + 1, v.getItem2()));
                } else {
                    List<Map<K, V>> list = new ArrayList<>();
                    list.add(map);
                    freq.put(value, Pair.pair(1, list));
                }
            }
        }
        return freq;
    }

}
