package com.open.utils;

public final class Tuple<T1, T2, T3> {

    private final Pair<T1, T2> pair;
    private final T3 item;

    public Tuple(T1 item1, T2 item2, T3 item3) {
        pair = new Pair<>(item1, item2);
        this.item = item3;
    }

    public T1 getItem1() {
        return pair.getItem1();
    }

    public T2 getItem2() {
        return pair.getItem2();
    }

    public T3 getItem3() {
        return item;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple tuple = (Tuple) o;

        if (item != null ? !item.equals(tuple.item) : tuple.item != null) return false;
        if (pair != null ? !pair.equals(tuple.pair) : tuple.pair != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = pair != null ? pair.hashCode() : 0;
        result = 31 * result + (item != null ? item.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "item1=" + pair.getItem1() +
                ", item2=" + pair.getItem2() +
                ", item3=" + item +
                '}';
    }
}
