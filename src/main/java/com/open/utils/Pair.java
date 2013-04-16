package com.open.utils;

public final class Pair<Item1, Item2> {

    private Item1 item1;
    private Item2 item2;

    public Pair(Item1 item1, Item2 item2) {
        this.item1 = item1;
        this.item2 = item2;
    }

    public Item1 getItem1() {
        return item1;
    }

    public Item2 getItem2() {
        return item2;
    }

    public void setItem1(Item1 item1) {
        this.item1 = item1;
    }

    public void setItem2(Item2 item2) {
        this.item2 = item2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (item1 != null ? !item1.equals(pair.item1) : pair.item1 != null) return false;
        if (item2 != null ? !item2.equals(pair.item2) : pair.item2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = item1 != null ? item1.hashCode() : 0;
        result = 31 * result + (item2 != null ? item2.hashCode() : 0);
        return result;
    }

    public static <Item1, Item2> Pair<Item1, Item2> pair(Item1 item1, Item2 item2) {
        return new Pair<>(item1, item2);
    }

    @Override
    public String toString() {
        return "Pair{" +
                "item1=" + item1 +
                ", item2=" + item2 +
                '}';
    }
}
