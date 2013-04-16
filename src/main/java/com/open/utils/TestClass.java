package com.open.utils;

public class TestClass {

    public static void main(String[] args) throws Exception {

        try (X x = new X()) {
            System.out.println("print");
            throw new RuntimeException("ddd");
        }
    }


    private static class X implements AutoCloseable {

        @Override
        public void close() throws Exception {
            System.out.println("Got close call");
        }
    }

}
