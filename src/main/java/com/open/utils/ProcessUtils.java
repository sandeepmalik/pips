package com.open.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ProcessUtils {
    public static String execute(String... commands) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        processBuilder.inheritIO();
        Process process = processBuilder.start();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        StringBuilder stringBuilder = new StringBuilder();
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println("Process " + line);
            stringBuilder.append(line);
        }
        System.out.println("This is the result " + stringBuilder.toString());
        process.waitFor();
        return stringBuilder.toString();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        execute("curl", "-version");
    }
}
