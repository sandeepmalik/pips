package com.open.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class ThreadDump {

    public static String getThreadDump(boolean printToStdOut) {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        StringBuilder sb = new StringBuilder();
        long[] threadIds = threadBean.getAllThreadIds();
        //ThreadInfo[] info = threadBean.getThreadInfo(threadIds, 5);
        ThreadInfo[] info = threadBean.dumpAllThreads(true, true);
        for (ThreadInfo inf : info) {
            StackTraceElement[] str = inf.getStackTrace();
            if (str == null)
                continue;
            sb.append(inf).append("\n*******************************************\n");
        }
        if (printToStdOut) {
            System.out.println(sb);
        }
        return sb.toString();
    }
}
