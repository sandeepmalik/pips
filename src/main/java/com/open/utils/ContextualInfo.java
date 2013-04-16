package com.open.utils;

import com.open.qbes.core.JobContext;

public final class ContextualInfo<T> {
    public static Object format() {
        if (JobContext.getContext() == null)
            return "";
        if (JobContext.getContext().getInfo() == null || JobContext.getContext().getInfo().toString().trim().length() == 0)
            return "[Context/" + JobContext.getContext().hashCode() + "] ";
        return JobContext.getContext().getInfo() + " ";
        /*
        if (true) {
            StringBuilder sb = new StringBuilder();
            ExecutionContext context = getContext();
            while (context != null) {
                sb.append("[Context/" + context.hashCode() + "]/");
                context = context.getSuper();
            }
            return sb.toString();
        } else return "NOT_SET";
         */
    }
}
