package com.open.qbes.cache.mq;

import com.google.gson.Gson;
import com.open.utils.Log;
import com.open.utils.ThreadSafe;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.util.HashMap;

@ThreadSafe
public class PersistentQueue<T> implements Closeable {

    private static final Log log = Log.getLogger(PersistentQueue.class);

    private final RandomAccessFile dataStore;
    private final Class<T> dataClass;

    private final Gson gson = new Gson();

    private long readCursor = -1;

    private final String READ_FLAG = "2";
    private final String UNREAD_FLAG = "1";

    @SuppressWarnings("unchecked")
    public PersistentQueue() {
        this("pq.ds", (Class<T>) HashMap.class);
    }

    public PersistentQueue(String dataStoreName, Class<T> dataClass) {
        try {
            String dsName = System.getProperty("user.home") + File.separator + dataStoreName;
            if (new File(dsName).exists())
                log.info("Using existing data store %s", dsName);
            else {
                log.info("Created a data store at %s", dsName);
            }
            this.dataStore = new RandomAccessFile(dsName, "rwd");
            this.dataClass = dataClass;
        } catch (FileNotFoundException e) {
            log.error("Could not create data store", e);
            throw new IllegalStateException(e);
        }
    }

    private void init() throws IOException {
        initReadCursor();
    }

    public synchronized void write(T data) {

    }

    public synchronized T read() {
        return null;
    }

    public synchronized T[] read(int howMany) {
        return null;
    }

    public synchronized void write(T[] data) {

    }

    public synchronized void close() throws IOException {
        log.info("Closing the data store");
        dataStore.close();
    }

    protected byte[] serialize(T data) throws IOException {
        String jsonData = gson.toJson(data);
        readyForWrite();
        dataStore.writeUTF(UNREAD_FLAG);
        dataStore.writeUTF(",");
        dataStore.writeUTF(jsonData);
        dataStore.writeUTF(",");
        dataStore.writeUTF("\n");
        //TODO: may be store time stamps?
        return jsonData.getBytes();
    }

    private void readyForWrite() throws IOException {
        dataStore.seek(dataStore.length());
    }

    protected T deserialize() throws IOException {

        if (readCursor == -1)
            initReadCursor();

        long tmpReadCursor = readCursor;
        String[] parts = dataStore.readLine().split(",");
        T data = null;
        if (parts.length > 2) {
            gson.fromJson(parts[1], dataClass);
        }
        long newReadCursorPos = readCursor;

        // adjust the flag status:
        readCursor = tmpReadCursor;
        // it is important to understand here that this is actually an update operation
        // the sole benefit (at least in this context) of using RandomAccessFile
        // is to be able to write stuff at any
        // location and that data gets 'over written' over the other bytes
        // there by giving the same semantics of an update:
        dataStore.writeUTF(READ_FLAG);
        readCursor = newReadCursorPos;
        return data;
    }

    private void initReadCursor() throws IOException {
        // loop through the file and set the read cursor to the first UNREAD_FLAG:
        long dsSize = dataStore.length();
        while (readCursor < dsSize) {
            byte flag = dataStore.readByte();
            if (flag == 0) {
                readCursor = dataStore.getFilePointer() - 1;
                return;
            } else {
                int size = dataStore.readInt();
                dataStore.seek(dataStore.getFilePointer() + size);
            }
        }
        if (readCursor == -1)
            return;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        PersistentQueue ps = new PersistentQueue();

        ps.serialize("sandeep malik");

    }
}
