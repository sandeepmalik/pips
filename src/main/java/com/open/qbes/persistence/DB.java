package com.open.qbes.persistence;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.open.qbes.api.http.StatusCode;
import com.open.qbes.core.*;
import com.open.utils.ExceptionUtils;
import com.open.utils.Log;
import com.open.utils.Pair;
import com.open.utils.SyncUtils;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static java.lang.Boolean.valueOf;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 7/23/12
 * Time: 11:10 AM
 */
public class DB {

    private static final Log log = Log.getLogger(DB.class);

    private static Map<String, ConnectionProvider> pools;
    private static ConnectionProvider defaultPool;

    static {
        try {
            String driverName = System.getProperty("db.driver.name", "org.postgresql.Driver");
            log.info("Loading driver %s", driverName);
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError("No suitable driver found");
        }
    }

    public static void init() throws Exception {
        String[] dbPrefixes;
        if (getProperty("db.prefixes") == null) {
            dbPrefixes = new String[0];
        } else {
            dbPrefixes = getProperty("db.prefixes").split(",");
        }
        String defaultDBPrefix = getProperty("default.db.prefix", "");
        if (dbPrefixes.length > 0) {
            if (defaultDBPrefix == null)
                throw new IllegalArgumentException("No default db prefix specified");
            log.info("Configuring %s databases", Arrays.asList(dbPrefixes));
        } else {
            dbPrefixes = new String[]{""};
        }

        Map<String, ConnectionProvider> pools = new HashMap<>();

        for (String dbPrefix : dbPrefixes) {
            log.info("Configuring database %s", dbPrefix);
            pools.put(dbPrefix, configurePool(dbPrefix));
        }
        DB.pools = Collections.unmodifiableMap(pools);
        if (!pools.containsKey(defaultDBPrefix)) {
            throw new IllegalArgumentException("Default db prefix must match one of the given prefixes");
        }

        for (String poolPrefix : pools.keySet()) {
            final String poolPrefixName = poolPrefix;
            QueueService.getInstance().addServiceStatusResponder(new ServiceStatusResponder() {
                @Override
                public ServiceStatus getStatus() {
                    return new ServiceStatus() {
                        @Override
                        public String checkedAt() {
                            return new SimpleDateFormat("yyyy/MM/dd hh:mm:ss").format(new Date());
                        }

                        @Override
                        public String getServiceName() {
                            return poolPrefixName == null || poolPrefixName.trim().length() == 0 ? "Database" : "DataBase_" + poolPrefixName + "_";
                        }

                        @Override
                        public Pair<StatusCode, String> getStatus() {
                            try {
                                DB.fetch("SELECT 1");
                                return Pair.pair(StatusCode.OK, null);
                            } catch (Exception e) {
                                return Pair.pair(StatusCode.SERVICE_UNAVAILABLE, ExceptionUtils.getExceptionString(e));
                            }
                        }
                    };
                }
            });
        }
        defaultPool = pools.get(defaultDBPrefix);
    }

    private static ConnectionProvider configurePool(String prefix) throws SQLException {
        prefix = prefix.length() > 0 ? prefix + "." : prefix;
        BoneCPConfig config = new BoneCPConfig();
        config.setJdbcUrl(getProperty(prefix + "db.url"));
        config.setUsername(getProperty(prefix + "db.user"));
        config.setPassword(getProperty(prefix + "db.password"));
        config.setMaxConnectionsPerPartition(parseInt(getProperty(prefix + "db.max.connection", "50")));
        config.setMinConnectionsPerPartition(parseInt(getProperty(prefix + "db.min.connection", "5")));
        config.setPartitionCount(parseInt(getProperty(prefix + "db.partition.count", "1")));
        //config.setCloseConnectionWatch(valueOf(getProperty(prefix + "db.close.connection.watcher", "true")));
        config.setIdleMaxAge(parseInt(getProperty(prefix + "db.idle.timeout.mins", "5")), MINUTES);
        config.setLogStatementsEnabled(true || valueOf(getProperty(prefix + "db.log.sql")));
        config.setPoolName("QBES-ConnectionPool(" + prefix + ")");
        log.info("Disabling statement caching");
        config.setStatementsCacheSize(0);

        if (valueOf(getProperty("use.db.pool", "true"))) {
            return new BoneCPConnectionProvider(config);
        } else {
            log.info("No pool is configured. Connections will be created from database correctly");
            return new NoPoolConnectionProvider(config);
        }
    }

    static ThreadLocal<Connection> TRANSACTIONAL_CONNECTION = new ThreadLocal<Connection>() {
        @Override
        protected WatchedConnection initialValue() {
            return new WatchedConnection(newConnection0());
        }

        @Override
        public void remove() {
            try {
                TRANSACTIONAL_CONNECTION.get().close();
            } catch (SQLException e) {
                log.error("Could not close connection", e);
            } finally {
                super.remove();
            }
        }
    };
    static ThreadLocal<Boolean> TRANSACTION_FLAG = new ThreadLocal<>();


    public static <T> T atomic(final Job<T> job) throws Exception {
        if (TRANSACTION_FLAG.get() != null && TRANSACTION_FLAG.get()) {
            log.debug("Participating in an already running transaction");
            return job.call();
        }
        try {
            startTransaction();
            T result = job.call();
            commitTransaction();
            return result;
        } catch (Exception e) {
            rollbackTransaction(job);
            throw e;
        } finally {
            finishTransaction();
        }
    }

    public static abstract class Atomic<T> extends Lambda<T> {
        @Override

        protected final T execute() throws Exception {
            if (TRANSACTION_FLAG.get() != null && TRANSACTION_FLAG.get()) {
                log.debug("Participating in an already running transaction");
                return atomic();
            }
            try {
                startTransaction();
                T result = atomic();
                commitTransaction();
                return result;
            } catch (Exception e) {
                rollbackTransaction(this);
                throw e;
            } finally {
                finishTransaction();
            }
        }

        protected abstract T atomic() throws Exception;

        public String toString() {
            return "DB.Atomic[" + hashCode() + "]";
        }
    }

    private static void rollbackTransaction(Job job) throws SQLException {
        log.error("Rolling back transaction for Connection %s ", TRANSACTIONAL_CONNECTION.get());
        TRANSACTIONAL_CONNECTION.get().rollback();
    }

    private static void finishTransaction() throws SQLException {
        TRANSACTIONAL_CONNECTION.get().setAutoCommit(true);
        TRANSACTION_FLAG.remove();
        TRANSACTIONAL_CONNECTION.remove();
        log.debug("Transaction Finished");
    }

    private static void commitTransaction() throws SQLException {
        log.debug("Committing transaction for Connection %s", TRANSACTIONAL_CONNECTION.get());
        TRANSACTIONAL_CONNECTION.get().commit();
    }

    private static void startTransaction() throws SQLException {
        TRANSACTION_FLAG.set(true);
        TRANSACTIONAL_CONNECTION.get().setAutoCommit(false);
        log.debug("Initiating transaction with Connection %s", TRANSACTIONAL_CONNECTION.get());
    }

    public static abstract class BatchJob extends Lambda<int[]> {

        private final String batchSQL;
        protected final Timestamp currentTimestamp;

        protected BatchJob(String batchSQL) {
            this.batchSQL = batchSQL;
            currentTimestamp = SyncUtils.getCurrentJavaSqlTimestamp();
        }

        public void safeSetItem(PreparedStatement preparedStatement, Object value, int type, int index) throws Exception {
            SyncUtils.safeSetItem(preparedStatement, value, type, index);
        }

        public void safeSetItem(PreparedStatement preparedStatement, Object value, int type, Object defaultValue, int index) throws Exception {
            SyncUtils.safeSetItem(preparedStatement, value, type, defaultValue, index);
        }

        public Timestamp getCurrentTimestamp() {
            return currentTimestamp;
        }

        @Override
        protected int[] execute() throws Exception {
            try (Connection c = newConnection(); PreparedStatement preparedStatement = c.prepareStatement(batchSQL)) {
                doBatch(preparedStatement);
                return preparedStatement.executeBatch();
            }
        }

        public void setCreatedAtAndUpdatedAt(PreparedStatement preparedStatement, int createdAtIndex, int updatedAtIndex) throws Exception {
            safeSetItem(preparedStatement, currentTimestamp, Types.TIMESTAMP, createdAtIndex);
            safeSetItem(preparedStatement, currentTimestamp, Types.TIMESTAMP, updatedAtIndex);
        }


        public abstract void doBatch(PreparedStatement p) throws Exception;
    }

    public static Connection newConnection() {
        if (TRANSACTION_FLAG.get() == null) {
            Connection connection = newConnection0();
            log.debug("Acquired new connection %s", connection);
            return connection;
        } else {
            log.debug("Reusing connection %s", TRANSACTIONAL_CONNECTION.get());
            return TRANSACTIONAL_CONNECTION.get();
        }
    }

    public static ThreadLocal<String> DB_CONTEXT = new ThreadLocal<>();

    private static Connection newConnection0() {
        try {
            ConnectionProvider provider = DB_CONTEXT.get() == null ? defaultPool : pools.get(DB_CONTEXT.get());
            Connection c = provider.getConnection();
            return new WatchedConnection(c);
        } catch (SQLException e) {
            log.error("Error in getting a new connection");
            throw new UncheckedSQLException(e);
        }
    }


    public static List<Map<String, Object>> fetch(String statement) {
        return fill(statement, new ArrayList<Map<String, Object>>());
    }


    public static Map<String, Map<String, Object>> orthogonalize(String statement, String key, Object... params) {
        return orthogonalize(statement, key, String.class, params);
    }

    @SuppressWarnings("unchecked")
    public static <K> Set<K> unique(String statement, String key, Object... params) {
        Set<K> keys = (Set<K>) orthogonalize(statement, key, Object.class, params).keySet();
        Set<K> dup = Sets.newHashSet();
        dup.addAll(keys);
        return dup;
    }

    public static <K> Map<K, Map<String, Object>> orthogonalize(String statement, String key, Class<K> keyClass, Object... params) {
        log.debug("Executing orthogonalize for %s, and key %s, and params %s", statement, key, Arrays.asList(params));
        if (params.length > 0) {
            try (Connection c = newConnection(); PreparedStatement preparedStatement = c.prepareStatement(statement)) {
                setPreparedStmtParams(c, preparedStatement, params);
                ResultSet rs = preparedStatement.executeQuery();
                Map<K, Map<String, Object>> result = new HashMap<>();
                while (rs.next()) {
                    ResultSetMetaData resultSetMetaData = rs.getMetaData();
                    int cols = resultSetMetaData.getColumnCount();
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= cols; i++) {
                        row.put(resultSetMetaData.getColumnLabel(i), rs.getObject(i));
                    }
                    result.put(keyClass.cast(row.get(key)), row);
                }
                return result;
            } catch (SQLException e) {
                log.error("Error in fetching: %s", statement);
                throw new UncheckedSQLException(e);
            }
        } else {
            try (Connection c = newConnection(); Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(statement)) {
                Map<K, Map<String, Object>> result = new HashMap<>();
                while (rs.next()) {
                    ResultSetMetaData resultSetMetaData = rs.getMetaData();
                    int cols = resultSetMetaData.getColumnCount();
                    Map<String, Object> row = new HashMap<>();

                    for (int i = 1; i <= cols; i++) {
                        row.put(resultSetMetaData.getColumnLabel(i), rs.getObject(i));
                    }
                    result.put(keyClass.cast(row.get(key)), row);
                }
                return result;
            } catch (SQLException e) {
                log.error("Error in fetching: %s", statement);
                throw new UncheckedSQLException(e);
            }
        }
    }

    public static <K1, K2> BiMap<K1, K2> bimap(String statement, Class<K1> k1Class, Class<K2> k2Class, Object... params) {
        log.debug("Creating bimap for %s", statement);
        try (Connection c = newConnection(); PreparedStatement preparedStatement = c.prepareStatement(statement)) {
            setPreparedStmtParams(c, preparedStatement, params);
            BiMap<K1, K2> biMap = HashBiMap.create();
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                biMap.put(k1Class.cast(rs.getObject(1)), k2Class.cast(rs.getObject(2)));
            }
            return biMap;
        } catch (SQLException e) {
            log.error("Error in fetching: %s", statement);
            throw new UncheckedSQLException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K1, K2> BiMap<K1, K2> bimap(String statement, Object... params) {
        log.debug("Creating bimap for %s", statement);
        try (Connection c = newConnection(); PreparedStatement preparedStatement = c.prepareStatement(statement)) {
            setPreparedStmtParams(c, preparedStatement, params);
            BiMap<K1, K2> biMap = HashBiMap.create();
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                biMap.put((K1) rs.getObject(1), (K2) rs.getObject(2));
            }
            return biMap;
        } catch (SQLException e) {
            log.error("Error in fetching: %s", statement);
            throw new UncheckedSQLException(e);
        }
    }

    public static <K, V> Map<K, V> map(String statement, Class<K> keyClass, Class<V> valueClass, Object... params) {
        log.debug("Creating map for %s", statement);
        try (Connection c = newConnection(); PreparedStatement stmt = c.prepareStatement(statement)) {
            Map<K, V> map = new HashMap<>();
            setPreparedStmtParams(c, stmt, params);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(keyClass.cast(rs.getObject(1)), valueClass.cast(rs.getObject(2)));
            }
            return map;
        } catch (SQLException e) {
            log.error("Error in fetching: %s", statement);
            throw new UncheckedSQLException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> map(String statement, Object... params) {
        log.debug("Creating map for %s", statement);
        try (Connection c = newConnection(); PreparedStatement stmt = c.prepareStatement(statement)) {
            Map<K, V> map = new HashMap<>();
            setPreparedStmtParams(c, stmt, params);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                map.put((K) (rs.getObject(1)), (V) (rs.getObject(2)));
            }
            return map;
        } catch (SQLException e) {
            log.error("Error in fetching: %s", statement);
            throw new UncheckedSQLException(e);
        }
    }


    public static <T extends Collection<Map<String, Object>>> T fill(String statement, T collection) {
        log.debug("Executing fetch for " + statement);
        try (Connection c = newConnection(); Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(statement)) {
            transformResultSet(collection, rs);
            return collection;
        } catch (SQLException e) {
            log.error("Error in fetching %s ", statement);
            throw new UncheckedSQLException(e);
        }
    }


    public static <T extends Collection<Map<String, Object>>> void transformResultSet(T collection, ResultSet rs) throws SQLException {
        while (rs.next()) {
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int cols = resultSetMetaData.getColumnCount();
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= cols; i++) {
                row.put(resultSetMetaData.getColumnLabel(i), rs.getObject(i));
            }
            collection.add(row);
        }
    }

    // with the connection pooling, the hope is that this stmt is pooled as well:

    public static List<Map<String, Object>> fetch(String statement, Object... params) {
        log.debug("Executing prepared fetch for \"%s\" with (%s)", statement, asList(params));
        try (Connection c = newConnection(); PreparedStatement stmt = c.prepareStatement(statement)) {
            setPreparedStmtParams(c, stmt, params);
            ResultSet rs = stmt.executeQuery();
            List<Map<String, Object>> result = new ArrayList<>();
            transformResultSet(result, rs);
            return result;
        } catch (SQLException e) {
            log.error("Error in fetch call for statement %s ", statement);
            throw new UncheckedSQLException(e);
        }
    }

    private static void setPreparedStmtParams(Connection c, PreparedStatement stmt, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            if (param.getClass().isArray()) {
                Array a = c.createArrayOf("varchar", (Object[]) param);
                stmt.setArray(i + 1, a);
            } else {
                stmt.setObject(i + 1, param);
            }
        }
    }

    public static void shutdown() {
        for (Map.Entry<String, ConnectionProvider> entry : pools.entrySet()) {
            log.info("Shutting down pool %s", entry.getKey());
            entry.getValue().shutdown();
        }
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        log.info("De-registering drivers");
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            try {
                log.debug("De-registering DB driver %s", driver);
                DriverManager.deregisterDriver(driver);
            } catch (SQLException e) {
                log.error("Error in de-registering a driver %s", e, driver);
            }
        }
    }

    // could be a create ot delete query as well:

    public static int update(String updateQuery) {
        log.debug("Updating with " + updateQuery);
        try (Connection c = newConnection(); Statement stmt = c.createStatement()) {
            return stmt.executeUpdate(updateQuery);
        } catch (Exception e) {
            log.error("Error in update call for %s", updateQuery);
            throw new UncheckedSQLException(e);
        }
    }


    public static int update(String updateQuery, Object... params) {
        log.debug("Executing prepared update with \"%s\" with (%s)", updateQuery, asList(params));
        try (Connection c = newConnection(); PreparedStatement stmt = c.prepareStatement(updateQuery)) {
            setPreparedStmtParams(c, stmt, params);
            return stmt.executeUpdate();
        } catch (Exception e) {
            log.error("Error in prepared update %s", updateQuery);
            throw new UncheckedSQLException(e);
        }
    }


    public static int update(PreparedStatement updateQuery) throws SQLException {
        log.debug("Updating with " + updateQuery);
        try (PreparedStatement dup = updateQuery) {
            return dup.executeUpdate();
        } catch (SQLException e) {
            log.error("Error in prepared update %s", updateQuery);
            throw new UncheckedSQLException(e);
        }
    }

    private static class NoPoolConnectionProvider implements ConnectionProvider {

        private final BoneCPConfig config;

        public NoPoolConnectionProvider(BoneCPConfig config) throws SQLException {
            this.config = config;
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUsername(), config.getPassword());
            return connection;
        }

        @Override
        public void shutdown() {
            //no-op
        }
    }

    private static class BoneCPConnectionProvider extends BoneCP implements ConnectionProvider {
        public BoneCPConnectionProvider(BoneCPConfig config) throws SQLException {
            super(config);
        }
    }

    private static interface ConnectionProvider {
        Connection getConnection() throws SQLException;

        void shutdown();
    }
}
