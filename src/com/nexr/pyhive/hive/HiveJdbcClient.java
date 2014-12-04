package com.nexr.pyhive.hive;

import com.nexr.pyhive.model.DataFrameModel;
import com.nexr.pyhive.model.ResultSetModel;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

public class HiveJdbcClient implements JDBCOperation {
    private DatabaseConnection databaseConnection;
    private boolean forServer2;

    public HiveJdbcClient(boolean forServer2) {
        this.forServer2 = forServer2;
    }

    public void connect(String host, int port, String db, String user, String password) throws SQLException {
        System.out.println("HiveJdbcClient.connect1");

        Properties properties = new Properties();
        if (user != null) {
            properties.setProperty("user", user);
        }

        if (password != null) {
            properties.setProperty("user", user);
        }

        connect(host, port, db, properties);
    }

    public void connect(String host, int port, String db, Properties properties) throws SQLException {

        System.out.println("HiveJdbcClient.connect2");

        HiveJdbcConnector hiveConnector = new HiveJdbcConnector(host, port, db, properties);
        hiveConnector.setDaemon(false);
        hiveConnector.start();
        try {
            hiveConnector.join(10000);
        } catch (InterruptedException e) {
            throw new SQLException(e);
        }

        if (hiveConnector.isAlive()) {
            int version = getVersion();
            throw new SQLException(
                    String.format(
                            "Connection to hive server has timed out.\n" +
                                    "\t--connection-parameters(version: hive server%d, host:%s, port:%d, db:%s, properties:%s)\n\n" +
                                    "Restart R session and retry with correct arguments.",
                            version, host, port, db, properties));
        }
    }


    public void close() throws SQLException {
        if (databaseConnection != null) {
            databaseConnection.close();
        }
    }

    DatabaseConnection getDatabaseConnection() {
        return databaseConnection;
    }


    public Connection getConnection(boolean reconnect) throws SQLException {
        checkConnection();
        DatabaseConnection connection = getDatabaseConnection();
        return connection.getConnection(reconnect);
    }

    DatabaseMetaData getDatabaseMetaData() throws SQLException {
        checkConnection();
        DatabaseConnection connection = getDatabaseConnection();
        if (connection.getDatabaseMetaData() == null) {
            throw new IllegalStateException("Not connected to hiveserver");
        }
        return connection.getDatabaseMetaData();
    }

    public void checkConnection() throws SQLException {
        if (getDatabaseConnection() == null) {
            throw new IllegalStateException("Not connected to hiveserver");
        }
    }

    public boolean hasUDF(String query) throws ParseException {
        ParseDriver parseDriver = new ParseDriver();
        try {
            ASTNode node = parseDriver.parse(query);
            return hasUDF(node);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean hasUDF(ASTNode node) {
        if (!node.isNil()) {
            String text = node.getText();
            if ("R".equals(text) || "RA".equals(text)) {
                ASTNode parent = (ASTNode) node.getParent();
                if ("TOK_FUNCTION".equals(parent.getText())) {
                    return true;
                }
            }
        }

        ArrayList<Node> children = node.getChildren();
        if (children != null) {
            for (int i = 0; i < children.size(); i++) {
                ASTNode child = (ASTNode) children.get(i);
                if (hasUDF(child)) {
                    return true;
                }
            }
        }

        return false;
    }

	
	/*
    QueryResult getColumns(String table) throws SQLException {
		DatabaseMetaData databaseMetaData = getDatabaseConnection().getDatabaseMetaData();
		ResultSet rs = databaseMetaData.getColumns(databaseMetaData.getConnection().getCatalog(), null, table, "%");
		return new QueryResult(rs);
	}
	
	QueryResult getTables() throws SQLException {
		DatabaseMetaData databaseMetaData = getDatabaseConnection().getDatabaseMetaData();
		ResultSet rs = databaseMetaData.getTables(databaseMetaData.getConnection().getCatalog(), null, "%", new String[] { "TABLE" });
		return new QueryResult(rs);
	}
	*/

    String dequote(String str) {
        if (str == null) {
            return null;
        }
        while ((str.startsWith("'") && str.endsWith("'"))
                || (str.startsWith("\"") && str.endsWith("\""))) {
            str = str.substring(1, str.length() - 1);
        }
        return str;
    }

    String[] split(String line, String delim) {
        StringTokenizer tok = new StringTokenizer(line, delim);
        String[] ret = new String[tok.countTokens()];
        int index = 0;
        while (tok.hasMoreTokens()) {
            String t = tok.nextToken();
            t = dequote(t);
            ret[index++] = t;
        }
        return ret;
    }

    static String replace(String source, String from, String to) {
        if (source == null) {
            return null;
        }

        if (from.equals(to)) {
            return source;
        }

        StringBuilder replaced = new StringBuilder();

        int index = -1;
        while ((index = source.indexOf(from)) != -1) {
            replaced.append(source.substring(0, index));
            replaced.append(to);
            source = source.substring(index + from.length());
        }
        replaced.append(source);

        return replaced.toString();
    }

    @Override
    public boolean execute(String query) throws SQLException {
        return execute(query, false);
    }

    protected boolean execute(String query, boolean reconnect) throws SQLException {
        Connection connection = getConnection(reconnect);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            return statement.execute(query);
        } catch (SQLException e) {
            if (!reconnect) {
                if (isThriftTransportException(e)) {
                    return reexecute(query);
                }
            }

            throw e;
        }
    }

    protected boolean reexecute(String query) throws SQLException {
        return execute(query, true);
    }

    @Override
    public DataFrameModel query(String query) throws SQLException {
        return query(query, 0, 50);
    }

    @Override
    public DataFrameModel query(String query, int maxRows, int fetchSize) throws SQLException {
        return query(query, maxRows, fetchSize, false);
    }

    protected DataFrameModel query(String query, int maxRows, int fetchSize, boolean reconnect) throws SQLException {
        Connection connection = getConnection(reconnect);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.setMaxRows(maxRows < 0 ? 0 : maxRows);
            statement.setFetchSize(fetchSize <= 0 ? 50 : fetchSize);
            ResultSet rs = statement.executeQuery(query);

            return new ResultSetModel(rs);
        } catch (SQLException e) {
            if (!reconnect) {
                if (isThriftTransportException(e)) {
                    return requery(query, maxRows, fetchSize);
                }
            }

            throw e;
        }
    }

    protected DataFrameModel requery(String query, int maxRows, int fetchSize) throws SQLException {
        return query(query, maxRows, fetchSize, true);
    }

    int getVersion() {
        return forServer2 ? 2 : 1;
    }

    protected boolean isThriftTransportException(Exception e) {
        String msg = e.getMessage();
        return msg.indexOf("TTransportException") != -1;
    }


    private class HiveJdbcConnector extends Thread {

        private String host;
        private int port;
        private String db;
        private Properties properties;

        public HiveJdbcConnector(String host, int port, String db, Properties properties) {
            this.host = host;
            this.port = port;
            this.db = db;
            this.properties = properties;
        }

        @Override
        public void run() {
            connect(host, port, db, properties);
        }

        public void connect(String host, int port, String db, Properties properties) {
            try {
                String url = getUrl(host, port, db);
                String driver = getDriver();
                DatabaseConnection connection = new DatabaseConnection(driver, url, properties);
                connection.connect();

                databaseConnection = connection;
            } catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }

        private String getUrl(String host, int port, String db) {
            String scheme = getUrlPrefix();
            StringBuilder sb = new StringBuilder(scheme);
            sb.append(host);
            sb.append(":");
            sb.append(port);
            sb.append("/");
            sb.append(db);

            return sb.toString();
        }


        private String getUrlPrefix() {
            if (forServer2) {
                return "jdbc:hive2://";
            } else {
                return "jdbc:hive://";
            }
        }

        private String getDriver() {
            if (forServer2) {
                return "org.apache.hive.jdbc.HiveDriver";
            } else {
                return "org.apache.hadoop.hive.jdbc.HiveDriver";
            }
        }
    }

    public void set(String key, String value) throws SQLException {
        String command = String.format("SET %s=%s", key, value);
        execute(command);
    }
}