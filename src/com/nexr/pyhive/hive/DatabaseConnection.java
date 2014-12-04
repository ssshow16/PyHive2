package com.nexr.pyhive.hive;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

class DatabaseConnection {
    private String driver;
    private String url;
    private Properties properties;

    private Connection connection;
    private DatabaseMetaData metaData;
    private Schema schema = null;

    DatabaseConnection(String driver, String url, String user, String password) {
        this.driver = driver;
        this.url = url;

        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);

        this.properties = properties;
    }

    DatabaseConnection(String driver, String url, Properties properties) {
        this.driver = driver;
        this.url = url;
        this.properties = (properties == null ? new Properties() : properties);
    }

    @Override
    public String toString() {
        return getUrl();
    }

    boolean connect() throws SQLException {
        try {
            if (driver != null && driver.length() != 0) {
                Class.forName(driver);
            }
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }

        boolean foundDriver = false;
        try {
            foundDriver = DriverManager.getDriver(getUrl()) != null;
        } catch (Exception e) {
        }

        close();

        setConnection(DriverManager.getConnection(getUrl(), properties));
        setDatabaseMetaData(getConnection(false).getMetaData());

        return true;
    }

    Connection getConnection(boolean reconnect) throws SQLException {
        if (reconnect) {
            reconnect();

            return connection;
        } else {
            if (connection != null) {
                return connection;
            }

            connect();
            return connection;
        }
    }

    void reconnect() throws SQLException {
        close();
        connect();
    }

    void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {

        } finally {
            setConnection(null);
            setDatabaseMetaData(null);
        }
    }

    String[] getTableNames() {
        Schema.Table[] t = getSchema().getTables();
        Set<String> names = new TreeSet<String>();
        for (int i = 0; t != null && i < t.length; i++) {
            names.add(t[i].getName());
        }
        return names.toArray(new String[names.size()]);
    }

    Schema getSchema() {
        if (schema == null) {
            schema = new Schema();
        }
        return schema;
    }

    void setConnection(Connection connection) {
        this.connection = connection;
    }

    DatabaseMetaData getDatabaseMetaData() {
        return metaData;
    }

    void setDatabaseMetaData(DatabaseMetaData metaData) {
        this.metaData = metaData;
    }

    String getUrl() {
        return url;
    }

    class Schema {
        private Table[] tables = null;

        Table[] getTables() {
            if (tables != null) {
                return tables;
            }

            List<Table> tnames = new LinkedList<Table>();

            try {
                ResultSet rs = getDatabaseMetaData().getTables(
                        getConnection(false).getCatalog(), null, "%",
                        new String[]{"TABLE"});
                try {
                    while (rs.next()) {
                        tnames.add(new Table(rs.getString("TABLE_NAME")));
                    }
                } finally {
                    try {
                        rs.close();
                    } catch (Exception e) {
                    }
                }
            } catch (Throwable t) {
            }
            return tables = tnames.toArray(new Table[0]);
        }

        Table getTable(String name) {
            Table[] t = getTables();
            for (int i = 0; t != null && i < t.length; i++) {
                if (name.equalsIgnoreCase(t[i].getName())) {
                    return t[i];
                }
            }
            return null;
        }

        class Table {
            final String name;
            Column[] columns;

            public Table(String name) {
                this.name = name;
            }

            public String getName() {
                return name;
            }

            class Column {
                final String name;

                public Column(String name) {
                    this.name = name;
                }
            }
        }
    }
}