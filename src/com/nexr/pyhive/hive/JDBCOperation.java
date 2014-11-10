package com.nexr.pyhive.hive;

import com.nexr.pyhive.model.DataFrameModel;

import java.sql.SQLException;

/**
 * Created by bruceshin on 10/31/14.
 */
public interface JDBCOperation {
    void connect(String host, int port, String db, String user, String password) throws SQLException;
    void close() throws SQLException;
    void checkConnection() throws SQLException;

    boolean execute(String query) throws Exception;

    DataFrameModel query(String query) throws Exception;
    DataFrameModel query(String query, int maxRows, int fetchSize) throws Exception;
}
