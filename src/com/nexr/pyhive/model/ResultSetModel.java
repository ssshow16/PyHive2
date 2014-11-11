package com.nexr.pyhive.model;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by bruceshin on 11/10/14.
 */
public class ResultSetModel implements DataFrameModel {

    private ResultSet rs;
    private ResultSetMetaData md;

    public ResultSetModel(ResultSet rs) throws SQLException {
        this.rs = rs;
        this.md = rs.getMetaData();
    }

    public int getColumnCount() throws SQLException {
        return md.getColumnCount();
    }

    public String[] getColumnNames() throws SQLException {
        String[] names = new String[getColumnCount()];
        for (int i = 0; i < getColumnCount(); i++) {
            names[i] = md.getColumnName(i+1);
        }
        return names;
    }

    public String[] getColumnTypes() throws SQLException {
        String[] types = new String[getColumnCount()];
        for (int i = 0; i < getColumnCount(); i++) {
            types[i] = md.getColumnTypeName(i+1);
        }
        return types;
    }


    public String getColumnName(int i) throws SQLException {
        return md.getColumnName(i);
    }

    public String getColumnType(int i) throws SQLException {
        return md.getColumnTypeName(i);
    }

    public boolean next() throws SQLException {
        return rs.next();
    }

    public Object getValue(int i) throws SQLException {
        return rs.getObject(i + 1);
    }

    public String getStringValue(int i) throws SQLException {
        return rs.getString(i + 1);
    }

    public double getDoubleValue(int i) throws SQLException {
        return rs.getDouble(i + 1);
    }

    public long getLongValue(int i) throws SQLException {
        return rs.getLong(i + 1);
    }

    public int getIntValue(int i) throws SQLException {
        return rs.getInt(i + 1);
    }

    public void close() throws SQLException {
        rs.close();
    }

    public String toString() {
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                if (sb.length() > 0) {
                    sb.append("\n");
                }

                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        sb.append(" ");
                    }

                    String value = rs.getString(i);
                    sb.append(value == null ? "" : value);
                }
            }

            return sb.toString();
        } catch (Exception e) {
            return e.toString();
        }
    }
}
