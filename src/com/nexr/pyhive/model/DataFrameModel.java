package com.nexr.pyhive.model;

/**
 * Created by bruceshin on 11/10/14.
 */
public interface DataFrameModel {

    int getColumnCount() throws Exception;

    String[] getColumnNames() throws Exception;

    String[] getColumnTypes() throws Exception;

    String getColumnName(int i) throws Exception;

    String getColumnType(int i) throws Exception;

    boolean next() throws Exception;

    Object getValue(int i) throws Exception;

    String getStringValue(int i) throws Exception;

    double getDoubleValue(int i) throws Exception;

    long getLongValue(int i) throws Exception;

    int getIntValue(int i) throws Exception;

    void close() throws Exception;
}
