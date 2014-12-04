package com.nexr.pyhive.model;

import java.util.List;

/**
 * Created by bruceshin on 11/10/14.
 */
public class MapModel implements DataFrameModel{

    private String[] columns;
    private String[] types;
    private List<List> values;
    private int position = -1;

    public MapModel(String[] columns, String[] types, List<List> values){
        this.columns = columns;
        this.types = types;
        this.values = values;
    }

    public int getColumnCount(){
        return columns.length;
    }

    public String[] getColumnNames(){
        return columns;
    }

    public String[] getColumnTypes(){
        return types;
    }

    public String getColumnName(int i){
        return columns[i];
    }

    public String getColumnType(int i){
        return types[i];
    }

    public boolean next(){
        ++position;
        return position < values.get(0).size();
    }

    public Object getValue(int i){
        return values.get(i).get(position);
    }

    public String getStringValue(int i){
        return values.get(i).get(position).toString();
    }

    public double getDoubleValue(int i){
        return ((Number)values.get(i).get(position)).doubleValue();
    }

    public long getLongValue(int i){
        return ((Number)values.get(i).get(position)).longValue();
    }

    public int getIntValue(int i){
        return ((Number)values.get(i).get(position)).intValue();
    }

    public void close() throws Exception{
        new UnsupportedOperationException();
    }

    public String toString() {

        StringBuffer strBuf = new StringBuffer();

        for(int i = 0 ; i < columns.length ; i++){
            strBuf.append(columns[i]).append("\t");
        }
        if(values.size() < 0) return strBuf.toString();

        strBuf.append("\n");

        for(int rowIdx = 0 ; rowIdx < values.get(0).size() ; rowIdx++){
            for(int colIdx = 0 ; colIdx < values.size() ; colIdx++){
                strBuf.append(values.get(colIdx).get(rowIdx));
                strBuf.append("\t");
            }
            strBuf.append("\n");
        }

        return strBuf.toString();
    }
}
