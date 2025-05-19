// (C) 2025 uchicom
package com.uchicom.csvdb.sql;

public class Header {

  private final String[] header;
  private String[] splitedColumn;
  private String columns;
  int[] columnIndexes;

  public Header(String[] header) {
    this.header = header;
  }

  public int length() {
    return header.length;
  }

  public void setColumn(String columns) {
    this.columns = columns;
    splitedColumn = columns.split(",");
    columnIndexes = new int[splitedColumn.length];
    for (int i = 0; i < splitedColumn.length; i++) {
      columnIndexes[i] = getColumnIndex(splitedColumn[i]);
    }
  }

  public int[] getSelectColumnIndexes() {
    return columnIndexes;
  }

  public int getColumnIndex(String column) {
    for (int i = 0; i < header.length; i++) {
      if (header[i] == null) {
        continue;
      }
      if (header[i].equalsIgnoreCase(column)) {
        return i;
      }
    }
    return -1;
  }

  public String getColumnHeaderString() {
    return columns;
  }
}
