// (C) 2025 uchicom
package com.uchicom.csvdb.sql;

public class Query {

  public Query(Header header, Where where) {
    this.header = header;
    this.where = where;
  }

  private final Header header;
  private final Where where;

  public boolean match(String[] record) {
    if (where == null) {
      return true;
    }
    return where.match(record);
  }

  public Header getHeader() {
    return header;
  }
}
