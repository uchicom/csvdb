// (C) 2025 uchicom
package com.uchicom.csvdb.jdbc;

import com.uchicom.csvdb.service.CsvService;
import com.uchicom.csve.util.CSVReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class CsvDbStatement implements Statement {

  /** コネクション */
  CsvDbConnection connection;

  CSVReader csvReader;

  /**
   * 引数ありのコンストラクタ.
   *
   * @param connection
   */
  public CsvDbStatement(CsvDbConnection connection) {
    this.connection = connection;
  }

  /* (非 Javadoc)
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  /* (非 Javadoc)
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  /**
   * box名{"name":"John Smith","age":33} //json部分は条件
   *
   * @param sql
   * @return
   * @throws SQLException
   */
  /* (非 Javadoc)
   * @see java.sql.Statement#executeQuery(java.lang.String)
   */
  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (sql.startsWith("select")) {
      var tokens = sql.split(" ", 0);
      if (tokens.length < 4) {
        throw new SQLSyntaxErrorException("Invalid SQL Syntax.");
      }
      try {
        csvReader = new CSVReader(connection.databaseName + "/" + tokens[3], "UTF-8");
        var query = new CsvService().createQuery(csvReader, tokens);
        return new CsvDbResultSet(csvReader, query);
      } catch (Exception e) {
        throw new SQLException(e);
      }

    } else {
      throw new SQLSyntaxErrorException("SQL文が不正です。");
    }
  }

  /**
   * insertとdeleteのみ<br>
   * insert<br>
   * i box名 {"name": "John Smith", "age": 33} //json部分は格納オブジェクト<br>
   * delete<br>
   * d box名 {"name": "John Smith", "age": 33} //json部分は条件<br>
   *
   * @param sql
   * @return
   * @throws SQLException
   */
  /* (非 Javadoc)
   * @see java.sql.Statement#executeUpdate(java.lang.String)
   */
  @Override
  public int executeUpdate(String sql) throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#close()
   */
  @Override
  public void close() throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#getMaxFieldSize()
   */
  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#setMaxFieldSize(int)
   */
  @Override
  public void setMaxFieldSize(int max) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#getMaxRows()
   */
  @Override
  public int getMaxRows() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#setMaxRows(int)
   */
  @Override
  public void setMaxRows(int max) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#setEscapeProcessing(boolean)
   */
  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#getQueryTimeout()
   */
  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#setQueryTimeout(int)
   */
  @Override
  public void setQueryTimeout(int seconds) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#cancel()
   */
  @Override
  public void cancel() throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#getWarnings()
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#clearWarnings()
   */
  @Override
  public void clearWarnings() throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#setCursorName(java.lang.String)
   */
  @Override
  public void setCursorName(String name) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#execute(java.lang.String)
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getResultSet()
   */
  @Override
  public ResultSet getResultSet() throws SQLException {
    return null;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getUpdateCount()
   */
  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getMoreResults()
   */
  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#setFetchDirection(int)
   */
  @Override
  public void setFetchDirection(int direction) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#getFetchDirection()
   */
  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#setFetchSize(int)
   */
  @Override
  public void setFetchSize(int rows) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#getFetchSize()
   */
  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getResultSetConcurrency()
   */
  @Override
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getResultSetType()
   */
  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#addBatch(java.lang.String)
   */
  @Override
  public void addBatch(String sql) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#clearBatch()
   */
  @Override
  public void clearBatch() throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#executeBatch()
   */
  @Override
  public int[] executeBatch() throws SQLException {
    return null;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getConnection()
   */
  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getMoreResults(int)
   */
  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getGeneratedKeys()
   */
  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#executeUpdate(java.lang.String, int)
   */
  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
   */
  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
   */
  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#execute(java.lang.String, int)
   */
  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#execute(java.lang.String, int[])
   */
  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
   */
  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#getResultSetHoldability()
   */
  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#isClosed()
   */
  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#setPoolable(boolean)
   */
  @Override
  public void setPoolable(boolean poolable) throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#isPoolable()
   */
  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Statement#closeOnCompletion()
   */
  @Override
  public void closeOnCompletion() throws SQLException {}

  /* (非 Javadoc)
   * @see java.sql.Statement#isCloseOnCompletion()
   */
  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }
}
