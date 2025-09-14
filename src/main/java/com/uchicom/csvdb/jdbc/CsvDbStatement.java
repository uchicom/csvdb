// (C) 2025 uchicom
package com.uchicom.csvdb.jdbc;

import com.uchicom.csvdb.service.CsvService;
import com.uchicom.csve.util.CSVReader;
import java.io.FileWriter;
import java.io.RandomAccessFile;
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

  public CsvDbStatement(CsvDbConnection connection) {
    this.connection = connection;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

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

  @Override
  public int executeUpdate(String sql) throws SQLException {
    if (sql.startsWith("insert")) {
      var tokens = sql.split(" *\\( *| *\\) *|(?<!,) (?!,)", 0);
      if (tokens.length < 5) {
        throw new SQLSyntaxErrorException("Invalid SQL Syntax.");
      }
      var filePath = connection.databaseName + "/" + tokens[2];
      try (var csvReader = new CSVReader(filePath, "UTF-8");
          var writer = new FileWriter(filePath, true)) {
        new CsvService().insert(csvReader, writer, tokens);
        return 1;
      } catch (Exception e) {
        throw new SQLException(e);
      }
    } else if (sql.startsWith("delete")) {
      var tokens = sql.split(" *\\( *| *\\) *|(?<!,) (?!,)", 0);
      if (tokens.length != 3 && tokens.length != 7) {
        throw new SQLSyntaxErrorException("Invalid SQL Syntax.");
      }
      var filePath = connection.databaseName + "/" + tokens[2];
      try (var csvReader = new CSVReader(filePath, "UTF-8");
          var writer = new RandomAccessFile(filePath, "rw")) {
        return new CsvService().delete(csvReader, writer, tokens);
      } catch (Exception e) {
        throw new SQLException(e);
      }
    } else if (sql.startsWith("update")) {
      var tokens = sql.split(" *\\( *| *\\) *|(?<!,) (?!,)", 0);
      if (tokens.length != 3 && tokens.length != 8) {
        throw new SQLSyntaxErrorException("Invalid SQL Syntax.");
      }
      var filePath = connection.databaseName + "/" + tokens[1];
      try (var csvReader = new CSVReader(filePath, "UTF-8");
          var writer = new RandomAccessFile(filePath, "rw")) {
        return new CsvService().update(csvReader, writer, tokens);
      } catch (Exception e) {
        throw new SQLException(e);
      }
    } else {
      throw new SQLSyntaxErrorException("SQL文が不正です。");
    }
  }

  @Override
  public void close() throws SQLException {}

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {}

  @Override
  public int getMaxRows() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {}

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {}

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {}

  @Override
  public void cancel() throws SQLException {}

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public void setCursorName(String name) throws SQLException {}

  @Override
  public boolean execute(String sql) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return null;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {}

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {}

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {}

  @Override
  public void clearBatch() throws SQLException {}

  @Override
  public int[] executeBatch() throws SQLException {
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    return 0;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {}

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {}

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }
}
