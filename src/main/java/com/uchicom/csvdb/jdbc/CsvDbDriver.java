// (C) 2025 uchicom
package com.uchicom.csvdb.jdbc;

import com.uchicom.csvdb.factory.di.DIFactory;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class CsvDbDriver implements Driver {
  private static final Logger logger = DIFactory.logger();

  public static final String URL_PREFIX = "jdbc:csvdb:";

  static final int MAJOR_VERSION = 0;

  static final int MINOR_VERSION = 1;

  static {
    try {
      DriverManager.registerDriver(new CsvDbDriver());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to register CsvDbDriver", e);
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return new CsvDbConnection(url, info);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url.startsWith(URL_PREFIX)) {
      return true;
    }
    return false;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return null;
  }

  @Override
  public int getMajorVersion() {
    return MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return MINOR_VERSION;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return logger;
  }
}
