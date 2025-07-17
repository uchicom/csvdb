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

  static {
    try {
      DriverManager.registerDriver(new CsvDbDriver());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to register CsvDbDriver", e);
    }
  }

  /* (非 Javadoc)
   * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
   */
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return new CsvDbConnection(url, info);
  }

  /* (非 Javadoc)
   * @see java.sql.Driver#acceptsURL(java.lang.String)
   */
  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url.startsWith(URL_PREFIX)) {
      return true;
    }
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
   */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return null;
  }

  /* (非 Javadoc)
   * @see java.sql.Driver#getMajorVersion()
   */
  @Override
  public int getMajorVersion() {
    return 0;
  }

  /* (非 Javadoc)
   * @see java.sql.Driver#getMinorVersion()
   */
  @Override
  public int getMinorVersion() {
    return 1;
  }

  /* (非 Javadoc)
   * @see java.sql.Driver#jdbcCompliant()
   */
  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  /* (非 Javadoc)
   * @see java.sql.Driver#getParentLogger()
   */
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return logger;
  }
}
