// (C) 2025 uchicom
package com.uchicom.csvdb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.uchicom.csvdb.AbstractTest;
import com.uchicom.csvdb.service.CsvService;
import java.sql.Date;
import java.sql.DriverManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link CsvService}のテストケース.
 *
 * @author uchicom
 */
@Tag("jdbc")
public class CsvDbConnectionTest extends AbstractTest {

  @BeforeEach
  public void setUp() throws Exception {
    Class.forName("com.uchicom.csvdb.jdbc.CsvDbDriver");
  }

  @Test
  public void select() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:csvdb:src/test/resources");
        var statement = connection.createStatement();
        var resultSet =
            statement.executeQuery("select name,height,birthday from test.csv where id = 1")) {
      assertTrue(resultSet.next());
      assertEquals("Alice", resultSet.getString(1));
      assertEquals(170, resultSet.getInt(2));
      assertEquals(Date.valueOf("2000-01-01"), resultSet.getDate(3));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void selectAll() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:csvdb:src/test/resources");
        var statement = connection.createStatement();
        var resultSet = statement.executeQuery("select * from test.csv where id = 1")) {
      assertTrue(resultSet.next());
      assertEquals(1L, resultSet.getLong(1));
      assertEquals("Alice", resultSet.getString(2));
      assertEquals(170, resultSet.getInt(3));
      assertEquals(Date.valueOf("2000-01-01"), resultSet.getDate(4));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void insert() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:csvdb:src/test/resources");
        var statement = connection.createStatement()) {
      statement.execute(
          "insert into test.csv ( id , name,birthday) values (4,'Dan', '2015-01-01')");
    }
  }

  @Test
  public void insert2() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:csvdb:src/test/resources");
        var statement = connection.createStatement()) {
      statement.execute("insert into test.csv values (5,'Dan', 190, '2015-01-01')");
    }
  }

  @Test
  public void delete() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:csvdb:src/test/resources");
        var statement = connection.createStatement()) {
      statement.execute("delete from test.csv where id = 2");
    }
  }

  @Test
  public void update() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:csvdb:src/test/resources");
        var statement = connection.createStatement()) {
      statement.execute(
          "update test.csv set name='Christina',height=170,birthday='2010-12-30' where id = 3");
    }
  }
}
