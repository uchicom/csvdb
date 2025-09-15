// (C) 2025 uchicom
package com.uchicom.csvdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;

import com.uchicom.csvdb.service.CsvService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;

public class MainTest extends AbstractTest {

  @Mock CsvService csvService;

  @Spy @InjectMocks Main main;

  @Captor ArgumentCaptor<String[]> tokensCaptor;

  @Captor ArgumentCaptor<String> tableNameCaptor;

  @Test
  public void execute_select() throws Exception {
    // mock
    doNothing().when(main).select(tokensCaptor.capture());

    var sql = "select * from test.csv";

    // test
    main.execute(sql);

    // assert
    assertArrayEquals(new String[] {"select", "*", "from", "test.csv"}, tokensCaptor.getValue());
  }

  @Test
  public void execute_update() throws Exception {
    // mock
    doNothing().when(main).update(tokensCaptor.capture());

    var sql = "update test.csv set name='namae'";

    // test
    main.execute(sql);

    // assert
    assertArrayEquals(
        new String[] {"update", "test.csv", "set", "name='namae'"}, tokensCaptor.getValue());
  }

  @Test
  public void execute_error() throws Exception {
    // mock
    doNothing().when(main).update(tokensCaptor.capture());

    var sql = "truncate table test.csv";

    // test
    var e = assertThrows(IllegalArgumentException.class, () -> main.execute(sql));

    // assert
    assertEquals("Invalid SQL", e.getMessage());
  }

  @Test
  public void select_error() throws Exception {
    // mock
    doNothing().when(csvService).read(tableNameCaptor.capture(), tokensCaptor.capture());

    var tokens = new String[] {"select", "*", "from"};

    // test
    var e = assertThrows(IllegalArgumentException.class, () -> main.select(tokens));

    // assert
    assertEquals("Invalid SQL", e.getMessage());
    assertEquals(0, tableNameCaptor.getAllValues().size());
    assertEquals(0, tokensCaptor.getAllValues().size());
  }

  @Test
  public void select() throws Exception {
    // mock
    doNothing().when(csvService).read(tableNameCaptor.capture(), tokensCaptor.capture());

    var tokens = new String[] {"select", "*", "from", "test.csv"};

    // test
    main.select(tokens);

    // assert
    assertEquals("test.csv", tableNameCaptor.getValue());
    assertEquals(tokens, tokensCaptor.getValue());
  }

  @Test
  public void update_error() throws Exception {
    // mock
    doNothing().when(csvService).readUpdate(tableNameCaptor.capture(), tokensCaptor.capture());

    var tokens = new String[] {"update", "test.csv", "set"};

    // test
    var e = assertThrows(IllegalArgumentException.class, () -> main.update(tokens));

    // assert
    assertEquals("Invalid SQL", e.getMessage());
    assertEquals(0, tableNameCaptor.getAllValues().size());
    assertEquals(0, tokensCaptor.getAllValues().size());
  }

  @Test
  public void update() throws Exception {
    // mock
    doNothing().when(csvService).readUpdate(tableNameCaptor.capture(), tokensCaptor.capture());

    var tokens = new String[] {"update", "test.csv", "set", "name='namae'"};

    // test
    main.update(tokens);

    // assert
    assertEquals("test.csv", tableNameCaptor.getValue());
    assertEquals(tokens, tokensCaptor.getValue());
  }

  @Test
  public void delete_error() throws Exception {
    // mock
    doNothing().when(csvService).readDelete(tableNameCaptor.capture(), tokensCaptor.capture());

    var tokens = new String[] {"delete", "from"};

    // test
    var e = assertThrows(IllegalArgumentException.class, () -> main.delete(tokens));

    // assert
    assertEquals("Invalid SQL", e.getMessage());
    assertEquals(0, tableNameCaptor.getAllValues().size());
    assertEquals(0, tokensCaptor.getAllValues().size());
  }

  @Test
  public void delete() throws Exception {
    // mock
    doNothing().when(csvService).readDelete(tableNameCaptor.capture(), tokensCaptor.capture());

    var tokens = new String[] {"delete", "from", "test.csv"};

    // test
    main.delete(tokens);

    // assert
    assertEquals("test.csv", tableNameCaptor.getValue());
    assertEquals(tokens, tokensCaptor.getValue());
  }
}
