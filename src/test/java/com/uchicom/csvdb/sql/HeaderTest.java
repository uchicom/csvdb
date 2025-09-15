// (C) 2025 uchicom
package com.uchicom.csvdb.sql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.uchicom.csvdb.AbstractTest;
import org.junit.jupiter.api.Test;

public class HeaderTest extends AbstractTest {

  @Test
  public void length() {

    // mock
    var headers = new String[] {"a", "b"};
    var header = new Header(headers);

    // test
    var result = header.length();

    // assert
    assertEquals(result, headers.length);
  }

  @Test
  public void setColumn_WildCard() {

    // mock
    var headers = new String[] {"a", "b"};
    var header = spy(new Header(headers));
    var columns = "*";

    // test
    header.setColumn(columns);

    // assert
    verify(header, times(1)).setWildCardColumns();
    verify(header, times(0)).setSplitColumns();
  }

  @Test
  public void setColumn() {

    // mock
    var headers = new String[] {"a", "b"};
    var header = spy(new Header(headers));
    var columns = "a,b";

    // test
    header.setColumn(columns);

    // assert
    verify(header, times(0)).setWildCardColumns();
    verify(header, times(1)).setSplitColumns();
  }

  @Test
  public void setWildCardColumns() {

    // mock
    var headers = new String[] {"a", "b"};
    var header = spy(new Header(headers));

    // test
    header.setWildCardColumns();

    // assert
    assertArrayEquals(header.columnIndexes, new int[] {0, 1});
  }

  @Test
  public void setSplitColumns() {

    // mock
    var headers = new String[] {"a", "b"};
    var header = spy(new Header(headers));
    header.columns = "b,a";

    // test
    header.setSplitColumns();

    // assert
    assertArrayEquals(header.columnIndexes, new int[] {1, 0});
  }

  @Test
  public void getSelectColumnIndexes() {

    // mock
    var headers = new String[] {"a", "b"};
    var header = new Header(headers);
    header.columnIndexes = new int[] {0, 1};

    // test
    var result = header.getSelectColumnIndexes();

    // assert
    assertEquals(result, header.columnIndexes);
  }

  @Test
  public void getColumnIndex() {

    // mock
    var headers = new String[] {null, "b", "c"};
    var header = new Header(headers);

    // test and assert
    assertEquals(header.getColumnIndex("B"), 1);
    assertEquals(header.getColumnIndex("d"), -1);
  }

  @Test
  public void getColumnHeaderString_wildCard() {

    // mock
    var headers = new String[] {"a", "b", "c"};
    var header = new Header(headers);
    header.columns = "*";

    // test
    var result = header.getColumnHeaderString();

    // assert
    assertEquals(result, "a,b,c");
  }

  @Test
  public void getColumnHeaderString() {

    // mock
    var headers = new String[] {null, "b", "c"};
    var header = new Header(headers);
    header.columns = "b,c";

    // test
    var result = header.getColumnHeaderString();

    // assert
    assertEquals(result, header.columns);
  }
}
