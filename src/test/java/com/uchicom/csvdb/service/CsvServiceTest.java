// (C) 2025 uchicom
package com.uchicom.csvdb.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;

import com.uchicom.csvdb.AbstractTest;
import com.uchicom.csvdb.sql.Header;
import com.uchicom.csvdb.sql.Query;
import com.uchicom.csvdb.sql.Where;
import com.uchicom.csve.util.CSVReader;
import java.io.File;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Spy;

/**
 * {@link CsvService}のテストケース.
 *
 * @author uchicom
 */
@Tag("service")
public class CsvServiceTest extends AbstractTest {

  @Captor ArgumentCaptor<String> filePathCaptor;

  @Captor ArgumentCaptor<String> columnCaptor;

  @Captor ArgumentCaptor<CSVReader> csvReaderCaptor;

  @Captor ArgumentCaptor<Header> headerCaptor;

  @Captor ArgumentCaptor<String[]> tokensCaptor;

  @Captor ArgumentCaptor<Query> queryCaptor;

  @Captor ArgumentCaptor<Integer> columnSizeCaptor;

  @Captor ArgumentCaptor<Boolean> isForceSizeFixCaptor;

  @Captor ArgumentCaptor<String[]> recordCaptor;

  @Captor ArgumentCaptor<String> printCaptor;

  @Captor ArgumentCaptor<String> printlnCaptor;

  @Captor ArgumentCaptor<String[]> updateValuesCaptor;

  @Captor ArgumentCaptor<String> columnsCaptor;

  @Captor ArgumentCaptor<String> updateValueCaptor;

  @Captor ArgumentCaptor<String> orgValueCaptor;

  @Captor ArgumentCaptor<Integer> charCaptor;

  @Captor ArgumentCaptor<Writer> writerCaptor;

  @Spy @InjectMocks CsvService service;

  @Test
  public void read() throws Exception {
    // mock
    var reader = mock(CSVReader.class);
    doReturn(reader).when(service).createCsvReader(filePathCaptor.capture());
    var query = mock(Query.class);
    doReturn(query).when(service).createQuery(csvReaderCaptor.capture(), tokensCaptor.capture());
    doNothing().when(service).readBody(csvReaderCaptor.capture(), queryCaptor.capture());

    var csvFile = "test.csv";
    var tokens = new String[] {"select", "a,b", "where", "c", "=", "'2'"};

    // test
    service.read(csvFile, tokens);

    // assert
    assertThat(filePathCaptor.getValue()).isEqualTo(csvFile);
    var csvReaders = csvReaderCaptor.getAllValues();
    assertThat(csvReaders).hasSize(2);
    assertThat(csvReaders.get(0)).isEqualTo(reader);
    assertThat(csvReaders.get(1)).isEqualTo(reader);
    assertThat(tokensCaptor.getValue()).isEqualTo(tokens);
    assertThat(queryCaptor.getValue()).isEqualTo(query);
  }

  @Test
  public void readUpdate() throws Exception {
    // mock
    var reader = mock(CSVReader.class);
    doReturn(reader).when(service).createCsvReader(filePathCaptor.capture());
    var query = mock(Query.class);
    doReturn(query)
        .when(service)
        .createUpdateQuery(csvReaderCaptor.capture(), tokensCaptor.capture());
    var header = mock(Header.class);
    doReturn(header).when(query).getHeader();
    doNothing().when(header).setColumn(columnsCaptor.capture());
    var updateValues = new String[] {"name='namae'"};
    doReturn(updateValues)
        .when(service)
        .createUpdateValues(queryCaptor.capture(), tokensCaptor.capture());
    doNothing()
        .when(service)
        .readUpdateBody(
            csvReaderCaptor.capture(), queryCaptor.capture(), updateValuesCaptor.capture());

    var csvFile = "test.csv";
    var tokens = new String[] {"select", "a,b", "where", "c", "=", "'2'"};

    // test
    service.readUpdate(csvFile, tokens);

    // assert
    assertThat(filePathCaptor.getValue()).isEqualTo(csvFile);
    var csvReaders = csvReaderCaptor.getAllValues();
    assertThat(csvReaders).hasSize(2);
    assertThat(csvReaders.get(0)).isEqualTo(reader);
    assertThat(csvReaders.get(1)).isEqualTo(reader);
    var tokenses = tokensCaptor.getAllValues();
    assertThat(tokenses).hasSize(2);
    assertThat(tokenses.get(0)).isEqualTo(tokens);
    assertThat(tokenses.get(1)).isEqualTo(tokens);
    var querys = queryCaptor.getAllValues();
    assertThat(querys).hasSize(2);
    assertThat(querys.get(0)).isEqualTo(query);
    assertThat(querys.get(1)).isEqualTo(query);
    assertThat(columnsCaptor.getValue()).isEqualTo("*");
    assertThat(updateValuesCaptor.getValue()).isEqualTo(updateValues);
  }

  @Test
  public void readDelete() throws Exception {
    // mock
    var reader = mock(CSVReader.class);
    doReturn(reader).when(service).createCsvReader(filePathCaptor.capture());
    var query = mock(Query.class);
    doReturn(query)
        .when(service)
        .createDeleteQuery(csvReaderCaptor.capture(), tokensCaptor.capture());
    var header = mock(Header.class);
    doReturn(header).when(query).getHeader();
    doNothing().when(header).setColumn(columnsCaptor.capture());
    doNothing().when(service).readDeleteBody(csvReaderCaptor.capture(), queryCaptor.capture());

    var csvFile = "test.csv";
    var tokens = new String[] {"delete", "from", "table", "where", "c", "=", "'2'"};

    // test
    service.readDelete(csvFile, tokens);

    // assert
    assertThat(filePathCaptor.getValue()).isEqualTo(csvFile);
    var csvReaders = csvReaderCaptor.getAllValues();
    assertThat(csvReaders).hasSize(2);
    assertThat(csvReaders.get(0)).isEqualTo(reader);
    assertThat(csvReaders.get(1)).isEqualTo(reader);
    var tokenses = tokensCaptor.getAllValues();
    assertThat(tokenses).hasSize(1);
    assertThat(tokenses.get(0)).isEqualTo(tokens);
    var querys = queryCaptor.getAllValues();
    assertThat(querys).hasSize(1);
    assertThat(querys.get(0)).isEqualTo(query);
    assertThat(columnsCaptor.getValue()).isEqualTo("*");
  }

  @Test
  public void readWhere_null() throws Exception {
    // mock
    var header = mock(Header.class);
    ;
    var tokens = new String[] {"select", "a,b", "from", "test.csv"};

    // test
    var result = service.readWhere(header, tokens);

    // assert
    assertThat(result).isNull();
  }

  @Test
  public void readWhere() throws Exception {
    // mock
    var header = mock(Header.class);
    ;
    var tokens = new String[] {"select", "a,b", "from", "test.csv", "where", "c", "=", "'2'"};

    // test
    try (var mocked =
        mockConstruction(
            Where.class,
            (mock, context) -> {
              assertThat(context.arguments().get(0)).isEqualTo(header);
              assertThat(context.arguments().get(1)).isEqualTo(new String[] {"c", "=", "'2'"});
            })) {
      var result = service.readWhere(header, tokens);

      // assert
      assertThat(result).isEqualTo(mocked.constructed().get(0));
    }
  }

  @Test
  public void readHeader() throws Exception {
    // mock
    var splitedCsvRecord = new String[] {"a", "b", "c"};
    doReturn(splitedCsvRecord).when(service).getSplitedCsvRecord(csvReaderCaptor.capture());
    var reader = mock(CSVReader.class);

    // test
    try (var mocked =
        mockConstruction(
            Header.class,
            (mock, context) -> {
              assertThat(context.arguments().get(0)).isEqualTo(new String[] {"a", "b", "c"});
            })) {
      var result = service.readHeader(reader);

      // assert
      assertThat(result).isEqualTo(mocked.constructed().get(0));
      assertThat(csvReaderCaptor.getValue()).isEqualTo(reader);
    }
  }

  @Test
  public void readBody() throws Exception {
    // mock
    var header = mock(Header.class);
    var query = mock(Query.class);
    doReturn(header).when(query).getHeader();
    var writer = mock(Writer.class);
    doReturn(writer).when(service).createWriter();
    doNothing().when(writer).write(printCaptor.capture());
    var columnHeaderString = "a,b";
    doReturn(columnHeaderString).when(header).getColumnHeaderString();
    var length = 3;
    doReturn(length).when(header).length();
    var selectColumnIndexes = new int[] {0, 1};
    doReturn(selectColumnIndexes).when(header).getSelectColumnIndexes();
    var splitedCsvRecord1 = new String[] {"1", "2", "3"};
    var splitedCsvRecord2 = new String[] {"4", "5", "6"};
    doReturn(splitedCsvRecord1)
        .doReturn(splitedCsvRecord2)
        .doReturn(null)
        .when(service)
        .getSplitedCsvRecord(csvReaderCaptor.capture(), columnSizeCaptor.capture());

    doReturn(true).doReturn(false).when(query).match(recordCaptor.capture());

    var reader = mock(CSVReader.class);

    // test
    service.readBody(reader, query);

    // assert
    var csvReaders = csvReaderCaptor.getAllValues();
    assertThat(csvReaders).hasSize(3);
    assertThat(csvReaders.get(0)).isEqualTo(reader);
    assertThat(csvReaders.get(1)).isEqualTo(reader);
    assertThat(csvReaders.get(2)).isEqualTo(reader);
    assertThat(columnSizeCaptor.getValue()).isEqualTo(length);
    var prints = printCaptor.getAllValues();
    assertThat(prints).hasSize(3);
    assertThat(prints.get(0)).isEqualTo("a,b");
    assertThat(prints.get(1)).isEqualTo("1");
    assertThat(prints.get(2)).isEqualTo("2");
  }

  @Test
  public void readUpdateBody() throws Exception {
    // mock
    var header = mock(Header.class);
    var query = mock(Query.class);
    doReturn(header).when(query).getHeader();
    var writer = mock(Writer.class);
    doReturn(writer).when(service).createWriter();
    doNothing().when(writer).write(printCaptor.capture());
    doNothing().when(writer).write(charCaptor.capture());
    var columnHeaderString = "a,b";
    doReturn(columnHeaderString).when(header).getColumnHeaderString();
    var length = 3;
    doReturn(length).when(header).length();
    var splitedCsvRecord1 = new String[] {"1", "2", "3"};
    var splitedCsvRecord2 = new String[] {"4", "5", "6"};
    doReturn(splitedCsvRecord1)
        .doReturn(splitedCsvRecord2)
        .doReturn(null)
        .when(service)
        .getSplitedCsvRecord(csvReaderCaptor.capture(), columnSizeCaptor.capture());
    doNothing().when(service).writeRecord(writerCaptor.capture(), recordCaptor.capture());
    doNothing()
        .when(service)
        .writeUpdateRecord(
            writerCaptor.capture(), recordCaptor.capture(), updateValuesCaptor.capture());

    doReturn(true).doReturn(false).when(query).match(recordCaptor.capture());

    var reader = mock(CSVReader.class);
    var udpateValues = new String[] {null, "c"};

    // test
    service.readUpdateBody(reader, query, udpateValues);

    // assert
    var csvReaders = csvReaderCaptor.getAllValues();
    assertThat(csvReaders).hasSize(3);
    assertThat(csvReaders.get(0)).isEqualTo(reader);
    assertThat(csvReaders.get(1)).isEqualTo(reader);
    assertThat(csvReaders.get(2)).isEqualTo(reader);
    assertThat(columnSizeCaptor.getValue()).isEqualTo(length);
    var prints = printCaptor.getAllValues();
    assertThat(prints).hasSize(1);
    assertThat(prints.get(0)).isEqualTo("a,b");
    var chars = charCaptor.getAllValues();
    assertThat(chars).hasSize(1);
    assertThat(chars.get(0)).isEqualTo('\n');
    assertThat(recordCaptor.getAllValues())
        .containsExactly(
            splitedCsvRecord1, splitedCsvRecord1, splitedCsvRecord2, splitedCsvRecord2);
    assertThat(writerCaptor.getAllValues()).containsExactly(writer, writer);
    assertThat(updateValuesCaptor.getAllValues()).containsExactly(udpateValues);
  }

  @Test
  public void readDeleteBody() throws Exception {
    // mock
    var header = mock(Header.class);
    var query = mock(Query.class);
    doReturn(header).when(query).getHeader();
    var writer = mock(Writer.class);
    doReturn(writer).when(service).createWriter();
    doNothing().when(writer).write(printCaptor.capture());
    doNothing().when(writer).write(charCaptor.capture());
    var columnHeaderString = "a,b";
    doReturn(columnHeaderString).when(header).getColumnHeaderString();
    var length = 3;
    doReturn(length).when(header).length();
    var splitedCsvRecord1 = new String[] {"1", "2", "3"};
    var splitedCsvRecord2 = new String[] {"4", "5", "6"};
    doReturn(splitedCsvRecord1)
        .doReturn(splitedCsvRecord2)
        .doReturn(null)
        .when(service)
        .getSplitedCsvRecord(csvReaderCaptor.capture(), columnSizeCaptor.capture());
    doNothing().when(service).writeRecord(writerCaptor.capture(), recordCaptor.capture());
    doReturn(true).doReturn(false).when(query).match(recordCaptor.capture());

    var reader = mock(CSVReader.class);

    // test
    service.readDeleteBody(reader, query);

    // assert
    var csvReaders = csvReaderCaptor.getAllValues();
    assertThat(csvReaders).hasSize(3);
    assertThat(csvReaders.get(0)).isEqualTo(reader);
    assertThat(csvReaders.get(1)).isEqualTo(reader);
    assertThat(csvReaders.get(2)).isEqualTo(reader);
    assertThat(columnSizeCaptor.getValue()).isEqualTo(length);
    var prints = printCaptor.getAllValues();
    assertThat(prints).hasSize(1);
    assertThat(prints.get(0)).isEqualTo("a,b");
    var chars = charCaptor.getAllValues();
    assertThat(chars).hasSize(1);
    assertThat(chars.get(0)).isEqualTo('\n');
    assertThat(recordCaptor.getAllValues())
        .containsExactly(splitedCsvRecord1, splitedCsvRecord2, splitedCsvRecord2);
    assertThat(writerCaptor.getAllValues()).containsExactly(writer);
  }

  @Test
  public void writeRecord() throws Exception {
    // mock
    var writer = mock(Writer.class);
    doReturn(writer).when(service).createWriter();
    doNothing().when(writer).write(printCaptor.capture());
    doNothing().when(writer).write(charCaptor.capture());

    var splitedCsvRecord = new String[] {"a", "b"};

    // test
    service.writeRecord(writer, splitedCsvRecord);

    // assert
    var prints = printCaptor.getAllValues();
    assertThat(prints).hasSize(2);
    assertThat(prints.get(0)).isEqualTo("a");
    assertThat(prints.get(1)).isEqualTo("b");
    var chars = charCaptor.getAllValues();
    assertThat(chars).hasSize(2);
    assertThat(chars.get(0)).isEqualTo(',');
    assertThat(chars.get(1)).isEqualTo('\n');
  }

  @Test
  public void writeUpdateRecord() throws Exception {
    // mock
    var writer = mock(Writer.class);
    doReturn(writer).when(service).createWriter();
    doNothing().when(writer).write(printCaptor.capture());
    doNothing().when(writer).write(charCaptor.capture());
    var updateValue = "updateValue";
    var orgValue = "orgValue";
    doReturn(updateValue)
        .doReturn(orgValue)
        .when(service)
        .getValue(updateValueCaptor.capture(), orgValueCaptor.capture());

    var splitedCsvRecord = new String[] {"a", "b"};
    var udpateValues = new String[] {null, "c"};

    // test
    service.writeUpdateRecord(writer, splitedCsvRecord, udpateValues);

    // assert
    var prints = printCaptor.getAllValues();
    assertThat(prints).hasSize(2);
    assertThat(prints.get(0)).isEqualTo(updateValue);
    assertThat(prints.get(1)).isEqualTo(orgValue);
    var chars = charCaptor.getAllValues();
    assertThat(chars).hasSize(2);
    assertThat(chars.get(0)).isEqualTo(',');
    assertThat(chars.get(1)).isEqualTo('\n');
    var updateValues = updateValueCaptor.getAllValues();
    assertThat(updateValues).hasSize(2);
    assertThat(updateValues.get(0)).isNull();
    assertThat(updateValues.get(1)).isEqualTo("c");
    var orgValues = orgValueCaptor.getAllValues();
    assertThat(orgValues).hasSize(2);
    assertThat(orgValues.get(0)).isEqualTo("a");
    assertThat(orgValues.get(1)).isEqualTo("b");
  }

  @Test
  public void getValue() throws Exception {

    // mock
    var updateValue = "updateValue";
    var orgValue = "orgValue";

    // test and assert
    assertEquals(orgValue, service.getValue(null, orgValue));
    assertEquals(updateValue, service.getValue(updateValue, orgValue));
  }

  @Test
  public void createCsvReader() throws Exception {
    // mock
    var filePath = "src/test/resources/customer.csv";

    // test
    try (var mocked =
        mockConstruction(
            CSVReader.class,
            (mock, context) -> {
              assertThat(context.arguments().get(0)).isEqualTo(new File(filePath));
              assertThat(context.arguments().get(1)).isEqualTo(StandardCharsets.UTF_8);
            })) {
      var result = service.createCsvReader(filePath);

      // assert
      assertThat(result).isEqualTo(mocked.constructed().get(0));
    }
  }

  @Test
  public void getSplitedCsvRecord() throws Exception {
    // mock
    var reader = mock(CSVReader.class);
    var line1 = new String[0];
    doReturn(line1)
        .when(reader)
        .getNextCsvLine(columnSizeCaptor.capture(), isForceSizeFixCaptor.capture());

    // test
    var result = service.getSplitedCsvRecord(reader);

    // assert
    assertThat(result).isEqualTo(line1);
    assertThat(columnSizeCaptor.getValue()).isEqualTo(1);
    assertThat(isForceSizeFixCaptor.getValue()).isTrue();
  }

  @Test
  public void getSplitedCsvRecord_columnSize() throws Exception {
    // mock
    var reader = mock(CSVReader.class);
    var splitedCsvRecord1 = new String[] {"\u007f"};
    var splitedCsvRecord2 = new String[] {"1", "2", "3"};
    doReturn(splitedCsvRecord1)
        .doReturn(splitedCsvRecord2)
        .doReturn(null)
        .when(reader)
        .getNextCsvLine(columnSizeCaptor.capture(), isForceSizeFixCaptor.capture());

    var columnSize = 3;
    // test
    var result = service.getSplitedCsvRecord(reader, columnSize);

    // assert
    assertThat(result).isEqualTo(splitedCsvRecord2);
    assertThat(columnSizeCaptor.getValue()).isEqualTo(columnSize);
    assertThat(isForceSizeFixCaptor.getValue()).isFalse();
  }
}
