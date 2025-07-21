// (C) 2025 uchicom
package com.uchicom.csvdb.service;

import com.uchicom.csvdb.sql.Header;
import com.uchicom.csvdb.sql.Query;
import com.uchicom.csvdb.sql.Where;
import com.uchicom.csve.util.CSVReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class CsvService {

  public void read(String csvFile, String[] tokens) throws Exception {

    try (var reader = createCsvReader(csvFile)) {
      var query = createQuery(reader, tokens);
      readBody(reader, query);
    }
  }

  public Query createQuery(CSVReader reader, String[] tokens) throws Exception {
    var header = readHeader(reader);
    header.setColumn(tokens[1]);
    var where = readWhere(header, tokens);
    return new Query(header, where);
  }

  Where readWhere(Header header, String[] tokens) throws Exception {
    if (tokens.length < 5) {
      return null;
    }
    return new Where(header, Arrays.copyOfRange(tokens, 5, tokens.length));
  }

  Header readHeader(CSVReader reader) throws Exception {
    return new Header(getSplitedCsvRecord(reader));
  }

  void readBody(CSVReader reader, Query query) throws Exception {
    var header = query.getHeader();
    try (var writer = createWriter()) {
      writer.write(header.getColumnHeaderString());
      writer.write('\n');
      var recordLength = header.length();
      var selectColumnIndexes = header.getSelectColumnIndexes();
      var selectColumnLength = selectColumnIndexes.length;
      String[] splitedCsvRecord = null;
      while ((splitedCsvRecord = getSplitedCsvRecord(reader, recordLength)) != null) {
        if (!query.match(splitedCsvRecord)) {
          continue;
        }
        writer.write(splitedCsvRecord[selectColumnIndexes[0]]);
        for (int i = 1; i < selectColumnLength; i++) {
          var columnIndex = selectColumnIndexes[i];
          writer.write(',');
          writer.write(splitedCsvRecord[columnIndex]);
        }
        writer.write('\n');
      }
    }
  }

  Writer createWriter() {
    return new BufferedWriter(
        new PrintWriter(System.out, false, StandardCharsets.UTF_8), 16 * 1024);
  }

  CSVReader createCsvReader(String csvFile) throws FileNotFoundException {
    return new CSVReader(new File(csvFile), StandardCharsets.UTF_8);
  }

  String[] getSplitedCsvRecord(CSVReader reader) throws IOException {
    return reader.getNextCsvLine(1, true);
  }

  String[] getSplitedCsvRecord(CSVReader reader, int columnSize) throws IOException {
    return reader.getNextCsvLine(columnSize, false);
  }

  public void insert(CSVReader csvReader, FileWriter fileWriter, String[] tokens) throws Exception {
    var header = readHeader(csvReader);
    csvReader.close();
    if (tokens.length == 6) {
      header.setColumn(tokens[3]);
      var selectColumnIndexs = header.getSelectColumnIndexes();
      var data = tokens[5].split(" *, *");
      for (var i = 0; i < header.length(); i++) {
        if (i > 0) {
          fileWriter.append(',');
        }
        for (var j = 0; j < selectColumnIndexs.length; j++) {
          if (selectColumnIndexs[j] == i) {
            fileWriter.append(
                data[j].startsWith("'") ? data[j].substring(1, data[j].length() - 1) : data[j]);
            break;
          }
        }
      }
    } else {
      var data = tokens[4].split(" *, *");
      for (var i = 0; i < header.length(); i++) {
        if (i > 0) {
          fileWriter.append(',');
        }
        fileWriter.append(
            data[i].startsWith("'") ? data[i].substring(1, data[i].length() - 1) : data[i]);
      }
    }
    fileWriter.append("\n");
    fileWriter.flush();
  }
}
