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
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class CsvService {

  public void read(String csvFile, String[] tokens) throws Exception {
    try (var reader = createCsvReader(csvFile)) {
      var query = createQuery(reader, tokens);
      readBody(reader, query);
    }
  }

  public void readUpdate(String csvFile, String[] tokens) throws Exception {
    try (var reader = createCsvReader(csvFile)) {
      var query = createUpdateQuery(reader, tokens);
      query.getHeader().setColumn("*");
      // 更新値
      var updateValues = createUpdateValues(query, tokens);
      readUpdateBody(reader, query, updateValues);
    }
  }

  public void readDelete(String csvFile, String[] tokens) throws Exception {
    try (var reader = createCsvReader(csvFile)) {
      var query = createDeleteQuery(reader, tokens);
      query.getHeader().setColumn("*");
      readDeleteBody(reader, query);
    }
  }

  public Query createQuery(CSVReader reader, String[] tokens) throws Exception {
    var header = readHeader(reader);
    header.setColumn(tokens[1]);
    var where = readWhere(header, tokens);
    return new Query(header, where);
  }

  public Query createDeleteQuery(CSVReader reader, String[] tokens) throws Exception {
    var header = readHeader(reader);
    var where = readDeleteWhere(header, tokens);
    return new Query(header, where);
  }

  public Query createUpdateQuery(CSVReader reader, String[] tokens) throws Exception {
    var header = readHeader(reader);
    var where = readUpdateWhere(header, tokens);
    return new Query(header, where);
  }

  Where readWhere(Header header, String[] tokens) throws Exception {
    if (tokens.length < 5) {
      return null;
    }
    return new Where(header, Arrays.copyOfRange(tokens, 5, tokens.length));
  }

  Where readDeleteWhere(Header header, String[] tokens) throws Exception {
    if (tokens.length < 4) {
      return null;
    }
    return new Where(header, Arrays.copyOfRange(tokens, 4, tokens.length));
  }

  Where readUpdateWhere(Header header, String[] tokens) throws Exception {
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

  void readUpdateBody(CSVReader reader, Query query, String[] updateValues) throws Exception {
    var header = query.getHeader();
    try (var writer = createWriter()) {
      writer.write(header.getColumnHeaderString());
      writer.write('\n');
      var recordLength = header.length();
      String[] splitedCsvRecord = null;
      while ((splitedCsvRecord = getSplitedCsvRecord(reader, recordLength)) != null) {
        if (!query.match(splitedCsvRecord)) {
          writeRecord(writer, splitedCsvRecord);
        } else {
          writeUpdateRecord(writer, splitedCsvRecord, updateValues);
        }
      }
    }
  }

  void readDeleteBody(CSVReader reader, Query query) throws Exception {
    var header = query.getHeader();
    try (var writer = createWriter()) {
      writer.write(header.getColumnHeaderString());
      writer.write('\n');
      var recordLength = header.length();
      String[] splitedCsvRecord = null;
      while ((splitedCsvRecord = getSplitedCsvRecord(reader, recordLength)) != null) {
        if (query.match(splitedCsvRecord)) {
          continue;
        }
        writeRecord(writer, splitedCsvRecord);
      }
    }
  }

  void writeRecord(Writer writer, String[] splitedCsvRecord) throws IOException {
    var iMax = splitedCsvRecord.length;
    writer.write(splitedCsvRecord[0]);
    for (int i = 1; i < iMax; i++) {
      writer.write(',');
      writer.write(splitedCsvRecord[i]);
    }
    writer.write('\n');
  }

  void writeUpdateRecord(Writer writer, String[] splitedCsvRecord, String[] updateValues)
      throws IOException {
    var iMax = splitedCsvRecord.length;
    writer.write(getValue(updateValues[0], splitedCsvRecord[0]));
    for (var i = 1; i < iMax; i++) {
      writer.write(',');
      writer.write(getValue(updateValues[i], splitedCsvRecord[i]));
    }
    writer.write('\n');
  }

  String getValue(String updateValue, String orgValue) {
    return updateValue == null ? orgValue : updateValue;
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
    while (true) {
      var record = reader.getNextCsvLine(columnSize, false);
      if (record == null) {
        return null;
      }
      if (record[0].charAt(0) == (char) '\u007f') {
        continue;
      }
      return record;
    }
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

  public int delete(CSVReader csvReader, RandomAccessFile randomAccessFile, String[] tokens)
      throws Exception {

    if (tokens.length == 3) {
      // ヘッダを削除指定はいけないのでこれはだめ。
      return deleteAll(randomAccessFile);
    } else {
      return deleteRecord(csvReader, randomAccessFile, tokens);
    }
  }

  int deleteAll(RandomAccessFile randomAccessFile) throws IOException {
    for (var filePointer = 0; filePointer < randomAccessFile.length(); filePointer++) {
      randomAccessFile.seek(filePointer);
      int bytesRead = randomAccessFile.read();
      if (bytesRead == '\n') {
        randomAccessFile.setLength(filePointer + 1);
        return 0;
      }
    }
    return 0;
  }

  int deleteRecord(CSVReader csvReader, RandomAccessFile randomAccessFile, String[] tokens)
      throws Exception {
    var query = createDeleteQuery(csvReader, tokens);
    return deleteBody(csvReader, randomAccessFile, query);
  }

  int deleteBody(CSVReader reader, RandomAccessFile randomAccessFile, Query query)
      throws Exception {
    var header = query.getHeader();
    var recordLength = header.length();
    String[] splitedCsvRecord = null;
    var count = 0;
    while ((splitedCsvRecord = getSplitedCsvRecord(reader, recordLength)) != null) {
      if (!query.match(splitedCsvRecord)) {
        continue;
      }
      randomAccessFile.seek(reader.getRecordFromIndex());
      byte[] bytes = new byte[reader.getRecordLength() - 1];
      Arrays.fill(bytes, (byte) '\u007f');
      randomAccessFile.write(bytes);
      count++;
    }
    return count;
  }

  public int update(CSVReader csvReader, RandomAccessFile randomAccessFile, String[] tokens)
      throws Exception {
    var query = createUpdateQuery(csvReader, tokens);
    // 更新値
    var updateValues = createUpdateValues(query, tokens);
    return updateBody(csvReader, randomAccessFile, query, updateValues);
  }

  String[] createUpdateValues(Query query, String[] tokens) {
    var header = query.getHeader();
    var updateValues = new String[header.length()];
    var setValues = tokens[3].split(",");
    for (var setValue : setValues) {
      var keyValue = setValue.split("=");
      var value =
          keyValue[1].charAt(0) == '\''
              ? keyValue[1].substring(1, keyValue[1].length() - 1)
              : keyValue[1];
      updateValues[header.getColumnIndex(keyValue[0])] = value;
    }
    return updateValues;
  }

  int updateBody(
      CSVReader reader, RandomAccessFile randomAccessFile, Query query, String[] updateValues)
      throws Exception {
    var header = query.getHeader();
    var recordLength = header.length();
    String[] splitedCsvRecord = null;
    var updateList = new ArrayList<String[]>();
    while ((splitedCsvRecord = getSplitedCsvRecord(reader, recordLength)) != null) {
      if (!query.match(splitedCsvRecord)) {
        continue;
      }
      updateList.add(splitedCsvRecord);
      // 削除
      randomAccessFile.seek(reader.getRecordFromIndex());
      byte[] bytes = new byte[reader.getRecordLength() - 1];
      Arrays.fill(bytes, (byte) '\u007f');
      randomAccessFile.write(bytes);
    }
    for (var record : updateList) {
      // 追加
      randomAccessFile.seek(randomAccessFile.length());
      for (var i = 0; i < record.length; i++) {
        if (i > 0) {
          randomAccessFile.write(",".getBytes(StandardCharsets.UTF_8));
        }
        var value = updateValues[i] == null ? record[i] : updateValues[i];
        randomAccessFile.write(value.getBytes(StandardCharsets.UTF_8));
      }
      randomAccessFile.write("\n".getBytes(StandardCharsets.UTF_8));
    }
    return updateList.size();
  }
}
