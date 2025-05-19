// (C) 2025 uchicom
package com.uchicom.csvdb.sql;

import java.util.ArrayList;
import java.util.List;

public class Where {
  private final List<Condition<?>> conditionList;

  public Where(Header header, String[] conditions) {
    conditionList = new ArrayList<>(conditions.length / 3);
    for (int i = 0; i < conditions.length; i += 3) {
      var value = conditions[i + 2];
      var lastChar = value.charAt(value.length() - 1);
      var cutted = value.substring(0, value.length() - 1);
      var columnIndex = header.getColumnIndex(conditions[i]);
      var operation = conditions[i + 1];
      var condition =
          switch (lastChar) {
            case '\'' -> new Condition<>(
                columnIndex,
                operation,
                value.substring(1, value.length() - 1),
                converted -> converted);
            case 'L' -> new Condition<>(
                columnIndex, operation, Long.valueOf(cutted), Long::valueOf);
            case 'F' -> new Condition<>(
                columnIndex, operation, Float.valueOf(cutted), Float::valueOf);
            case 'D' -> new Condition<>(
                columnIndex, operation, Double.valueOf(cutted), Double::valueOf);
            case 'B' -> new Condition<>(
                columnIndex, operation, Boolean.valueOf(cutted), Boolean::valueOf);
            default -> new Condition<>(
                columnIndex, operation, Integer.valueOf(value), Integer::valueOf);
          };
      conditionList.add(condition);
    }
  }

  public boolean match(String[] record) {
    if (conditionList == null || conditionList.isEmpty()) {
      return true;
    }
    for (Condition<?> condition : conditionList) {
      if (!condition.match(record)) {
        return false;
      }
    }
    return true;
  }
}
