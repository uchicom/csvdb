// (C) 2025 uchicom
package com.uchicom.csvdb.sql;

import java.util.function.Function;

public class Condition<T> {
  private final int columnIndex;
  private final Function<String, Boolean> operatorFunction;
  private T comparedValue;
  private final Function<String, Comparable<T>> convertorFunction;

  public Condition(
      int columnIndex,
      String operation,
      T value,
      Function<String, Comparable<T>> convertorFunction) {
    this.columnIndex = columnIndex;
    this.convertorFunction = convertorFunction;
    this.operatorFunction = operator(operation);
    this.comparedValue = value;
  }

  Function<String, Boolean> operator(String operator) {
    return switch (operator) {
      case "=" -> value -> convertorFunction.apply(value).compareTo(this.comparedValue) == 0;
      case "<" -> value -> convertorFunction.apply(value).compareTo(this.comparedValue) < 0;
      case "<=" -> value -> convertorFunction.apply(value).compareTo(this.comparedValue) <= 0;
      case ">" -> value -> convertorFunction.apply(value).compareTo(this.comparedValue) > 0;
      case ">=" -> value -> convertorFunction.apply(value).compareTo(this.comparedValue) >= 0;
      case "!=" -> value -> convertorFunction.apply(value).compareTo(this.comparedValue) != 0;
      default -> throw new IllegalArgumentException("Invalid operator: " + operator);
    };
  }

  public boolean match(String[] record) {
    var value = record[columnIndex];
    if (value == null) {
      return false;
    }
    return operatorFunction.apply(value);
  }
}
