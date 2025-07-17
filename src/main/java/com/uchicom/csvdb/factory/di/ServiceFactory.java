// (C) 2025 uchicom
package com.uchicom.csvdb.factory.di;

import com.uchicom.csvdb.service.CsvService;

public class ServiceFactory {
  public static CsvService csvService() {
    return new CsvService();
  }
}
