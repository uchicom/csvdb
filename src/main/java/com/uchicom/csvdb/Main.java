// (C) 2025 uchicom
package com.uchicom.csvdb;

import com.uchicom.csvdb.factory.di.DIFactory;
import com.uchicom.csvdb.service.CsvService;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;

public class Main {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    var logger = DIFactory.logger();
    logger.info("start");
    if (args.length == 0) {
      logger.log(Level.SEVERE, "Invalid Parameter Empty");
      return;
    }
    try {
      var main = DIFactory.main();
      main.execute(args[0]);
    } catch (Throwable e) {
      logger.log(Level.SEVERE, "System Error", e);
    }
    logger.info("end");
  }

  private final CsvService csvService;

  public Main(CsvService csvService) {
    this.csvService = csvService;
  }

  void execute(String sql) throws Exception {
    var tokens = sql.split(" ", 0);
    if (tokens[0].equalsIgnoreCase("select")) {
      select(tokens);
    } else if (tokens[0].equalsIgnoreCase("update")) {
      update(tokens);
    } else {
      throw new IllegalArgumentException("Invalid SQL");
    }
  }

  void select(String[] tokens) throws Exception {
    if (tokens.length < 4) {
      throw new IllegalArgumentException("Invalid SQL");
    }
    csvService.read(tokens[3], tokens);
  }

  void update(String[] tokens) throws Exception {
    if (tokens.length < 4) {
      throw new IllegalArgumentException("Invalid SQL");
    }
    csvService.readUpdate(tokens[1], tokens);
  }
}
