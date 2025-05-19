// (C) 2025 uchicom
package com.uchicom.csvdb.factory.di;

import com.uchicom.csvdb.Main;
import com.uchicom.csvdb.logger.DailyRollingFileHandler;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class DIFactory {

  public static Main main() throws FileNotFoundException, IOException {
    return new Main(ServiceFactory.csvService());
  }

  public static Logger logger() {
    try {
      var PROJECT_NAME = "csvdb";
      var name =
          Stream.of(Thread.currentThread().getStackTrace())
              .map(StackTraceElement::getClassName)
              .filter(className -> className.endsWith("Main"))
              .findFirst()
              .orElse(PROJECT_NAME);
      Logger logger = Logger.getLogger(name);
      if (!PROJECT_NAME.equals(name)) {
        if (Arrays.stream(logger.getHandlers())
            .filter(handler -> handler instanceof DailyRollingFileHandler)
            .findFirst()
            .isEmpty()) {
          logger.addHandler(new DailyRollingFileHandler(name + "_%d.log"));
        }
      }
      return logger;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Properties properties(String file) throws FileNotFoundException, IOException {

    try (var fis = new FileInputStream(file)) {
      return properties(fis);
    }
  }

  public static Properties properties(InputStream is) throws FileNotFoundException, IOException {
    var properties = new Properties();
    properties.load(is);
    return properties;
  }

  static String csvFileNameFormat(Properties config) {
    return String.format(
        config.getProperty("csv.file_name_format"), now(config.getProperty("csv.zone_id")));
  }

  static LocalDateTime now(String zoneId) {
    return LocalDateTime.now(ZoneId.of(zoneId));
  }
}
