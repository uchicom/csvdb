# csvdb
csv database

## jar実行
```
java -jar ./target/csvdb-0.0.1-SNAPSHOT-jar-with-dependencies.jar "select a,c from test.csv where b > 2"
```
### SQL
* selectのカラムは_a,c_のようにカンマ区切りでつなげて記載する
* whereの_b > 2_のようにスペース区切りで記載する.bはカラム名で左側に記載、右側は値を記載.
* whereに使用できる条件は現在のところ1つのみ
* whereで指定できる比較演算子は=,!=,<,>,<=,>=の6種類
* whereでintを指定する場合は、23と記載する
* whereでlongを指定する場合は、23LとLをつける
* whereでfloatを指定する場合は、23.0FとFをつける
* whereでdoubleを指定する場合は、23.0DとDをつける
* whereでbooleanを指定する場合は、trueBをつける
* whereでStringを指定する場合は、'23'とシングルクォーテーションで囲む

## mvn
### 実行
```
mvn exec:java "-Dexec.mainClass=com.uchicom.csvdb.Main" "-Dexec.args='select * from test.csv'"
```

### フォーマッタ
```
mvn spotless:apply
```

### 全体テスト実行
```
mvn verify
```

### フォーマッタ & 全体テスト実行
```
mvn spotless:apply clean compile verify
```
