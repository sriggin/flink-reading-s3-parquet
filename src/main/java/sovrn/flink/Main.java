package sovrn.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
// import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;

public class Main {

  public static void main(String[] args) {
    // who knows
    new Main().run();
  }

  void run() {

    // Create a TableEnvironment for batch or streaming execution.
    // See the "Create a TableEnvironment" section for details.
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inStreamingMode()
        .build();

    TableEnvironment tableEnv = TableEnvironment.create(settings);

    System.out.println("Starting application");

    tableEnv.executeSql("CREATE TEMPORARY TABLE cookies (\n"
        + "lijit_identity STRING,\n"
        + "provider_id STRING,\n"
        + "third_party_identity STRING,\n"
        + "dt STRING\n"
        + ")\n"
        + " PARTITIONED BY (dt)\n"
        + " WITH (\n"
        + "'connector' = 'filesystem',\n"
        + "'path' = 's3a://sean-flink-test/data',\n"
        + "'format' = 'parquet'\n"
        + ")");

    tableEnv.executeSql("CREATE TABLE print_table \n"
        + "(res1 BIGINT)\n"
        + "WITH ('connector' = 'print')");

    Table t = tableEnv.sqlQuery("SELECT count(*) as res1 FROM cookies");
    t.executeInsert("print_table");


  }

}
