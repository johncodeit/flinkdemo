package run.been.flinkdemo.sql

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object HdfsToClickhouse {

  def test02() = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val tableEnv = StreamTableEnvironment.create(env)

    // Create a sink table (using SQL DDL)
    val sourceSql =
      """
        |
        |CREATE TABLE  sourceTable (
        |    `day` String,
        |    number_plate String,
        |    gate_post_code String,
        |    dev_name String,
        |    direct String,
        |    in_out_time TIMESTAMP,
        |    car_img String,
        |    plate_img String,
        |    car_color String,
        |    parking_car_color String
        |) WITH (
        |    'connector' = 'filesystem',
        |    'path' = 'hdfs://mycluster/data/bigdata/database/entrance_guard_system/ads_without_app_inandout/dt=2022-07-15',
        |     'format' = 'csv'
        |);
        |""".stripMargin
    tableEnv.executeSql(sourceSql)

    val sinkSql =
      """
        |
        |CREATE TABLE  sinkTable (
        |    `day` String,
        |    number_plate String,
        |    gate_post_code String,
        |    dev_name String,
        |    direct String,
        |    in_out_time TIMESTAMP,
        |    car_img String,
        |    plate_img String,
        |    car_color String,
        |    parking_car_color String
        |) WITH (
        |    'connector' = 'clickhouse',
        |    'url' = 'jdbc:clickhouse://node1:8123/entrance_guard_system',
        |    'table-name' = 'ads_without_app_inandout_test',
        |    'username' = 'default',
        |    'password' = 'qwert',
        |     'format' = 'csv'
        |);
        |""".stripMargin
    tableEnv.executeSql(sinkSql)

    // Create a Table object from a Table API query
    val table1 = tableEnv.from("sourceTable")

    // Create a Table object from a SQL query
//    val table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable")
//    println(table2.printExplain())
//    val result = table1.insertInto("sinkTable").execute()

    val insert =
      """
        |insert into sinkTable
        |    select * from sourceTable
        |""".stripMargin

    tableEnv.executeSql(insert)
    // Emit a Table API result Table to a TableSink, same for SQL result
//    val tableResult = table1.insertInto("sinkTable").execute()
//    println(table2.printExplain())

  }

  def main(args: Array[String]): Unit = {

//    env.setParallelism(1)
    test02()
//    env.execute("flink sql batch")
  }

}
