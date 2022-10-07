package run.been.flinkdemo.sql

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object FlinkSqlBatch {

  def test02() = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val tableEnv = StreamTableEnvironment.create(env)

    // Create a sink table (using SQL DDL)
    val sourceSql =
      """
        |
        |CREATE TABLE SourceTable (
        |    id INT,
        |    description1 STRING,
        |    description2 STRING,
        |    description3 STRING,
        |        PRIMARY KEY(`id`) NOT ENFORCED
        |) WITH (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://10.10.21.226:3306/hbgg_data_center?useSSL=false',
        |    'table-name' = 't_equipment',
        |    'username' = 'root',
        |    'password' = '@BJcenter123'
        |);
        |""".stripMargin
    tableEnv.executeSql(sourceSql)

    val sinkSql =
      """
        |CREATE TABLE  sinkTable (
        |    id INT,
        |    description1 STRING,
        |    description2 STRING,
        |    description3 STRING,
        |    PRIMARY KEY(`id`) NOT ENFORCED
        |) WITH (
        |    'connector' ='jdbc',
        |    'url'='jdbc:mysql://10.10.21.226:3306/db01?useSSL=false',
        |    'table-name'='t_equipment',
        |    'username'='root',
        |    'password'='@BJcenter123'
        |);
        |""".stripMargin
    tableEnv.executeSql(sinkSql)

    // Create a Table object from a Table API query
    val table1 = tableEnv.from("SourceTable")

    // Create a Table object from a SQL query
    val table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable")
    println(table2.printExplain())
//    val result = table1.insertInto("sinkTable").execute()

    val insert =
      """
        |insert into sinkTable
        |    select * from SourceTable
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
