package run.been.flinkdemo.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

object MapStateDemo {
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(2* 1000)

    /**
     * 定义水印生成策略
     */
    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(5)) //延迟0秒
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp //指定事件时间字段
      })

    // ingest sensor stream
    val sensorData = env.socketTextStream("localhost", 9999)
      .map { text =>
        val arr: Array[String] = text.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }.assignTimestampsAndWatermarks(strategy)


    val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)

    val alerts = keyedSensorData.process(new AggregatingStateDemo())

    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }

}

//class MapStateFunction extends KeyedProcessFunction[String,SensorReading, String]{
//  private var mapState:MapState[]
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//
//  }
//
//  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = ???
//}