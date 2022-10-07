package run.been.flinkdemo.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

object ValueStateDemo {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

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

    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
      .flatMap(new MyValueFlatMapFunction())


    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }

}

class MyValueFlatMapFunction extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  //定义值状态
  var valueState: ValueState[SensorReading] = _

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[SensorReading]("my-value", classOf[SensorReading]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //获取状态的值，对状态进行更新
    println("值状态为：" + valueState.value())
    valueState.update(value)
    println("值状态为：" + valueState.value())
  }
}

/**
 * 出现2个null是因为不同的key会在第一次初始化为null
 * 值状态为：null
值状态为：SensorReading(sensor_1,1000,1.0)
值状态为：null
值状态为：SensorReading(sensor_2,1000,1.0)
值状态为：SensorReading(sensor_2,1000,1.0)
值状态为：SensorReading(sensor_2,1000,1.0)
 */