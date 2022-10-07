package run.been.flinkdemo.window


import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.{SensorReading, SensorSource, SensorTimeAssigner}

import java.time.Duration

object SideOutputs {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /**
     * 定义水印生成策略
     */
    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(3)) //延迟5秒
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp //指定事件时间字段
      })

    val inputStream = env.socketTextStream("localhost", 9999)
      .map { text =>
        val arr: Array[String] = text.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }.assignTimestampsAndWatermarks(strategy)

    inputStream.print()
    val outputTage = new OutputTag[SensorReading]("later data")

    val sumResult = inputStream.keyBy(_.id)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) //10秒一个窗口，每5秒钟是一个滑动一次
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(outputTage)
      .reduce((newSensor, oldSensor) => SensorReading(oldSensor.id,oldSensor.timestamp,oldSensor.temperature + newSensor.temperature))


    sumResult.print("main stream ")

    val sideOutput = sumResult.getSideOutput(outputTage)
    sideOutput.print("side output stream ")

    env.execute()
  }
}

/**
 * 输入数据
 * sensor_1,2000,1.0
sensor_1,7000,1.0
sensor_1,8000,1.0
sensor_1,9000,1.0
sensor_1,10000,1.0
sensor_1,12000,1.0
sensor_1,13000,1.0
sensor_1,6000,1.0
sensor_1,7500,1.0
sensor_1,12000,1.0
sensor_1,13000,1.0
sensor_1,15000,1.0
sensor_1,17000,1.0
sensor_1,18000,1.0
sensor_1,1000,1.0
 */

/**
 * 输出结果
 * SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,7000,1.0)
SensorReading(sensor_1,8000,1.0)
main stream > SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,9000,1.0)
SensorReading(sensor_1,10000,1.0)
SensorReading(sensor_1,12000,1.0)
SensorReading(sensor_1,13000,1.0)
main stream > SensorReading(sensor_1,9000,4.0)
SensorReading(sensor_1,6000,1.0)
main stream > SensorReading(sensor_1,6000,5.0)
SensorReading(sensor_1,7500,1.0)
main stream > SensorReading(sensor_1,7500,6.0)
SensorReading(sensor_1,12000,1.0)
SensorReading(sensor_1,13000,1.0)
SensorReading(sensor_1,15000,1.0)
SensorReading(sensor_1,17000,1.0)
SensorReading(sensor_1,18000,1.0)
main stream > SensorReading(sensor_1,13000,10.0)
SensorReading(sensor_1,1000,1.0)
side output stream > SensorReading(sensor_1,1000,1.0)

 */