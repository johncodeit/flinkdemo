package run.been.flinkdemo.window


import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.{SensorReading, SensorSource, SensorTimeAssigner}

import java.time.Duration

object SideOutputs {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // configure watermark interval
//    env.getConfig.setAutoWatermarkInterval(1000L)

    /**
     * 定义水印生成策略
     */
    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(5)) //延迟0秒
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp //指定事件时间字段
      })

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(strategy)

    val monitoredReadings: DataStream[SensorReading] = readings
      // monitor stream for readings with freezing temperatures
      .process(new FreezingMonitor)

    // retrieve and print the freezing alarms
    monitoredReadings
      .getSideOutput(new OutputTag[String]("freezing-alarms"))
      .print()

    // print the main output
    readings.print()

    env.execute()
  }
}

/** Emits freezing alarms to a side output for readings with a temperature below 32F. */
class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {

  // define a side output tag
  lazy val freezingAlarmOutput: OutputTag[String] =
    new OutputTag[String]("freezing-alarms")

  override def processElement(
      r: SensorReading,
      ctx: ProcessFunction[SensorReading, SensorReading]#Context,
      out: Collector[SensorReading]): Unit = {
    // emit freezing alarm if temperature is below 32F.
    if (r.temperature < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${r.id}"+"====" + r.temperature)
    }
    // forward all readings to the regular output
    out.collect(r)
  }
}
