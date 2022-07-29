package run.been.flinkdemo.window


import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment, WindowedStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import run.been.flinkdemo.util.SensorReading

import java.time.Duration
import scala.::

object WindowAllDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    test01(env)
    env.execute("flink job")
  }

//  获取最小值和最新的时间戳
  def test01(env: StreamExecutionEnvironment) = {
    val dstream = env.socketTextStream("localhost", 9999)

    val textWithTsDstream= dstream.map { text =>
      val arr: Array[String] = text.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }

//    val value: AllWindowedStream[SensorReading, GlobalWindow] = textWithTsDstream.windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(5)))
//    value.min(2).print()
//      .assignTimestampsAndWatermarks(WatermarkStrategy
//      .forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(5))
//      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
//        override def extractTimestamp(element:SensorReading, recordTimestamp: Long): Long = element.timestamp
//      }))
////    textWithTsDstream.print()
//
//    val resultStream = textWithTsDstream
//      .keyBy(_.id)    // 按照二元组的第一个元素（id）分组
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))    // 滚动时间窗口
//      .sum("temperature")
//    resultStream.print()



//    val textWithEventTimeDstream: DataStream[(String, Long, Int)] = textWithTsDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000)) {
//      override def extractTimestamp(element: (String, Long, Int)): Long = {
//
//        return  element._2
//      }
//    })
//    textWithEventTimeDstream.print()
//
//    val textKeyStream: KeyedStream[(String, Long, Int), Tuple] = textWithEventTimeDstream.keyBy(0)
//    textKeyStream.print("textkey:")
//
//    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))
//



  }

}
