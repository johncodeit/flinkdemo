package run.been.flinkdemo.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import run.been.flinkdemo.util.SensorReading

/**
 * 数据数据格式：sensor_1,1655676604000,1.0
 * 需求：滚动窗口为5秒的窗口。按照处理时间进行滚动。
 */
object TumblingProcessWindowDemo {

  def test01(env: StreamExecutionEnvironment) = {
    val inputStream = env.socketTextStream("localhost", 9999).map { text =>
      val arr: Array[String] = text.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }

    val keyByStream = inputStream.keyBy(x =>x.id)
    /**
     * 第一个参数Stream中的数据类型，第二个参数key的类型，第三个是窗口类型
     */
    val windowStream: WindowedStream[SensorReading, String, TimeWindow] = keyByStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    val sumDataStream: DataStream[SensorReading] = windowStream.sum("temperature")
    sumDataStream.print()
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    test01(env)
    env.execute("flink job")
  }

}
