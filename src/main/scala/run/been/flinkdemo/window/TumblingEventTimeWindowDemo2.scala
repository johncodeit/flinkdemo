package run.been.flinkdemo.window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import run.been.flinkdemo.util.{SensorReading}

import java.time.Duration

/**
 *
 * 需求：滚动窗口为5秒的窗口。按照事件时间进行处理
 */
object TumblingEventTimeWindowDemo2 {

  def test02(env: StreamExecutionEnvironment) = {
    /**
     * 定义水印生成策略
     */
    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(0)) //延迟0秒
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp //指定事件时间字段
      })

    val inputStream = env.socketTextStream("localhost", 9999)
      .map { text =>
        val arr: Array[String] = text.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      } .assignTimestampsAndWatermarks(strategy)
          .keyBy(x => x.id)

    inputStream.print()
    /**
     * 第一个参数Stream中的数据类型，第二个参数key的类型，第三个是窗口类型
     */
    val windowStream: WindowedStream[SensorReading, String, TimeWindow] = inputStream
      .keyBy(x => x.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))//5秒钟是一个滚动窗口
//      .evictor()
    //获取温度总和，没过5分钟滑动一次窗口
    val reduceWindowStream: DataStream[SensorReading] = windowStream
      .reduce((newSensor, oldSensor) => SensorReading(oldSensor.id,oldSensor.timestamp,oldSensor.temperature + newSensor.temperature))

    reduceWindowStream.print("reduce==")
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    println(env.getParallelism)
    test02(env)
    env.execute("window job")
  }

  /**
   * 输入数据1
   * sensor_1,1000,1.0
sensor_1,2000,1.0
sensor_1,3000,1.0
sensor_1,4000,1.0
sensor_1,4999,1.0
   */
/**
 * 输出结果1：
 * SensorReading(sensor_1,1000,1.0)
SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,3000,1.0)
SensorReading(sensor_1,4000,1.0)
SensorReading(sensor_1,4999,1.0)
*/


  /**
   * 接着输入数据2
   * sensor_1,5000,1.0
   */
  /**
   * 输出结果2：
   * SensorReading(sensor_1,5000,1.0)
reduce==> SensorReading(sensor_1,4999,5.0)
   */

  /**
   * 接着输入数据3
   * sensor_1,6000,1.0
sensor_1,7000,1.0
   */
  /**
   * 输出结果3：
   * SensorReading(sensor_1,6000,1.0)
SensorReading(sensor_1,7000,1.0)
   */

  /**
   * 接着输入数据4
   * sensor_1,10000,1.0
   */
  /**
   * 输出结果4：
   * SensorReading(sensor_1,10000,1.0)
reduce==> SensorReading(sensor_1,7000,3.0)
   */

  /**
   * 接着输入数据5
   * sensor_1,11000,1.0
sensor_1,12000,1.0
sensor_1,13000,1.0
   */
  /**
   * 输出结果5：
   * SensorReading(sensor_1,11000,1.0)
SensorReading(sensor_1,12000,1.0)
SensorReading(sensor_1,13000,1.0)
   */

  /**
   * 接着输入数据6
   * sensor_1,15000,1.0
   */
  /**
   * 输出结果6：
   * SensorReading(sensor_1,15000,1.0)
reduce==> SensorReading(sensor_1,13000,4.0)
   */

  /**
   * 通过上面分析，从数据中获取数据的时间戳，为事件时间，延迟设置为0，
   * 滚动窗口设置为5，表示将时间按照每5秒一个滚动窗口，从0秒开始计算。
   */


  /**
   * 输入数据
   * sensor_1,1000,1.0
sensor_1,2000,1.0
sensor_1,3000,1.0
sensor_1,4000,1.0
sensor_1,4999,1.0
sensor_1,5000,1.0
sensor_1,6000,1.0
sensor_1,7000,1.0
sensor_1,10000,1.0
sensor_1,11000,1.0
sensor_1,12000,1.0
sensor_1,13000,1.0
sensor_1,15000,1.0
   */



  /**
   * 输出结果：
   * SensorReading(sensor_1,1000,1.0)
SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,3000,1.0)
SensorReading(sensor_1,4000,1.0)
SensorReading(sensor_1,4999,1.0)
SensorReading(sensor_1,5000,1.0)
reduce==> SensorReading(sensor_1,4999,5.0)
SensorReading(sensor_1,6000,1.0)
SensorReading(sensor_1,7000,1.0)
SensorReading(sensor_1,10000,1.0)
reduce==> SensorReading(sensor_1,7000,3.0)
SensorReading(sensor_1,11000,1.0)
SensorReading(sensor_1,12000,1.0)
SensorReading(sensor_1,13000,1.0)
SensorReading(sensor_1,15000,1.0)
reduce==> SensorReading(sensor_1,13000,4.0)
   */

}
