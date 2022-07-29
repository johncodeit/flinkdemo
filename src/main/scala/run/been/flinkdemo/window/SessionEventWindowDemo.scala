package run.been.flinkdemo.window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

/**
 *
 * 需求：会话窗口，
 */
object SessionEventWindowDemo {

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
      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))//10秒一个回话窗口，如果10秒没有收到数据就统计

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
   */
/**
 * 输出结果1：
 * SensorReading(sensor_1,1000,1.0)
SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,3000,1.0)
*/

  //这里没有进行计算

  /**
   * 接着输入数据2
   * sensor_1,13001,1.0
   */
  /**
   * 输出结果2：
   * SensorReading(sensor_1,13001,1.0)
   * reduce==> SensorReading(sensor_1,3000,3.0)
   */

  /**
   * 第一次计算输出总数：3,数据sensor_1,13001,1.0没有计算在内
   * 13001-3000=10001>10000所以开始计算
   */
  /**
   * sensor_1,1000,1.0
sensor_1,2000,1.0
sensor_1,3000,1.0
   */

  /**
   * 接着输入数据3
   * sensor_1,15000,1.0
sensor_1,16000,1.0
   */
  /**
   * 输出结果3：[0,5000）,第一个是5秒是因为要进行滑动了，所以前面的要进行计算出结果
   * SensorReading(sensor_1,15000,1.0)
SensorReading(sensor_1,16000,1.0)
   */
  //没有触发

  /**
   * 接着输入数据4
   * sensor_1,26001,1.0
   */

  /**
   * 输出结果3：
   * 26001-26000=10001>10000所以开始计算
   * SensorReading(sensor_1,26001,1.0)
reduce==> SensorReading(sensor_1,16000,3.0)
   */


  /**
   * 回话窗口是按照时间间隔，进行计算，符合时间间隔就会触发计算。
   */


  /**
   * 输入数据
   * sensor_1,1000,1.0
sensor_1,2000,1.0
sensor_1,3000,1.0
sensor_1,13001,1.0
sensor_1,15000,1.0
sensor_1,16000,1.0
sensor_1,26001,1.0
   */



  /**
   * 输出结果：
   * SensorReading(sensor_1,1000,1.0)
SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,3000,1.0)
SensorReading(sensor_1,13001,1.0)
reduce==> SensorReading(sensor_1,3000,3.0)
SensorReading(sensor_1,15000,1.0)
SensorReading(sensor_1,16000,1.0)
SensorReading(sensor_1,26001,1.0)
reduce==> SensorReading(sensor_1,16000,3.0)

   */

}
