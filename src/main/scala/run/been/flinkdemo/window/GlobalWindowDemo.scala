package run.been.flinkdemo.window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

/**
 *
 * 需求：
 */
object GlobalWindowDemo {

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
//        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        arr(2).toDouble
      }
//      .assignTimestampsAndWatermarks(strategy)
//          .keyBy(x => x.id)

    inputStream.print()
//    val value: WindowedStream[SensorReading, String, GlobalWindow] = inputStream
//      .keyBy(x => x)
//      .window(GlobalWindows.create())
//      .trigger(PurgingTrigger.of(CountTrigger.of(2)))
    /**
     * 第一个参数Stream中的数据类型，第二个参数key的类型，第三个是窗口类型
     */
//    val windowStream = value


//    val reduceWindowStream: DataStream[SensorReading] = windowStream
//      .reduce((newSensor, oldSensor) => SensorReading(oldSensor.id,oldSensor.timestamp,oldSensor.temperature + newSensor.temperature))

//    reduceWindowStream.print("reduce==")
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    println(env.getParallelism)
    test02(env)
    env.execute("window job")
  }


//  class MyTrigger extends PurgingTrigger
  /**
   * 输入数据1
   * sensor_1,1000,1.0
sensor_1,2000,1.0
   */
/**
 * 输出结果1：
 * SensorReading(sensor_1,1000,1.0)
SensorReading(sensor_1,2000,1.0)
*/

  //这里没有进行计算

  /**
   * 接着输入数据2
   * sensor_1,3000,1.0
   */
  /**
   * 输出结果2：
   * reduce==> SensorReading(sensor_1,3000,3.0)
   */

  /**
   * 第一次计算输出总数：3,数据sensor_1,3000,1.0没有计算在内
   * 已经够5条记录
   */
  /**
   * SensorReading(sensor_1,1000,1.0)
SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,3000,1.0)
   */

  /**
   * 接着输入数据3
   * sensor_1,4000,1.0
sensor_1,5000,1.0
   */
  /**
   * 输出结果3：
   * SensorReading(sensor_1,4000,1.0)
SensorReading(sensor_1,5000,1.0)
   */
  //没有触发

  /**
   * 接着输入数据4
   * sensor_1,6000,1.0
   */

  /**
   * 输出结果3：
   * SensorReading(sensor_1,6000,1.0)
reduce==> SensorReading(sensor_1,6000,5.0)
   */

  /**
   * 接着输入数据4
   * sensor_1,6000,1.0
   */

  /**
   * 输出结果3：
   * SensorReading(sensor_1,6000,1.0)
reduce==> SensorReading(sensor_1,6000,5.0)
   */


  /**
   * 计数窗口，是按照统计个数进行开窗，每5个滚动一次
   */


  /**
   * 输入数据
   * sensor_1,1000,1.0
sensor_1,2000,1.0
sensor_1,3000,1.0
sensor_1,4000,1.0
sensor_1,5000,1.0
sensor_1,6000,1.0
sensor_1,7000,1.0
sensor_1,8000,1.0
sensor_1,9000,2.0
   */



  /**
   * 输出结果：
   * SensorReading(sensor_1,1000,1.0)
SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,3000,1.0)
reduce==> SensorReading(sensor_1,3000,3.0)
SensorReading(sensor_1,4000,1.0)
SensorReading(sensor_1,5000,1.0)
SensorReading(sensor_1,6000,1.0)
reduce==> SensorReading(sensor_1,6000,5.0)
SensorReading(sensor_1,7000,1.0)
SensorReading(sensor_1,8000,1.0)
SensorReading(sensor_1,9000,2.0)
reduce==> SensorReading(sensor_1,9000,6.0)



   */

}
