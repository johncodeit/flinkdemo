package run.been.flinkdemo.watermark

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

/**
 *
 *
 */
object WaterMarkDemoUI {

  def test02(env: StreamExecutionEnvironment) = {
    /**
     * 定义水印生成策略
     * 这里实现的水印生成策略的时候，实现接口SerializableTimestampAssigner，目的是从输入的数据中抽取时间字段，
     * 时间类型要是Long型的
     * 其实就是一个接口的实现
     */
    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(0)) //延迟0秒
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp //指定事件时间字段
      })


    val inputStream = env.socketTextStream("localhost", 9999)
      .map { text =>
        val arr: Array[String] = text.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }.assignTimestampsAndWatermarks(strategy)

    inputStream.print()
    //为了查看watermark的生成，下面代码获取对应的时间
    inputStream.process(new ProcessFunction[SensorReading,SensorReading] {
      override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        //数据时间戳
//        println("本次时间收到的时间："+value.timestamp)
//        println("本次数据"+ value)
        //获取处理时间
        val processTime = ctx.timerService().currentProcessingTime()
//        println("此刻处理时间 = " + processTime)
        //获取水印
        val watermarkTime = ctx.timerService().currentWatermark()
//        println("此刻水印时间 = " + watermarkTime)
      }
    }).print()

  }

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    println(env.getParallelism)
    test02(env)
    env.execute("window job")
  }
}


/**
 * 输入数据
 * sensor_1,2000,1.0
sensor_1,3000,2.0
sensor_1,4000,5.0
sensor_1,2000,3.0
sensor_1,5000,4.0
 */

/**
 *
 * SensorReading(sensor_1,2000,1.0)
本次时间收到的时间：2000
本次数据SensorReading(sensor_1,2000,1.0)
此刻处理时间 = 1658848170282
此刻水印时间 = -9223372036854775808

再次重新执行，第一次水印时间：此刻水印时间 = -9223372036854775808，所以应该是一个固定时间
SensorReading(sensor_1,3000,2.0)
本次时间收到的时间：3000
本次数据SensorReading(sensor_1,3000,2.0)
此刻处理时间 = 1658848179445
此刻水印时间 = 1999
SensorReading(sensor_1,4000,5.0)
本次时间收到的时间：4000
本次数据SensorReading(sensor_1,4000,5.0)
此刻处理时间 = 1658848250156
此刻水印时间 = 2999
SensorReading(sensor_1,2000,3.0)
本次时间收到的时间：2000
本次数据SensorReading(sensor_1,2000,3.0)
此刻处理时间 = 1658848502883
此刻水印时间 = 3999
SensorReading(sensor_1,5000,4.0)
本次时间收到的时间：5000
本次数据SensorReading(sensor_1,5000,4.0)
此刻处理时间 = 1658848521186
此刻水印时间 = 3999
 */

