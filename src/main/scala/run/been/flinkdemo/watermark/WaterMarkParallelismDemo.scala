package run.been.flinkdemo.watermark

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

/**
 *
 *
 */
object WaterMarkParallelismDemo {

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
      }.assignTimestampsAndWatermarks(strategy).setParallelism(2)
    //这里设置为2个并行度
    println("inputStream parallelism is " + inputStream.parallelism)

    inputStream.print()
    //为了查看watermark的生成，下面代码获取对应的时间
    val sink = inputStream.process(new ProcessFunction[SensorReading,SensorReading] {
      override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        //数据时间戳
        println("本次时间收到的时间："+value.timestamp)
        println("本次数据"+ value)
        //获取处理时间
        val processTime = ctx.timerService().currentProcessingTime()
        println("此刻处理时间 = " + processTime)
        //获取水印
        val watermarkTime = ctx.timerService().currentWatermark()
        println("此刻水印时间 = " + watermarkTime)
      }
      //这里有1个并行度
    }).setParallelism(1)
      println("sink parallelism is " + sink.parallelism)
      sink.print()

  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    println("Env Parallelism is "+env.getParallelism)
    test02(env)
    env.execute("window job")
  }
}


/**
 * 输入数据
 * sensor_1,2000,3.0
sensor_2,1000,2.0
sensor_2,5000,2.0
sensor_2,6000,2.0
sensor_2,16000,2.0
sensor_1,26000,2.0
sensor_1,36000,2.0
 */

/**
 *
 * Env Parallelism is 1
inputStream parallelism is 2
sink parallelism is 1

本次时间收到的时间：2000
本次数据SensorReading(sensor_1,2000,3.0)
SensorReading(sensor_1,2000,3.0)
此刻处理时间 = 1658923852116
此刻水印时间 = -9223372036854775808

本次时间收到的时间：1000
SensorReading(sensor_2,1000,2.0)
本次数据SensorReading(sensor_2,1000,2.0)
此刻处理时间 = 1658924211426
此刻水印时间 = -9223372036854775808
通过这里发现初始水印会有个负的时间，并行度是2的时候，会有2条记录进行初始化
并行度是2所以获取的2边中最小值，所以还是负数

本次时间收到的时间：5000
SensorReading(sensor_2,5000,2.0)
本次数据SensorReading(sensor_2,5000,2.0)
此刻处理时间 = 1658924226955
此刻水印时间 = 999
第三条记录输入会有水印为1000-1=999
1000和2000中最小值所以取1000


本次时间收到的时间：6000
SensorReading(sensor_2,6000,2.0)
本次数据SensorReading(sensor_2,6000,2.0)
此刻处理时间 = 1658924254119
此刻水印时间 = 999
第4条记录水印是999
1000和5000中最小的 1000



本次时间收到的时间：16000
SensorReading(sensor_2,16000,2.0)
本次数据SensorReading(sensor_2,16000,2.0)
此刻处理时间 = 1658924267754
此刻水印时间 = 4999
第5条记录水印是5000-1
前一次的最小值是5000和6000中最小的5000

本次时间收到的时间：26000
SensorReading(sensor_1,26000,2.0)
本次数据SensorReading(sensor_1,26000,2.0)
此刻处理时间 = 1658924292727
此刻水印时间 = 5999
第6条记录6000-1=5999

本次时间收到的时间：36000
SensorReading(sensor_1,36000,2.0)
本次数据SensorReading(sensor_1,36000,2.0)
此刻处理时间 = 1658924328410
此刻水印时间 = 15999
第6条记录6000-1=15999
 */

