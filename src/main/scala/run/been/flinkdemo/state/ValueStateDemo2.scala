package run.been.flinkdemo.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

/**
 * 定时每10秒统计一次收到的不同传感器，记录的次数，
 */
object ValueStateDemo2 {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /**
     * 定义水印生成策略
     */
    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(0)) //延迟0秒
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

    //返回类型为String
    val alerts: DataStream[String] = keyedSensorData
      .process(new MyValueStateProcessFunction())


    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }

}

/**
 * KeyedProcessFunction[String,SensorReading, String]
 * 参数：keyBy的类型：String
 */
class MyValueStateProcessFunction extends KeyedProcessFunction[String,SensorReading, String] {
  //保存每个传感器的总数,这里使用懒加载，没有使用open方法进行初始化，在使用的时候会进行初始化
  lazy val valueCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))

  //定义值状态，保存定时器的时间
  lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor("timer-ts",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //每次过来一条记录，对记录进行处理
    //获取当前传感器的值
    val count = valueCountState.value()
    //更新值，对原来的值+1
    valueCountState.update(count + 1)
    //注册定时器，每过10秒执行一次
    if(timeState.value() == 0L){
      ctx.timerService().registerEventTimeTimer(value.timestamp + 10*1000L)
      //更新状态
      timeState.update(value.timestamp + 10 * 1000L)
    }
  }
  //定时器触发，输出当前的统计结果

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(s"传感器ID" + ctx.getCurrentKey + " 总数" + valueCountState.value())
    //输出一次后，重新计时，所以时间状态要重新计时
    timeState.clear()
  }
}

/**
 * 输入
 * sensor_1,2000,1.0
sensor_1,8000,1.0
sensor_1,12000,1.0
sensor_1,13000,1.0
sensor_1,14000,1.0
sensor_2,4000,1.0
sensor_2,14000,1.0
sensor_2,15000,1.0
sensor_1,15000,1.0
sensor_1,25000,1.0
 */

/**
 * 输出结果
 * 传感器IDsensor_1 总数4
传感器IDsensor_2 总数3
传感器IDsensor_1 总数7
 */
