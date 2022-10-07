package run.been.flinkdemo.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

/**
 * 2条流进行join,2个流中的所有数据进行join
 */
object FakeWindowDemo {
  def main(args: Array[String]): Unit = {
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
      .process(new FakeWindow(10000L))//模拟10秒的滚动窗口


    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }


}

class FakeWindow(size : Long) extends KeyedProcessFunction[String,SensorReading,String]{
  //定义一个映射状态，用来保存window的
  //MapStateDescriptor[Long,Long]第一个参数是开始时间戳（或者结束时间戳），第二个参数是总数
  lazy val windowMapState = getRuntimeContext.getMapState(new MapStateDescriptor[Long,Long]("windowmap",classOf[Long],classOf[Long]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //需要窗口分配器，知道每条交流进入了那个窗口
    //窗口的开始时间戳
    val start = value.timestamp /size * size
    val end = start + size;
    //注册定时器，用来触发窗口
    ctx.timerService().registerEventTimeTimer(end - 1)

    //数据到来后，更新状态
    if(windowMapState.contains(start)){
      val pv = windowMapState.get(start)
      windowMapState.put(start,pv + 1)
    }else{
      windowMapState.put(start,1)
    }
}

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时器触发
    val start = timestamp + 1 -size
    val pv = windowMapState.get(start)
    out.collect(s"传感器ID " + ctx.getCurrentKey+ s"窗口为  ${pv}   " + start +s"~ ${start + size}")

    //窗口销毁，将窗口信息删除
    windowMapState.remove(start)
  }
}

/**
 * 输入sensor_1,2000,1.0
sensor_1,5000,4.0
sensor_2,15000,4.0
sensor_2,25000,4.0
 *
 * 输出：
 * 传感器ID sensor_1窗口为  2   0~ 10000
传感器ID sensor_2窗口为  1   10000~ 20000
 */
