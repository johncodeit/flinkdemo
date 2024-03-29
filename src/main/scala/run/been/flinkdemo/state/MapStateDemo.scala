package run.been.flinkdemo.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

object MapStateDemo {
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /**
     * 定义水印生成策略
     */
    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(5)) //延迟0秒
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
    val alerts = keyedSensorData.flatMap(new MapStateFunction())
    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }

}

class MapStateFunction extends RichFlatMapFunction[SensorReading,String]{
  //map:key表示传感器ID，value表示次数
  private var mapState:MapState[String,Long] = _


  override def open(parameters: Configuration): Unit = {
    mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("my-map",classOf[String],classOf[Long]))
  }

  override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
    println(value.id+ "值状态： " + mapState.values())
    val count = if(mapState.contains(value.id)) mapState.get(value.id) else 0
    mapState.put(value.id,count +1)
    println(value.id +"当前传感器值 ： " +mapState.get(value.id) )

  }
}

/**
 * 输入值
 * sensor_1,2000,1.0
sensor_1,3000,2.0
sensor_2,3000,2.0
sensor_2,4000,2.0
 */
/**
 * 输出结果
 * sensor_1值状态： []
sensor_1当前传感器值 ： 1
sensor_1值状态： [1]
sensor_1当前传感器值 ： 2
sensor_2值状态： []
sensor_2当前传感器值 ： 1
sensor_2值状态： [1]
sensor_2当前传感器值 ： 2
 */