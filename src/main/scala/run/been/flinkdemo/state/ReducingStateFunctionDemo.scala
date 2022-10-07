package run.been.flinkdemo.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration

/**
 * 需求：获取温度最大值的记录
 */
object ReducingStateFunctionDemo {
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(2* 1000)

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

    val alerts: DataStream[SensorReading] = keyedSensorData.process(new ReducingStateDemo)

    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }

}
class ReducingStateDemo extends KeyedProcessFunction[String, SensorReading, SensorReading]{
  private var reducingState: ReducingState[SensorReading] = _
  override def open(parameters: Configuration): Unit = {
    //定义描述器
    val reducingStateDescriptor = new ReducingStateDescriptor("max temp state",new MaxTemp(),classOf[SensorReading])

    //获取ReducingState
    reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor)
  }

  override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    reducingState.add(value)
    out.collect(reducingState.get())
  }
}

/**
 * 自定义
 */
class MaxTemp extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    if(value1.temperature > value2.temperature) value1 else value2
  }
}

/**
 * 输入数据
 * sensor_1,2000,1.0
sensor_1,3000,2.0
sensor_1,4000,5.0
sensor_1,4000,3.0
sensor_1,5000,2.0
 */

/**
 * SensorReading(sensor_1,2000,1.0)
SensorReading(sensor_1,3000,2.0)
SensorReading(sensor_1,4000,5.0)
SensorReading(sensor_1,4000,5.0)
SensorReading(sensor_1,4000,5.0)
 */