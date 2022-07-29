package run.been.flinkdemo.state

import org.apache.flink.api.common.accumulators.AverageAccumulator
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import run.been.flinkdemo.util.SensorReading

import java.time.Duration
import scala.collection.mutable.ArrayBuffer

/**
 *
 */
object AggregatingStateDemo {
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

    val alerts = keyedSensorData.process(new AggregatingStateDemo())

    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }
}

/**
 * 需求：求平均值，输入数据类型SensorReading，Tuple2第一个参数：温度总和，第二个参数数据个数
 */
class AvgTemp extends AggregateFunction[SensorReading,AverageAccumulator,Double]{
  override def createAccumulator(): AverageAccumulator = {
    new AverageAccumulator()
  }

  override def add(value: SensorReading, accumulator: AverageAccumulator): AverageAccumulator = {
    println(value)
    accumulator.add(value.temperature)
    accumulator
  }

  override def getResult(accumulator: AverageAccumulator): Double = {
    accumulator.getLocalValue
  }

  override def merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator = {
    a.merge(b)
    a
  }
}

class AggregatingStateDemo extends KeyedProcessFunction[String,SensorReading, Double]{

  private var aggregatingState: AggregatingState[SensorReading,Double] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val aggregatingStateDescriptor = new AggregatingStateDescriptor[SensorReading,AverageAccumulator,Double]("avg temp",new AvgTemp(),Types.of[AverageAccumulator])
    aggregatingState = getRuntimeContext.getAggregatingState(aggregatingStateDescriptor)
  }
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading,  Double]#Context, out: Collector[ Double]): Unit = {
    aggregatingState.add(value)
    out.collect(aggregatingState.get())
  }
}

/**
 * 输入数据，一条一条输入
 * sensor_1,3000,1.0
sensor_1,3000,2.0
sensor_1,3000,3.0
sensor_1,3000,6.0
 */

/**
 * SensorReading(sensor_1,3000,1.0)
1.0
SensorReading(sensor_1,3000,2.0)
1.5
SensorReading(sensor_1,3000,3.0)
2.0
SensorReading(sensor_1,3000,6.0)
3.0
 */