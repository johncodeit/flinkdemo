package run.been.flinkdemo.udf

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import run.been.flinkdemo.util.{SensorReading, SensorSource}

/**
 * 富函数调用
 */
object UdfDemo5 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
    val mapData = readings.map(new MyRichMapper)
    mapData.print()

    env.execute()
  }
}

