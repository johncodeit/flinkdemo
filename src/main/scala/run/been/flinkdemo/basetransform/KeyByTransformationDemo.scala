package run.been.flinkdemo.basetransform

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import run.been.flinkdemo.util.{SensorReading, SensorSource}

object KeyByTransformationDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)

    val sensorIds: KeyedStream[SensorReading,String] = readings.keyBy(_.id)
    sensorIds.print()
    env.execute("Base transformation map ")
  }
}
