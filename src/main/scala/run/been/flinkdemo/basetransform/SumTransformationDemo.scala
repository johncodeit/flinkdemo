package run.been.flinkdemo.basetransform

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import run.been.flinkdemo.util.{SensorReading, SensorSource}


object SumTransformationDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val path = "D:\\flinksrc\\flinkdemo\\data\\sensor.txt"
    val readings = env.readTextFile(path).map({data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    }})

    val sensorIds: KeyedStream[SensorReading,String] = readings.keyBy(_.id)
    //min
//    val minData = sensorIds.min("temperature")
//    minData.print()

//
//    //max
    val maxData = sensorIds.max("temperature")
    maxData.print()

    /**
     *10> SensorReading(sensor_1,1655676604643,89.4)
      4> SensorReading(sensor_2,1655676604643,70.7)
      10> SensorReading(sensor_1,1655676604643,89.4)
      4> SensorReading(sensor_2,1655676604643,100.1)
      1> SensorReading(sensor_3,1655676604643,55.6)
     */

//    //sum
//    val sumData = sensorIds.sum("temperature")
//    sumData.print()

//    //minBy
//    val minByData = sensorIds.minBy("temperature")
//    minByData.print()

    //maxBy
//    val maxByData = sensorIds.maxBy("temperature")
//    maxByData.print()

    /**
     * 1> SensorReading(sensor_3,1655676604643,55.6)
      4> SensorReading(sensor_2,1655676604643,70.7)
      4> SensorReading(sensor_2,1655676604644,100.1)
      10> SensorReading(sensor_1,1655676604644,32.2)
      10> SensorReading(sensor_1,1655676604643,89.4)
     */
    env.execute("Base transformation map ")
  }

}
