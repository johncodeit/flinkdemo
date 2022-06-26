package run.been.flinkdemo.basetransform

import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import run.been.flinkdemo.util.SensorReading


object ReduceTransformationDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val path = "D:\\flinksrc\\flinkdemo\\data\\sensor.txt"
    val readings = env.readTextFile(path).map({data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    }})

    val sensorIds: KeyedStream[SensorReading,String] = readings.keyBy(_.id)

    val reduceData = sensorIds.reduce((x,y) => SensorReading(x.id,x.timestamp+3,y.temperature+1))
    reduceData.print()

    env.execute("Base transformation map ")
  }

}
