package run.been.flinkdemo.state

import org.apache.commons.lang3.RandomUtils
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.net.URI

object OperatorStateDemo2 {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // ingest sensor stream
    val sensorData = env.socketTextStream("localhost", 9999)

    val keyedSensorData = sensorData

    val alerts: DataStream[String] = keyedSensorData
      .map( new OperatorStateMapFunction2())

    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }

}

class OperatorStateMapFunction2 extends MapFunction[String,String] with CheckpointedFunction{
  var listState:ListState[String] = _

  //map的处理逻辑
  override def map(value: String): String = {
     //将本条数据插入到list中
    listState.add(value)
    val strings = listState.get()

    val sb:StringBuilder = new StringBuilder
    strings.forEach((x:String) => sb.append(x))
    sb.toString()
  }
  //系统做快照的时候，调用的方法，用户可以在持久化前，对状态数据进行处理,一般不需要处理
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    //自动进行快照
//    println("快照存储触发，第几次checkpoint" + context.getCheckpointId)
  }

  //算子在启动的开始的时候，会调用，进行初始化的时候调用，获取状态管理器
  override def initializeState(context: FunctionInitializationContext): Unit = {
    //算子状态的存储器
    val operatorStateStore = context.getOperatorStateStore()
    //当用户重启的时候，会进行加载持久化中的数据
    listState = operatorStateStore.getListState(new ListStateDescriptor[String]("strings", classOf[String]))
  }
}

/**
 * 输入
 * a
b
c
d
 */

/**
 * 输出
 * a
ab
abc
abcd
 */