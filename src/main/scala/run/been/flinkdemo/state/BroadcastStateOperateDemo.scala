package run.been.flinkdemo.state

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.net.URI

//样例类
case class Action(userID:String,action : String)
case class Pattern(action1:String,action2:String)

object BroadcastStateOperateDemo {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义数据流，用户行为数据
    val actionStream = env.fromElements(
      Action("Alice","login"),
      Action("Alice","pay"),
      Action("Bob","login"),
      Action("Bob","buy")
    )

    //定义规则流，读取指定的行为模式
    val patternStream = env.fromElements(
      Pattern("login","pay")
    )
    //定义广播状态描述
    //Unit表示空
    val patterns = new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern])
    val broadcastStream = patternStream.broadcast(patterns)

    //连接两条流进行处理
    val value = actionStream.keyBy(_.userID) //按照userID 进行分组
      .connect(broadcastStream)
      .process(new MyPatternProcess)

    value.print()
    // execute application
    env.execute("Generate Temperature Alerts")
  }

}

/**
 * keyby中key的类型：String,
 * 第一个输入类型：Action,
 * 第二个输入类型：Pattern,
 * 输出结果：String
 */
class MyPatternProcess extends KeyedBroadcastProcessFunction[String,Action,Pattern,(String,Pattern)]{
//  定义值状态，保存上一次用户值状态
  lazy val prevActionState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("prev-action",classOf[String]))

  override def processElement(value: Action, ctx: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#ReadOnlyContext, out: Collector[(String, Pattern)]): Unit = {
    //从广播状态中获取行为数据
    val pattern = ctx.getBroadcastState(new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern]))
      .get(Unit)
    //从值状态中获取上次行为
    val prevAction = prevActionState.value()
    if(pattern != null && prevAction != null){
      //前一次行文与行为的第一次行为一致，当前值的行为与规则的第二个行为一致
      if(pattern.action1.equals(prevAction)  && pattern.action2.equals(value.action)){
        out.collect((ctx.getCurrentKey,pattern))
      }
    }
    //保存状态
    prevActionState.update(value.action)
  }

  override def processBroadcastElement(value: Pattern, ctx: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#Context, out: Collector[(String, Pattern)]): Unit = {
    //修改当前规则
    val bcState = ctx.getBroadcastState(new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern]))
    bcState.put(Unit,value)
  }
}

/**
 * 输出结果
 * (Alice,Pattern(login,pay))
 */