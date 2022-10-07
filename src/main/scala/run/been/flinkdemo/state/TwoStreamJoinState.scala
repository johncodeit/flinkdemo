package run.been.flinkdemo.state

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 2条流进行join,2个流中的所有数据进行join
 */
object TwoStreamJoinState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //创建2条流
    val stream1 = env.fromElements(("a","stream-1",1000L),("b","stream-1",2000L)).assignAscendingTimestamps(_._3)

    val stream2 = env.fromElements(("a","stream-2",3000L),("b","stream-2",4000L)).assignAscendingTimestamps(_._3)

    //2条流进行join操作
    stream1.keyBy(_._1)
      .connect(stream2.keyBy(_._1))
      .process(new TwoStreamJoin)
      .print()
    env.execute()
  }


}

class TwoStreamJoin extends CoProcessFunction[(String,String,Long),(String,String,Long),String]{
  //定义列表状态，保存已经到达的数据
  lazy val streamOneListState:ListState[(String,String,Long)] = getRuntimeContext.getListState(new ListStateDescriptor[(String, String, Long)]("streamOne-list",classOf[(String,String,Long)]))

  lazy val streamTwoListState:ListState[(String,String,Long)]  = getRuntimeContext.getListState(new ListStateDescriptor[(String, String, Long)]("streamTwo",classOf[(String,String,Long)]))

  override def processElement1(value: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
    //添加到列表中
    streamOneListState.add(value)
    //streamTwoListState.get()返回的是java对象，这里要用scala,所以需要一个隐式转换
    import scala.collection.convert.ImplicitConversions._
    for(value2 <- streamTwoListState.get()){
      out.collect(value + " ==> " + value2)
    }
  }

  override def processElement2(value: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
    //添加到列表中
    streamTwoListState.add(value)
    //streamTwoListState.get()返回的是java对象，这里要用scala,所以需要一个隐式转换
    import scala.collection.convert.ImplicitConversions._
    for(value2 <- streamOneListState.get()){
      out.collect(value + " ==> " + value2)
    }
  }
}
