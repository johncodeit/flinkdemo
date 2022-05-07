package run.been.flinkdemo.hello

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/overview/
 */
object WindowWordCount {
  def main(args: Array[String]) {
    //1.获取一个执行环境，上下文（execution environment）；
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.source:加载/创建初始数据；
    val text = env.socketTextStream("localhost", 9999)

    //3.transform 指定数据相关的转换；
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    //4.sink:指定计算结果的存储位置；
    counts.print()

    //5.触发程序执行。
    env.execute("Window Stream WordCount")
  }
}