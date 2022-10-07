package run.been.flinkdemo.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.net.URI

object RocksDbTtlStateDemo {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //开启状态检查checkpoint,参数：快照的周期，快照的模式
    env.enableCheckpointing(1000)
    //存储
    env.getCheckpointConfig.setCheckpointStorage(new URI("file:///d:/checkpointdata/"))

     // ingest sensor stream
    val sensorData = env.socketTextStream("localhost", 9999)
//设置使用状态后端
    val backend = new HashMapStateBackend()
    env.setStateBackend(backend)

    //Rocksdb
    val embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend()
    env.setStateBackend(embeddedRocksDBStateBackend)


    val keyedSensorData = sensorData.keyBy(x=>0)

    val alerts = keyedSensorData
      .map(new MyOptionFunctionRocksdb())


    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }

}

class MyOptionFunctionRocksdb extends RichMapFunction[String, String] {
  //定义值状态
  var listState: ListState[String] = _

  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor = new ListStateDescriptor[String]("my-value", classOf[String])



    //一个异步线程在定期处理，数据a已经到了销毁时间，但是还没有进行销毁，这时去查询，就可能查询到应该过期的数据，
    // 这个时候就可以设置过期状态的可见性，NeverReturnExpired不返回过期数据，ReturnExpiredIfNotCleanedUp返回过期的数据
    //如何实现的呢？
    //在调用数据的时候，会有个过滤器，这样过期的数据就不能获取到。
    //listState都有自己的独立计时，其它类似，也就是都是针对每条记录的
    val config = StateTtlConfig.newBuilder(Time.milliseconds(5000)) //配置存活时长5s
      .setTtl(Time.milliseconds(4000)) //配置数据的存活时长4秒
      //      .updateTtlOnReadAndWrite()//当读取和写入的时候，更新ttl从0开始
      //      .updateTtlOnCreateAndWrite()//刷新ttl,当插入和更新的时候
//      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //设置状态的可见性,永远不可见过期的数据
      .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) //返回过期数据
      .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) //默认是处理时间，时间语义
      .cleanupIncrementally(1000,true)
      .build()
    listStateDescriptor.enableTimeToLive(config)

    listState = getRuntimeContext.getListState(listStateDescriptor)
  }

  override def map(value: String): String = {
    listState.add(value)
    val strings = listState.get()
    var sb = new StringBuilder
    strings.forEach(x => sb.append(x))
    sb.toString()
  }

}

/**
 * a
ab
abc
abcd
abcdf
abcdfa
abcdfa1
abcdfa12
abcdfa123
1234
12345
123456a
123456aa
123456aaf
123456aafc
123456aafcd
123456aafcdf
123456aafcdfs
123456aafcdfsf
123456aafcdfsfs
123456aafcdfsfsd
123456aafcdfsfsdf
cdfsfsdfs
cdfsfsdfsf
cdfsfsdfsfs
cdfsfsdfsfsa
cdfsfsdfsfsad
cdfsfsdfsfsadf
cdfsfsdfsfsadff
cdfsfsdfsfsadfff
cdfsfsdfsfsadfffs
cdfsfsdfsfsadfffsf
cdfsfsdfsfsadfffsfsf
cdfsfsdfsfsadfffsfsfa
cdfsfsdfsfsadfffsfsfaa

Process finished with exit code -1

 */