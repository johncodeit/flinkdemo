package run.been.flinkdemo.datatype

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

case class Person(name:String,age:Int)

/**
 * Flink中的数据类型
 */
object DataTypeDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1.基础类型
    val numberType = env.fromElements(1L,3L,5L,8L)
    numberType.map(x => x+10).print()

    //2.元组
    val tupleType = env.fromElements(("ZhangSan",28),("Lisi",21))
    tupleType.filter(x => x._2>25).print()

    //3.样例类
    val personType = env.fromElements(Person("Wangwu",38),Person("Zhaoliu",41))
    personType.filter(x => x.age>40).print()

    //4.Java简单对象
    val pojoType = env.fromElements(new Person("Wangwu",38),new Person("Zhaoliu",41))

    pojoType.filter(x =>x.age<40).print()
    env.execute("DataType Demo")

  }

}
