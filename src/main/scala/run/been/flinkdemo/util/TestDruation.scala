package run.been.flinkdemo.util

import java.time.Duration

object TestDruation {
  def main(args: Array[String]): Unit = {
    println(Duration.ofSeconds(5))
    println(Duration.ofMillis(5))

  }

}
