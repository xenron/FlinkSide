package com.example.scala.wordcount

import org.apache.flink.api.java.ExecutionEnvironment

object WordCount {
  def main(args: Array[String]) {

    //val env = ExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.createLocalEnvironment()
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")


//    val counts = text.flatMap {
//      _.toLowerCase.split("\\W+") filter {
//        _.nonEmpty
//      }
//    }
//      .map {
//        (_, 1)
//      }
//      .groupBy(0)
//      .sum(1)
//
//    counts.print()
  }
}
