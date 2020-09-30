package com.bourgeoisie.demo

import org.apache.spark.{SparkConf, SparkContext}

object Hello {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
//      .setMaster("local[*]")
      .setAppName("WordCount")
    // 2. 创建SparkContext对象
    val sc = new SparkContext(conf)
    val res = sc.textFile(args(0))
      .flatMap(_.split("\t"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
    res.foreach(println(_))

    // 4. 关闭连接
    sc.stop()
  }

}
