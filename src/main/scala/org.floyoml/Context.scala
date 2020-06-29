package org.floyoml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Context {
  val sparkConfig: SparkConf = new SparkConf().setAppName(Shared.Configuration.sparkAppName)

  val sparkContext = new SparkContext(sparkConfig)

  val sparkSession: SparkSession =
    SparkSession.builder()
      .master("local[4]")
      .appName(Shared.Configuration.sparkAppName)
      .getOrCreate()
}
