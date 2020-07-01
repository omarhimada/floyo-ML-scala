package org.floyoml.shared

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Context {
  val sparkConfig: SparkConf = new SparkConf().setAppName(Configuration.sparkAppName)

  val sparkContext = new SparkContext(sparkConfig)

  val sparkSession: SparkSession =
    SparkSession.builder()
      .master(Configuration.Behaviour.sparkMaster)
      .appName(Configuration.sparkAppName)
      .getOrCreate()
}
