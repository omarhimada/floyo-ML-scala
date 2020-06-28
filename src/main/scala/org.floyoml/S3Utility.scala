package org.floyoml

import org.apache.spark.sql.SparkSession


object S3Utility {
  /**
   * get or create a spark session for downloading data from S3
   */
  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName(Shared.sparkAppName)
    .getOrCreate()

  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", sys.env(Shared.environmentVariableAWSAccessKey))
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sys.env(Shared.environmentVariableAWSSecretKey))
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", Shared.awsS3Endpoint)

  // todo
}
