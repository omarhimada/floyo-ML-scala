package org.floyoml.shared

import java.io.File

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object Utility {
  /**
   * Delete the previously persisted ML model from the provided path
   * @param path the path of the ML model to delete
   */
  def deletePreviousModel(path: String): Unit = {
    def getRecursively(f: File): Seq[File] =
      f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles

    getRecursively(new File(path)).foreach{f =>
      if (!f.delete())
        throw new RuntimeException("Failed to delete " + f.getAbsolutePath)
    }

    new File(path).delete()
  }

  /**
   * Download multiple related datasets from S3 and union them
   * @param objectPaths remote paths to related datasets in S3
   */
  def unionManyDatasets(objectPaths: ListBuffer[String]): RDD[String] = {
    val manyDatasets = ListBuffer.empty[RDD[String]]

    // for each relevant object in S3...
    for (objectPath <- objectPaths) {
      // resilient distributed dataset (RDD) for training
      val rdd = Context.sparkContext.textFile(objectPath)

      // append the RDD to our collection of RDDs
      manyDatasets.append(rdd)
    }

    Context.sparkContext.union(manyDatasets)
  }

  /**
   * Compute the top-end percentile
   * @param data: input data set of integers
   * @return value of input data at the 95th percentile
   */
  def computeTopPercentile(data: RDD[Double]): Double = computePercentile(data, 95)

  /**
   * Compute the top-end percentile
   * @param data: input data set of integers
   * @return value of input data at the 5th percentile
   */
  def computeBottomPercentile(data: RDD[Double]): Double = computePercentile(data, 5)

  /**
   * Compute percentile from an unsorted RDD
   * @param data: input data set of integers
   * @param percentile: percentile to compute (eg. 95th percentile)
   * @return value of input data at the specified percentile
   */
  def computePercentile(data: RDD[Double], percentile: Double): Double = {
    // NIST method; data to be sorted in ascending order
    val r = data.sortBy(x => x)
    val c = r.count()
    if (c == 1) r.first()
    else {
      val n = (percentile / 100d) * (c + 1d)
      val k = math.floor(n).toLong
      val d = n - k
      if (k <= 0) r.first()
      else {
        val index = r.zipWithIndex().map(_.swap)
        val last = c
        if (k >= c) {
          index.lookup(last - 1).head
        } else {
          index.lookup(k - 1).head + d * (index.lookup(k).head - index.lookup(k - 1).head)
        }
      }
    }
  }
}