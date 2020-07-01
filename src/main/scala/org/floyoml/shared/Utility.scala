package org.floyoml.shared

import java.io.File

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object Utility {
  private val _dimensions: Int = 1000
  private val tf = new HashingTF(_dimensions)

  /**
   * Transform (min-hash) string to Vector[Double] for KMeans
   */
  def featurize(s: String): Vector = tf.transform(s.sliding(2).toSeq) // (s.toSeq.sliding(2).map(_.unwrap).toSeq)

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
}