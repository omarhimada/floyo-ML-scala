package org.floyoml

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

object Shared {
  private val _dimensions: Int = 1000
  private val tf = new HashingTF(_dimensions)

  /**
   * transform (min-hash) string to Vector[Double] for KMeans
   */
  def featurize(s: String): Vector = tf.transform(s.sliding(2).toSeq) // (s.toSeq.sliding(2).map(_.unwrap).toSeq)
}
