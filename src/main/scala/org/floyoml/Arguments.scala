package org.floyoml

import com.beust.jcommander.Parameter

/**
 * Arguments passed to Seed, defining the expected behaviour of the process
 */
object Arguments {
  @Parameter(names = Array("-po", "--outputPredictions"))
  var outputPredictions: Option[String] = None

  /**
   * Whether or not to attempt training
   */
  @Parameter(names = Array("-t", "--train"))
  var train: Boolean = false

  /**
   * A new KMeans model will be persisted here, or an existing model will be loaded from here
   */
  @Parameter(names = Array("-km-ml", "--kMeansModelLocation"))
  var kMeansModelLocation: Option[String] = None

  /**
   * A new Matrix Factorization model will be persisted here, or an existing model will be loaded from here
   */
  @Parameter(names = Array("-mf-ml", "--matrixFactorizationModelLocation"))
  var matrixFactorizationModelLocation: Option[String] = None
}