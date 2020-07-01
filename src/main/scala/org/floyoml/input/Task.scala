package org.floyoml.input

/**
 * Some task or function that the system will execute
 */
trait Task

/**
 * Some task that automates a piece of functionality
 */
trait Automation extends Task

/**
 * Automation task for sending emails to customers/users
 */
case class EmailAutomation(provider: String) extends Automation

/**
 * Some task or function related to ML that the system will execute
 */
trait MLTask extends Task

/**
 * ML task for customer segmentation via K-Means++
 */
case class Segmentation(isTraining: Boolean) extends MLTask

/**
 * ML task for recommendations via collaborative filtering (matrix factorization)
 */
case class Recommendations(isTraining: Boolean) extends MLTask

/**
 * ML task for predicting churn via logistic regression
 */
case class ChurnPrediction(isTraining: Boolean) extends MLTask
