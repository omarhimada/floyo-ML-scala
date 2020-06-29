package org.floyoml

/**
 * some task or function that the system will execute
 */
trait Task

/**
 * some task that automates a piece of functionality
 */
trait Automation extends Task

/**
 * automation task for sending emails to customers/users
 */
case class EmailAutomation(provider: String) extends Automation

/**
 * some task or function related to ML that the system will execute
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
