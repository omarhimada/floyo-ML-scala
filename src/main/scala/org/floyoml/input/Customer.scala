package org.floyoml.input

/**
 * Transformed from a series of Transactions grouped by customer ID
 */
case class Customer(customerId: Double, recency: Double, frequency: Double, monetary: Double)
