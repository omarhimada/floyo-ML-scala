package org.floyoml.input

import java.sql.Date

/**
 * Input transaction (e.g.: eCommerce sale)
 * // todo
 */
case class Transaction(
  customerId: Int,
  sku: Int,
  quantity: Int,
  date: Date,
  unitPrice: Double)