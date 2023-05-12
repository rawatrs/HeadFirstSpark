package com.reena.explore.data

object SuperMarketData {
  case class Customer(customerid: Int, customername: String, state: String, city: String)

  case class FactTable(sales: Double,
                       profit: Double,
                       SRNO: Int,
                       transactionid: String,
                       customerid: Int,
                       productid: String)

  case class Product(productid: String,
                     category: String,
                     subcategory: String,
                     productname: String,
                     quantity: String)

  case class Transaction(transactionid: String, payment: String)

}
