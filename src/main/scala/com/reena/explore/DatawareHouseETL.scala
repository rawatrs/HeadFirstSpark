package com.reena.explore

class DatawareHouseETL {

  val a1 = Automobile.build()
  val a2 = Automobile.build()

  val a3 = Automobile.combiner(a1, a2)
}
