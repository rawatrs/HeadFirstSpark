package com.reena.explore

class Automobile() {
    def move() = {}
}

object Automobile {
    val MAX_WEIGHT = 1000
    def build(): Automobile = new Automobile
    def combiner(a: Automobile, b: Automobile): Automobile = a
}
