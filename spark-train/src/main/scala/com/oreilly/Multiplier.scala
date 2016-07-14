package com.oreilly

class Multiplier(val x:Int) {
  def product(y:Int) = x * y
}

object Multiplier {def apply(x:Int) = new Multiplier(x)}