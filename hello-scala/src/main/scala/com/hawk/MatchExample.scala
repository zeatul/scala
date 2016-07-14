package com.hawk

object MatchExample {
  
  def main(args : Array[String]):Unit={
    val message = "Ok"
    val status = message match{
      case "Ok" => 200
      case other =>{
        println(s"Couldn't parse $other")
        "".length()
        -1
      }
    }
  }
}