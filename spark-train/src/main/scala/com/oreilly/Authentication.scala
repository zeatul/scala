package com.oreilly

class Authentication {
  
  private[this] val password = "jason"
  def validate = password.size>0
  
}