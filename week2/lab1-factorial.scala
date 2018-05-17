#!/usr/bin/env scala

//sum of all factorials between 1 and 10

def factorial (n: Int): Int = {
  {
    val factResult = List.range(1, n+1).reduceLeft(_*_)
    return factResult
  }
}

//List.range(1,5).map(factorial).reduceLeft(_+_)
println(List.range(1,6).map(factorial).reduceLeft(_+_))
