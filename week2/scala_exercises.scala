#!/usr/bin/env scala

//exercise 1 create a point co-ordinate
class point (val x:Int, val y:Int) {
     def move (dx: Int, dy: Int) = new point(dx+x, dy+y)
     def print = println("(" + x + "," + y + ")")
     }

val pointB = new point(1,1)

pointB.move(2,3).print

pointB.move(1,1).print


