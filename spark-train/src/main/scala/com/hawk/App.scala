package com.hawk



/**
 * @author ${user.name}
 */
object App {
  
  import com.hawk.Hello
  
  def main(args : Array[String]) {
     def to(head:Char,end:Char):Stream[Char] = (head > end) match{
       case true => Stream.empty
       case fasle => head #:: to((head+1).toChar,end)
     }
     
     val s = to('A','F').take(20).toList
     
     println(s)
     
     Hello.println();
  }

}
