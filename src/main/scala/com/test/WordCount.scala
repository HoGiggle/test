package com.test



/**
 * Created by admin on 2016/4/20.
 */
abstract class Person
case class Student(name:String,sno:Int) extends Person
case class Teacher(name:String,tno:Int) extends Person
case class None(name:String) extends Person

class Company(name:String)

object WordCount extends App {
    def caseClassMatch(p:Person) = p match{
        case Student(name,sno) => println(name + " is a student,sno is:" + sno)
        case Teacher(name,tno) => println(name + " is a teacher,tno is:" + tno)
        case None(name) => println("None matched")
        case _ => println("Abstract class")
    }

    def caseClassMatch(p:Object) = p match{
        case Student(name,sno) => println(name + " is a student,sno is:" + sno)
        case Teacher(name,tno) => println(name + " is a teacher,tno is:" + tno)
        case None(name) => println("None matched")
        case x:Company => println(s"$x{name}")
        case _ => println("Abstract class")
    }
    val p = Student("yy",20151214)
    val obj = new Person {}
    val com = new Company("YY")

    caseClassMatch(p)
    caseClassMatch(obj)
    caseClassMatch(com)
}
