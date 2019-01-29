package com.logicalgenetics.setup

import scala.io.Source
import sys.process._

object Setup {

  val inputFile = "src/main/ksql/beer-fest.ksql"
  val home : String = System.getProperty("user.home")

  private def handleComment(comment: String) : Unit = {
    println(comment)
  }

  private def handleReminder(reminder: String) : Unit = {
    println(reminder)
    System.in.read
  }

  private def handleCommand(cmd: String) : Unit = {
    val thing = cmd.substring(1).trim.replace("~", home)
    println(thing)

    val result = thing.split(' ').toList.!!
    println(result)
  }

  private def handleKsqlQuery(ksql: String) : Unit = {
    println(s"RUNNING: $ksql")
    println(Ksql.query(ksql))
  }

  private def handleKsqlCommand(ksql: String) : Unit = {
    println(s"RUNNING: $ksql")
    println(Ksql.command(ksql))
  }

  def main(args: Array[String]): Unit = {

    Source
      .fromFile(inputFile)
      .getLines
      .map(_.trim)
      .filterNot(_.isEmpty)
        .foreach{
          case comment if comment.startsWith("//") => handleComment(comment)
          case reminder if reminder.startsWith("#") => handleReminder(reminder)
          case cmd if cmd.startsWith("$") => handleCommand(cmd)
          case ksql if ksql.toLowerCase.startsWith("select") => handleKsqlQuery(ksql)
          case ksql => handleKsqlCommand(ksql)
        }
  }
}
