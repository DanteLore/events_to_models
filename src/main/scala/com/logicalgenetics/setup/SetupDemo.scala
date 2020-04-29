package com.logicalgenetics.setup

import scala.io.Source
import scala.sys.process._

object SetupDemo {

  val inputFile = "beer-fest.setup"
  val home : String = System.getProperty("user.home")

  private def handleComment(comment: String) : Boolean = {
    println(comment)
    true
  }

  private def handleReminder(reminder: String) : Boolean = {
    println(reminder)
    System.in.read
    true
  }

  private def handleCommand(cmd: String) : Boolean = cmd
    .replaceFirst("\\$", "")
    .trim
    .replace("~", home) match {
    case c if c.contains('|') => handlePipedCommand(c)
    case c => handleBasicCommand(c)
  }

  private def handlePipedCommand(cmd: String) : Boolean = {
    val Array(a, b) = cmd.split('|').map(_.trim).filterNot(_.isEmpty)

    (a #| b).!!.trim

    true // There'll be an exception if the command fails and I see no reason to squash it :)
  }

  private def handleBasicCommand(cmd: String) : Boolean = {
    val thing = cmd
    println(thing)

    val result = thing.split("\\s").toList.!!
    println(result)

    true // There'll be an exception if the command fails and I see no reason to squash it :)
  }

  private def handleKsqlQuery(ksql: String) : Boolean = {
    println(s"RUNNING: $ksql")
    val result = Ksql.query(ksql)
    println(s"${result.code}: ${result.body}")
    result.code == 200
  }

  private def handleKsqlCommand(ksql: String) : Boolean = {
    println(s"RUNNING: $ksql")
    val result = Ksql.command(ksql)
    println(s"${result.code}: ${result.body}")
    result.code == 200
  }

  private def handleScalaCode(line: String) : Boolean = {
    val code = line.replaceFirst("!", "").trim
    println(s"Executing: $code")
    ScalaCode.executeInThread(code)
    Thread.sleep(5000)
    true
  }

  def main(args: Array[String]): Unit = {
    Source
      .fromFile(inputFile)
      .getLines
      .map(_.trim)
      .filterNot(_.isEmpty)
        .map{
          case comment if comment.startsWith("//") => handleComment(comment)
          case reminder if reminder.startsWith("#") => handleReminder(reminder)
          case cmd if cmd.startsWith("$") => handleCommand(cmd)
          case code if code.startsWith("!") => handleScalaCode(code)
          case ksql if ksql.toLowerCase.startsWith("select") => handleKsqlQuery(ksql)
          case ksql => handleKsqlCommand(ksql)
        }
      .map(if (_) '\u2705' else '\u274C')
      .foreach(println)
  }
}
