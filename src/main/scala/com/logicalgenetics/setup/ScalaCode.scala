package com.logicalgenetics.setup

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

object ScalaCode {
  class ScalaRunner(code : String) extends Runnable {
    override def run(): Unit = {

      val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
      tb.eval(tb.parse(code))
    }
  }

  def executeInThread(code: String) : Unit = {
    new Thread(new ScalaRunner(code)).start()
  }
}
