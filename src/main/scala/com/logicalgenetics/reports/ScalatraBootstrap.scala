package com.logicalgenetics.reports

import javax.servlet.ServletContext
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    context mount (new BeerServer, "/beer/*")
    context mount (new SalesServer, "/sales/*")
  }
}