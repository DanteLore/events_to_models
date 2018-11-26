package com.logicalgenetics.reports

import org.scalatra._

class ReportServer extends ScalatraFilter {
  get("/") {
    <h1>Hello, {params("name")}</h1>
  }
}