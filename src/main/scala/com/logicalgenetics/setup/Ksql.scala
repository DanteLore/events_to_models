package com.logicalgenetics.setup
import com.softwaremill.sttp._

case class KsqlResponse(code : Int, body : String)

object Ksql {
  def query(ksql: String): KsqlResponse = {

    val message = s"""{
                      "ksql": "${clean(ksql)}",
                      "streamsProperties": {
                         "ksql.streams.auto.offset.reset": "earliest"
                       }
                    }"""

    val request = sttp
      .header("Accept", "application/json")
      .header("Content-Type", "application/json")
      .header("charset", "utf-8")
      .body(message)
      .post(uri"http://localhost:8088/query")

    val response = request.send()

    KsqlResponse(response.code, response.body match { case Left(x) => x case _ => "SUCCESS" } )
  }

  implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()

  def command(ksql : String) : KsqlResponse = {

    val message = s"""{
                      "ksql": "${clean(ksql)}"
                    }"""

    val request = sttp
      .header("Accept", "application/json")
      .header("Content-Type", "application/json")
      .header("charset", "utf-8")
      .body(message)
      .post(uri"http://localhost:8088/ksql")

    val response = request.send()

    KsqlResponse(response.code, response.body match { case Left(x) => x case _ => "SUCCESS" } )
  }

  private def clean(ksql: String) = {
    val pretty = ksql.trim match {
      case s if s.endsWith(";") => s
      case s => s"$s;"
    }
    pretty
  }
}