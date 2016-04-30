package build.unstable.sonicd.service.source

import build.unstable.sonicd.model.Query
import spray.json._

object Fixture {

  val syntheticSourceConfig = """{"class" : "SyntheticSource"}""".parseJson.asJsObject
  val syntheticQuery = Query("10", syntheticSourceConfig)

  // in memory db
  val testDB = "testdb"
  val H2Url = s"jdbc:h2:mem:$testDB;DB_CLOSE_DELAY=-1;"
  val H2Driver = "org.h2.Driver"
  val H2Config =
    s"""
       | {
       |  "driver" : "$H2Driver",
       |  "url" : "$H2Url",
       |  "class" : "JdbcSource"
       | }
    """.stripMargin.parseJson.asJsObject

}
