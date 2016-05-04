package build.unstable.sonicd.service

import akka.actor.{Actor, Props}
import akka.stream.actor.ActorPublisher
import akka.testkit.CallingThreadDispatcher
import build.unstable.sonicd.model.{SonicMessage, SonicdSource}
import build.unstable.sonicd.source.SyntheticPublisher
import spray.json._

object Fixture {

  import build.unstable.sonicd.model.Fixture._

  val queryBytes = SonicdSource.lengthPrefixEncode(syntheticQuery.toBytes)

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

  val syntheticPubProps = Props(classOf[SyntheticPublisher], 1000, Some(1), 10, "1", false)
    .withDispatcher(CallingThreadDispatcher.Id)

  val zombiePubProps = Props[Zombie].withDispatcher(CallingThreadDispatcher.Id)
}

class Zombie extends ActorPublisher[SonicMessage] {
  override def receive: Actor.Receive = {
    case any ⇒ //ignore
  }
}
