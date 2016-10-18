package build.unstable.sonicd.service

import java.io.File
import java.nio.file.WatchEvent.Kind
import java.nio.file.{Path, WatchEvent}

import akka.actor.{Actor, ActorRef, Props}
import akka.stream.actor.ActorPublisher
import akka.testkit.CallingThreadDispatcher
import build.unstable.sonic._
import build.unstable.sonic.client.Sonic
import build.unstable.sonic.model.{ApiUser, AuthConfig, RequestContext, SonicMessage}
import build.unstable.sonicd.source.SyntheticPublisher
import build.unstable.sonicd.source.file.FileWatcherWorker

object Fixture {

  import build.unstable.sonicd.model.Fixture._

  val queryBytes = Sonic.lengthPrefixEncode(syntheticQuery.toBytes)

  // in memory db
  val H2Driver = "org.h2.Driver"

  val testUser = ApiUser("serrallonga", 10, AuthConfig.Mode.ReadWrite, None)

  val testCtx = RequestContext("1", Some(testUser))

  val syntheticPubProps = Props(classOf[SyntheticPublisher], 1L, None, Some(1), 10, "1", false, None, testCtx)
    .withDispatcher(CallingThreadDispatcher.Id)

  val zombiePubProps = Props[Zombie].withDispatcher(CallingThreadDispatcher.Id)

  val tmp = new File("/tmp/sonicd_specs")
  val tmp2 = new File("/tmp/sonicd_specs/recursive")
  val tmp3 = new File("/tmp/sonicd_specs/recursive/rec2")
  val tmp32 = new File("/tmp/sonicd_specs/recursive/rec2/rec2")
  val tmp4 = new File("/tmp/sonicd_specs/recursive2")
  val file = new File("/tmp/sonicd_specs/recursive/tmp.txt")
  val file2 = new File("/tmp/sonicd_specs/recursive/rec2/tmp.txt")
  val file3 = new File("/tmp/sonicd_specs/logback.xml")
  val file4 = new File("/tmp/sonicd_specs/logback2.xml")

  def getEvent(k: Kind[Path], path: Path) = new WatchEvent[Path] {
    override def count(): Int = 1

    override def kind(): Kind[Path] = k

    override def context(): Path = path
  }
}

class Zombie extends ActorPublisher[SonicMessage] {
  override def receive: Actor.Receive = {
    case any ⇒ //ignore
  }
}

class ImplicitRedirectActor(implicitSender: ActorRef) extends Actor {
  override def receive: Receive = {
    case FileWatcherWorker.DoWatch ⇒ implicitSender ! FileWatcherWorker.DoWatch
    case anyMsg ⇒ implicitSender ! anyMsg
  }
}
