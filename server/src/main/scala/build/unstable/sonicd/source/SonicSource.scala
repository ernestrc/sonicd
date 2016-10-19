package build.unstable.sonicd.source

import java.net.InetSocketAddress

import akka.actor.{ActorContext, Props}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.client.SonicPublisher
import build.unstable.sonic.model.{DataSource, Query, RequestContext, SonicCommand}
import build.unstable.sonic.scaladsl.Sonic._
import spray.json.JsObject

class SonicSource(q: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(q, actorContext, context) {

  val host: String = getConfig[String]("host")
  val port: Int = getOption[Int]("port").getOrElse(8889)
  val config: JsObject = getConfig[JsObject]("config")
  val addr = new InetSocketAddress(host, port)

  val query: SonicCommand = Query(q.query, config, q.auth).copy(trace_id = q.traceId)

  val supervisorName = s"sonic_${addr.hashCode()}"

  lazy val publisher: Props = {
    val sonicSupervisor = actorContext.child(supervisorName).getOrElse {
      actorContext.actorOf(sonicSupervisorProps(addr), supervisorName)
    }

    Props(classOf[SonicPublisher], sonicSupervisor, query, false)
  }
}
