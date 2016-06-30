package build.unstable.sonicd.source

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, Props, ActorContext}
import build.unstable.sonicd.model.{SonicdLogging, DataSource, RequestContext, Query}

/*
class ElasticSearchSource (query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  val nodeUrl: String = getConfig[String]("url")
  val nodePort: Int = getConfig[Int]("port")
  
  val supervisorName = ElasticSearch.getSupervisorName(nodeUrl)

  lazy val handlerProps: Props = {
    //if no es supervisor has been initialized yet for this cluster, initialize one
    val prestoSupervisor = actorContext.child(supervisorName).getOrElse {
      actorContext.actorOf(prestoSupervisorProps(masterUrl, masterPort), supervisorName)
    }

    Props(classOf[ElasticSearchPublisher], query.id.get.toString, query.query, prestoSupervisor,
      masterUrl, SonicdConfig.PRESTO_MAX_RETRIES,
      SonicdConfig.PRESTO_RETRYIN, context)
  }
}

object ElasticSearch {

  def getSupervisorName(nodeUrl: String): String = s"es_$nodeUrl"

}

class ElasticSearchSupervisor(masterUrl: String, port: Int) extends Actor with SonicdLogging {
  def receive: Receive = {

  }
}*/
