package build.unstable.sonicd.source.kafka

import akka.NotUsed
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Status, SupervisorStrategy, Terminated}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, KafkaConsumerActor, Subscriptions}
import akka.stream._
import akka.stream.scaladsl.{BroadcastHub, Keep, Source}
import build.unstable.sonic.model.RequestContext
import build.unstable.sonicd.SonicdLogging
import build.unstable.tylog.Variation
import org.apache.kafka.common.TopicPartition
import org.slf4j.event.Level

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

class KafkaSupervisor(bootstrapServers: String, groupId: Option[String], maxPartitions: Int, bufferSize: Int)
                     (implicit val materializer: ActorMaterializer) extends Actor with SonicdLogging {
  import KafkaSupervisor._

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("stopping kafka supervisor of {} for group {}", bootstrapServers, groupId)
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting kafka supervisor of {} for group {} with maxPartitions {} and bufferSize {}",
      bootstrapServers, groupId, maxPartitions, bufferSize)
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(_) ⇒ SupervisorStrategy.Stop
  }

  val streams = mutable.Map.empty[String, (ActorRef, Source[_, NotUsed])]
  val subscribers = mutable.Map.empty[ActorRef, String]

  // FIXME: for now settings are ignored
  // so first query will set the consumer settings
  // and the rest will use the same subscriber
  // even if they have different settings
  // (except bootstrapServers, groupId)
  def getId(topic: String, partition: Option[Int], offset: Option[Long], settings: ConsumerSettings[_, _]): String =
  topic + partition.map(_.toString).getOrElse("") + offset.map(_.toString).getOrElse("") +
    settings.properties.values.toVector.sorted

  override def receive: Receive = {
    case Terminated(ref) ⇒
      // handled terminated sonic publisher
      subscribers.remove(ref).foreach { id ⇒
        // if there aren't any more subscribers kill graph
        if (subscribers.count(_._2 == id) == 0) {
          streams.remove(id).foreach { kv ⇒
            kv._1 ! PoisonPill
            log.info("shutting down down broadcast hub {}", id)
          }
        }
      }
      context unwatch ref

    case c@GetBroadcastHub(topic, partition, offset, settings) ⇒
      val id = getId(topic, partition, offset, settings)
      val sdr = sender()
      val message = streams.get(id).map(_._2).getOrElse {
        log.tylog(Level.INFO, c.ctx.traceId, MaterializeBroadcastHub(id), Variation.Attempt,
          "hub with id {} not found. materializing one..", id)

        lazy val actor = context.actorOf(KafkaConsumerActor.props(settings))

        ((partition, offset) match {
          // create manual subscription with given offset and partition
          case (Some(p), Some(o)) ⇒
            log.info("creating plain external source with settings {}", settings.properties)
            val subscription = Subscriptions.assignmentWithOffset(new TopicPartition(topic, p), o)
            Try(Consumer.plainExternalSource(actor, subscription))

          case (Some(p), None) ⇒
            log.info("creating plain external source with settings {}", settings.properties)
            val subscription = Subscriptions.assignment(new TopicPartition(topic, p))
            Try(Consumer.plainExternalSource(actor, subscription))

          // FIXME: disabled until we figure out how to terminate underlying resources
          // create auto subscription, offset will need to either be handled manually by client
          // or automatically if offset set to true in query config
          //  kafka-clients {
          //    enable.auto.commit = true
          //  }
          // case (None, None) ⇒
          //   log.debug("creating plain source with settings {}", settings.properties)
          //   Try(Consumer.plainSource(settings, Subscriptions.topics(topic)))
          //
          // error: both need to be set
          case _ ⇒
            Failure(new Exception("unable to create manual subscription: both 'partition' and 'offset' need to be set"))
        }).map { source ⇒

          val broadcast = source.toMat(BroadcastHub.sink(bufferSize))(Keep.right).run()

          // monitor subscriber termination to determine if graph should be terminated
          context watch sdr

          streams.update(id, actor → broadcast)
          subscribers.update(sdr, id)

          log.tylog(Level.INFO, c.ctx.traceId, MaterializeBroadcastHub(id), Variation.Success,
            "successfully materialized {} with consumer properties {}", id, settings.properties)
          broadcast: Source[_, _]
        }.recover {
          case e: Exception ⇒
            streams.remove(id) //just in case the error was in the flow or sink phases materialization
            log.tylog(Level.INFO, c.ctx.traceId, MaterializeBroadcastHub(id), Variation.Failure(e), "")
            Status.Failure(e)
        }.get
      }

      sdr ! message
  }
}

object KafkaSupervisor {

  case class GetBroadcastHub(topic: String, partition: Option[Int],
                             offset: Option[Long], settings: ConsumerSettings[_, _])(implicit val ctx: RequestContext)
}
