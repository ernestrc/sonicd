package build.unstable.sonicd.source

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.file.{Path, Paths, StandardWatchEventKinds, WatchEvent}

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import spray.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
 * Watches JSON files in 'path' and streams their contents.
 *
 * Takes an optional 'tail' parameter to configure if only new data should be streamed
 */
class JsonStreamSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends DataSource(config, queryId, query, context) {

  val handlerProps: Props = {
    val file = getConfig[String]("path")
    val tail = getOption[Boolean]("tail").getOrElse(false)

    Props(classOf[JsonStreamPublisher], queryId, file, query, tail)
  }
}

class JsonStreamPublisher(queryId: String, folderPath: String, rawQuery: String, tail: Boolean)
  extends Actor with ActorPublisher[SonicMessage] with ActorLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting json stream publisher of '{}'", queryId)
  }

  override def postStop(): Unit = {
    log.info(s"stopping json stream publisher of '$queryId'")
    //close resources
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.error(reason, "restarted json stream publisher")
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy.apply(loggingEnabled = true) {
    case NonFatal(e) ⇒
      self ! DoneWithQueryExecution.error(e)
      log.error(e, "watcher terminated unexpectedly")
      Stop
  }

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 10.seconds

  import context.dispatcher

  /* HELPERS */

  def newFile(fileName: String) = {
    val fs = new FileInputStream(Paths.get(folderPath, fileName).toFile)
    val br = new BufferedReader(new InputStreamReader(fs))
    if (tail) {
      var count = br.lines.count() - 1
      log.debug("tail mode enabled. Skipping {} lines", count)
      while (count > 0) {
        br.readLine()
        count -= 1
      }
    }
    files.update(fileName, br)
    br
  }

  def terminate(done: DoneWithQueryExecution) = {
    onNext(done)
    onCompleteThenStop()
  }

  def filter(json: JsObject, query: Query): Option[Map[String, JsValue]] = {
    if (query.filter(json)) {
      val fields = json.fields.filter(query.select)
      Some(fields)
    } else None
  }

  @tailrec
  final def stream(fw: ActorRef, br: BufferedReader, query: Query, retries: Int = 2): Unit = {
    lazy val read =
      if (buffer.isEmpty) Option(br.readLine())
      else Option(buffer.remove(0))

    if (totalDemand > 0 && read.isDefined && read.get != "") {

      val raw = read.get
      val json = raw.parseJson.asJsObject
      val filtered = filter(json, query)

      if (!sentMeta && filtered.isDefined) {
        onNext(TypeMetadata(filtered.get.toVector))
        sentMeta = true
      }

      filtered match {
        case Some(fields) if totalDemand > 0 ⇒
          onNext(OutputChunk(JsArray(fields.values.to[Vector])))
        case Some(fields) ⇒ buffer.append(raw)
        case None ⇒ log.debug("filtered {}", filtered)
      }
      stream(fw, br, query)
    } else if (totalDemand > 0 && retries > 0) {
      stream(fw, br, query, retries - 1)
    } else if (totalDemand > 0) {
      fw ! Request(totalDemand)
    }
  }

  case class Query(select: ((String, JsValue)) ⇒ Boolean,
                   filter: JsObject ⇒ Boolean, raw: String)

  def matchObject(arg: JsObject): JsObject ⇒ Boolean = (o: JsObject) ⇒ {
    val argFields = arg.fields
    o.fields.forall {
      case (s: String, b: JsObject) if argFields.get(s).isDefined && argFields(s).isInstanceOf[JsObject] ⇒
        matchObject(argFields(s).asJsObject)(b)
      case (s: String, b: JsValue) ⇒ arg.fields.forall(kv ⇒ if (kv._1 == s) b == kv._2 else true)
    }
  }

  /**
   * {
   * "select" : ["field1", "field2"],
   * "filter" : { "field1" : "value1" }
   * }
   */
  def parseQuery(raw: String): Query = {
    val r = raw.parseJson.asJsObject(s"Query must be a valid JSON object: $raw").fields

    val select = r.get("select").map { v ⇒
      val fields = v.convertTo[Vector[String]]
      (v: (String, JsValue)) ⇒ fields.contains(v._1)
    }.getOrElse((v: (String, JsValue)) ⇒ true)

    val f = r.get("filter").map { fo ⇒
      val fObj = fo.asJsObject(s"filter key must be a valid JSON object: ${fo.compactPrint}")
      matchObject(fObj)
    }.getOrElse((o: JsObject) ⇒ true)

    Query(select, f, raw)
  }


  /* STATE */

  val files = mutable.Map.empty[String, BufferedReader]
  val buffer = ListBuffer.empty[String]
  var sentMeta: Boolean = false
  val watching: Boolean = false


  /* BEHAVIOUR */

  def streaming(watcher: ActorRef, query: Query): Receive = {

    case req: Request ⇒
      files.foreach(kv ⇒ stream(watcher, kv._2, query))
      if (totalDemand > 0) watcher ! Request(totalDemand)

    case done: DoneWithQueryExecution ⇒
      if (totalDemand > 0) terminate(done)
      else context.system.scheduler.scheduleOnce(1.second, self, done)

    case event: WatchEvent[_] ⇒
      val kind = event.kind()

      //not registered OVERFLOW so all events context are Path
      val ev = event.asInstanceOf[WatchEvent[Path]]
      val fileName = ev.context().toFile.toString

      //log.debug("file {} changed: {}", fileName, kind.name())

      kind match {
        case StandardWatchEventKinds.ENTRY_CREATE ⇒ newFile(fileName)
        case StandardWatchEventKinds.ENTRY_MODIFY ⇒ files.get(fileName) match {
          case Some(reader) ⇒
            stream(watcher, reader, query)
          case None ⇒
            val reader = newFile(fileName)
            stream(watcher, reader, query)
        }
        case StandardWatchEventKinds.ENTRY_DELETE ⇒ files.remove(fileName)
      }
  }

  def receive: Receive = {

    case req: Request ⇒
      log.debug("recv first request for query {} on path {}", rawQuery, folderPath)
      try {
        val fw = context.actorOf(Props(classOf[JsonWatcher], Paths.get(folderPath), queryId))
        val parsed = parseQuery(rawQuery)
        fw ! req
        context.become(streaming(fw, parsed))
      } catch {
        case e: Exception ⇒
          terminate(DoneWithQueryExecution.error(e))
      }

    case Cancel ⇒
      log.debug("client canceled")
      onCompleteThenStop()
  }
}

class JsonWatcher(folder: Path, queryId: String) extends Actor with ActorLogging {

  import java.nio.file._

  import scala.collection.JavaConversions._

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting file watcher of '{}'", queryId)
  }

  override def postStop(): Unit = {
    log.info(s"stopping file watcher of '$queryId'")
    try {
      watcher.close()
      key.cancel()
      log.debug("closed watcher object {}", watcher)
    } catch {
      case e: Exception ⇒
    }
  }

  var watcher: WatchService = FileSystems.getDefault.newWatchService
  log.debug("created watcher object {}", watcher)

  def watch(): List[WatchEvent[_]] = {
    //FIXME blocks until file changes. Use with timeout
    val k = watcher.take
    val events = k.pollEvents

    if (k.reset()) {
      events.toList.filter { ev ⇒
        ev.asInstanceOf[WatchEvent[Path]].context().toFile.toString.endsWith(".json")
      }
    } else throw new Exception("aborted")
  }

  val key = folder.register(
    watcher,
    StandardWatchEventKinds.ENTRY_CREATE,
    StandardWatchEventKinds.ENTRY_MODIFY,
    StandardWatchEventKinds.ENTRY_DELETE
  )

  override def receive: Actor.Receive = {
    case r: Request ⇒
      log.debug("watching contents of folder {}; request is {}", folder, r.n)
      val ev = watch()
      if (ev.nonEmpty) {
        ev.foreach(context.parent ! _)
      } else {
        log.debug("watching again..")
        self ! r
      }
    case msg ⇒ log.warning("oops! extraneous message")
  }
}
