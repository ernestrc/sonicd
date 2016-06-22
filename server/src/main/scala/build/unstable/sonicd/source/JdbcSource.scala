package build.unstable.sonicd.source

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import build.unstable.sonicd.SonicdConfig
import build.unstable.sonicd.auth.RequestContext
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.JdbcConnectionsHandler.{GetJdbcHandle, JdbcHandle}
import spray.json._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try

class JdbcSource(config: JsObject, queryId: String, query: String, context: ActorContext, apiUser: Option[RequestContext])
  extends DataSource(config, queryId, query, context, apiUser) {

  val jdbcConnectionsProps: Props =
    Props(classOf[JdbcConnectionsHandler]).withDispatcher("akka.actor.jdbc-dispatcher")

  //if no jdbc-conn-guardian actor has been initialized yet, initialize one
  lazy val jdbcConnectionsActor = context.child(JdbcConnectionsHandler.actorName).getOrElse {
    context.actorOf(jdbcConnectionsProps, JdbcConnectionsHandler.actorName)
  }
  val user: String = getOption[String]("user").getOrElse("sonicd")
  val initializationStmts: List[String] = getOption[List[String]]("pre").getOrElse(Nil)
  val password: String = getOption[String]("password").getOrElse("")
  val dbUrl: String = getConfig[String]("url")
  val driver: String = getConfig[String]("driver")
  val executorProps = (conn: Connection, stmt: Statement) ⇒
    Props(classOf[JdbcExecutor], queryId, query, conn, stmt, initializationStmts)
      .withDispatcher("akka.actor.jdbc-dispatcher")

  lazy val handlerProps: Props = Props(classOf[JdbcPublisher],
    queryId, query, dbUrl, user, password, driver,
    executorProps, jdbcConnectionsActor, initializationStmts
  ).withDispatcher("akka.actor.jdbc-dispatcher")

}

object JdbcPublisher {

  val IS_SQL_SELECT = "^(\\s*?)(?i)select\\s*?.*?\\s*?(?i)from(.*)*?".r
}

class JdbcPublisher(queryId: String,
                    query: String,
                    dbUrl: String,
                    user: String,
                    password: String,
                    driver: String,
                    executorProps: (Connection, Statement) ⇒ Props,
                    connections: ActorRef,
                    initializationStmts: List[String])
  extends ActorPublisher[SonicMessage] with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info(s"stopping jdbc publisher of '$queryId'")
    if (handle != null & !isDone) {
      try {
        handle.stmt.cancel()
        log.debug(s"successfully canceled query '$queryId'")
      } catch {
        case e: Exception ⇒ log.warning(s"could not cancel query '$queryId': $e")
      }
    }
    connections ! handle
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info(s"starting jdbc publisher of '$queryId' pointing at '$dbUrl'")
  }


  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case e: Exception ⇒ SupervisorStrategy.Escalate
    }

  var handle: JdbcHandle = null
  var isDone: Boolean = false

  def streaming(executor: ActorRef): Receive = {
    executor ! Request(totalDemand)

    {
      case r: Request ⇒ executor ! r
      case Terminated(ref) ⇒
        isDone = true
        log.debug("executor stopped")
        onCompleteThenStop()
      case s: SonicMessage ⇒ onNext(s)
      case Cancel ⇒
        log.debug("client canceled")
        onCompleteThenStop()
    }
  }

  // FIXME
  def isSelect(query: String): Boolean = {
    log.debug("determining if statement is select statement..")
    false //JdbcPublisher.IS_SQL_SELECT.pattern.matcher(query).matches
  }

  def waitingForHandle: Receive = {
    case j@JdbcHandle(conn, stmt) ⇒
      handle = j
      log.debug("received jdbc handle {}'", handle)
      val executor = context.actorOf(executorProps(conn, stmt))
      context.watch(executor)
      context.become(streaming(executor))

    case r: DoneWithQueryExecution ⇒
      onNext(r)
      onCompleteThenStop()
  }

  def receive: Receive = {

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒
      connections ! GetJdbcHandle(isSelect(query), driver, dbUrl, user, password)
      log.debug("waiting for handle of {}", dbUrl)
      context.become(waitingForHandle, discardOld = false)

    case Cancel ⇒ onCompleteThenStop()
  }

}

//decoupled from publisher so that we can cancel the query
class JdbcExecutor(queryId: String,
                   query: String,
                   conn: Connection,
                   stmt: Statement,
                   initializationStmts: List[String]) extends Actor with ActorLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting jdbc executor of '{}'", queryId)
  }

  override def postStop(): Unit = {
    log.info(s"stopping jdbc executor of '$queryId'")
    //close resources
    try {
      rs.close()
      log.debug("closed result set {}", rs)
    } catch {
      case e: Exception ⇒
    }
  }

  var rs: ResultSet = null
  var classMeta: Vector[Class[_]] = null
  var isDone = false
  val classLoader = this.getClass.getClassLoader

  def splitBatch(query: String): List[String] = {
    val buf = ListBuffer.empty[String]
    for (s ← query.split(";")) {
      val trim = s.trim()
      if (trim != "") buf.append(trim)
    }
    buf.toList
  }

  def parseArrayVal: PartialFunction[Any, JsValue] = {
    case s: String ⇒ JsString(s)
    case n if n.getClass.isAssignableFrom(classOf[java.lang.Number]) ⇒
      JsNumber(n.asInstanceOf[java.lang.Number].doubleValue())
    case b: java.lang.Boolean ⇒ JsBoolean(b)
    case a: Array[_] ⇒ JsArray(a.map(parseArrayVal).toVector)
    case el ⇒ JsString(el.toString)
  }

  def extractValue[T](v: T)(c: (T) ⇒ JsValue): JsValue =
    if (v != null) c(v) else JsNull

  def terminate(done: DoneWithQueryExecution) = {
    context.parent ! done
    context.stop(self)
  }

  def streaming(): Receive = {
    case Request(n) ⇒
      var i = n
      while (i > 0 && (if (rs.next()) true
      else {
        isDone = true;
        false
      })) {
        val data = scala.collection.mutable.ListBuffer.empty[JsValue]
        var pos = 1
        while (pos <= classMeta.size) {
          val typeHint = classMeta(pos - 1)
          val value = typeHint match {
            case clazz if clazz.isAssignableFrom(classOf[String]) ⇒
              extractValue(rs.getString(pos))(JsString.apply)
            case clazz if clazz.isAssignableFrom(classOf[Boolean]) ⇒
              extractValue(rs.getBoolean(pos))(JsBoolean.apply)
            case clazz if clazz.isAssignableFrom(classOf[Double]) ⇒
              extractValue(rs.getDouble(pos))(JsNumber.apply)
            case clazz if clazz.isAssignableFrom(classOf[Long]) ⇒
              extractValue(rs.getLong(pos))(JsNumber.apply)
            case clazz if clazz.isAssignableFrom(classOf[java.sql.Array]) ⇒
              extractValue(rs.getArray(pos)) { value ⇒
                JsArray(value
                  .getArray
                  .asInstanceOf[Array[AnyRef]]
                  .map(parseArrayVal).toVector)
              }
            case clazz if classOf[AnyRef].isAssignableFrom(clazz) ⇒
              extractValue(rs.getString(pos))(value ⇒ value.parseJson)
            case clazz ⇒ extractValue(rs.getString(pos))(JsString.apply)
          }
          if (rs.wasNull) {
            data.append(JsNull)
          } else data.append(value)
          pos += 1
        }
        if (data.isEmpty) {
          throw new Exception("could not extract any column from row. this is most likely an error in sonicd's JdbcSource")
        }
        context.parent ! OutputChunk(JsArray(data.toVector))
        i -= 1
      }
      if (isDone && n > 0) {
        log.debug("stopping: last row extracted")
        terminate(DoneWithQueryExecution.success)
      }
  }

  override def receive: Actor.Receive = {
    case Request(n) ⇒
      log.debug("recv first request of {} elements", n)

      initializationStmts.foreach { s ⇒
        log.debug("executing initialization statement: {}", s)
        stmt.execute(s.replace(";", ""))
      }

      val statements = splitBatch(query)
      log.debug("split query into statements: {}", statements)

      if (statements.isEmpty) {
        terminate(DoneWithQueryExecution.error(new Exception("nothing to run")))
      } else if (statements.foldLeft(true)((acc, u) ⇒ { val isResultSet = stmt.execute(u); acc && isResultSet })) {
        rs = stmt.getResultSet
        val rsmd = rs.getMetaData
        val columnCount = rsmd.getColumnCount

        import scala.language.existentials

        // The column count starts from 1
        val m = (1 to columnCount).foldLeft(Vector.empty[(String, JsValue)] → Vector.empty[Class[_]]) {
          case ((columns, met), i) ⇒
            val (typeHint, clazz): (JsValue, Class[_]) = rsmd.getColumnClassName(i) match {
              case "java.lang.String" ⇒ JsString("") → classOf[String]
              case "java.lang.Boolean" ⇒ JsBoolean(true) → classOf[Boolean]
              case "java.lang.Object" ⇒ JsObject.empty → classOf[AnyRef]
              case "java.sql.Array" ⇒ JsArray.empty → classOf[java.sql.Array]
              case "java.lang.Double" | "java.lang.Float" | "java.math.BigDecimal" ⇒
                JsNumber(0.0) → classOf[Double]
              case num if Try(classLoader.loadClass(num).getSuperclass.equals(classOf[Number])).getOrElse(false) ⇒
                JsNumber(0) → classOf[Long]
              case e ⇒ JsString(rsmd.getColumnClassName(i)) → classOf[Any]
            }
            (columns :+((rsmd.getColumnLabel(i), typeHint)), met :+ clazz)
        }
        classMeta = m._2
        context.parent ! TypeMetadata(m._1)
        self ! Request(n - 1L)
        context.become(streaming())
      } else {
        conn.commit()
        context.parent ! TypeMetadata(Vector.empty) //n at least will be 1
        if (n - 1 > 0) terminate(DoneWithQueryExecution.success)
        else context.become({
          case r: Request ⇒ terminate(DoneWithQueryExecution.success)
        })
      }
  }
}

class JdbcConnectionsHandler extends Actor with ActorLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info("starting jdbc connection handler")
  }

  override def receive: Actor.Receive = {
    case JdbcHandle(conn, stmt) ⇒
      try {
        stmt.close()
        log.debug("closed statement {} of connection {}", stmt, conn)
      } catch {
        case e: Exception ⇒
      }

      try {
        conn.close()
        log.debug("closed connection {}", conn)
      } catch {
        case e: Exception ⇒
      }

    case cmd@GetJdbcHandle(isQuery, driver, url, user, password) ⇒
      try {
        val conn: Connection = {
          //register driver
          Class.forName(driver)
          log.debug("registered driver {}", driver)
          val c = DriverManager.getConnection(url, user, password)
          log.debug("created new connection {}", c)
          c.setAutoCommit(false)
          c
        }
        var stmt: Statement = null
        //try to set streaming properties for each driver
        try {
          if (driver == "org.postgresql.Driver" && isQuery) {
            log.debug("setting streaming properties for postgresql")
            stmt = conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.FETCH_FORWARD
            )
            stmt.setFetchSize(SonicdConfig.JDBC_FETCHSIZE)
          } else if (driver == "com.mysql.jdbc.Driver" && isQuery) {
            log.debug("setting streaming properties for mysql")
            stmt = conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY
            )
            stmt.setFetchSize(Integer.MIN_VALUE)
          } else if (isQuery) {
            stmt = conn.createStatement()
            stmt.setFetchSize(SonicdConfig.JDBC_FETCHSIZE)
          } else {
            stmt = conn.createStatement()
          }
        } catch {
          case e: Exception ⇒
            log.warning(s"could not set streaming properties for driver '{}'", driver)
            stmt = conn.createStatement()
        }
        sender() ! JdbcHandle(conn, stmt)
      } catch {
        case e: Exception ⇒
          log.error(e, "error when preparing connection/statement")
          sender() ! DoneWithQueryExecution.error(e)
      }
  }
}

object JdbcConnectionsHandler {
  val actorName = "jdbconn"

  case class GetJdbcHandle(isQuery: Boolean, driver: String, url: String, user: String, password: String)

  case class JdbcHandle(conn: Connection, stmt: Statement)

}
