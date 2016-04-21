package build.unstable.sonicd.source

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import akka.actor._
import akka.stream.actor.ActorPublisher
import build.unstable.sonicd.SonicConfig
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.JdbcConnectionsHandler.{GetJdbcHandle, JdbcHandle}
import spray.json._

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try

class JdbcSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends DataSource(config, queryId, query, context) {

  val jdbcConnectionsProps: Props =
    Props(classOf[JdbcConnectionsHandler]).withDispatcher("akka.actor.jdbc-dispatcher")

  lazy val handlerProps: Props = {
    //if no jdbc-conn-guardian actor has been initialized yet, initialize one
    val jdbcConns = context.child(JdbcConnectionsHandler.actorName).getOrElse {
      context.actorOf(jdbcConnectionsProps, JdbcConnectionsHandler.actorName)
    }

    val user: String = config.fields.get("user").map(_.convertTo[String]).getOrElse("sonicd")
    val initializationStmts: List[String] =
      config.fields.get("pre").map(_.convertTo[List[String]]).getOrElse(Nil)
    val password: String = config.fields.get("password").map(_.convertTo[String]).getOrElse("")
    val dbUrl: String = config.fields.getOrElse("url",
      throw new Exception("missing 'url' key in config")).convertTo[String]
    val driver: String = config.fields.getOrElse("driver",
      throw new Exception("missing 'driver' key in config")).convertTo[String]

    Props(classOf[JdbcPublisher], queryId, query, dbUrl, user, password, driver,
      jdbcConns, initializationStmts).withDispatcher("akka.actor.jdbc-dispatcher")
  }
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
                    connections: ActorRef,
                    initializationStmts: List[String])
  extends ActorPublisher[SonicMessage] with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info(s"stopping jdbc source of '$queryId'")
    //close resources
    try {
      rs.close()
      log.debug("closed result set {}", rs)
    } catch {
      case e: Exception ⇒
    }
    if (handle != null) {
      if (!isDone) {
        try {
          //TODO move execute to child actor so that we can cancel
          handle.stmt.cancel()
          log.debug(s"successfully canceled query '$queryId'")
        } catch {
          case e: Exception ⇒
        }
      }
      connections ! handle
    }
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info(s"starting jdbc source of '$queryId' pointing at '$dbUrl'")
  }

  def splitBatch(query: String): List[String] = {
    val buf = ListBuffer.empty[String]
    for (s ← query.split(";")) {
      val trim = s.trim()
      if (trim != "" && !trim.startsWith("--"))
        buf.append(trim)
    }
    buf.toList
  }

  var handle: JdbcHandle = null
  var rs: ResultSet = null
  var metadata: TypeMetadata = null
  var isDone: Boolean = false

  def parseArrayVal: PartialFunction[Any, JsValue] = {
    case s: String ⇒ JsString(s)
    case n: java.lang.Number ⇒ JsNumber(n.longValue)
    case b: java.lang.Boolean ⇒ JsBoolean(b)
    case a: Array[_] ⇒ JsArray(a.map(parseArrayVal).toVector)
    case el ⇒ JsString(el.toString)
  }

  def stream(n: Int) = {
    try {
      var i = n
      var last = false
      while (i > 0 && (if (rs.next()) true
      else {
        last = true; false
      })) {
        val data = scala.collection.mutable.ListBuffer.empty[JsValue]
        var pos = 1
        while (pos <= metadata.typesHint.size) {
          val (_, typeHint) = metadata.typesHint(pos - 1)
          val value = typeHint match {
            case s: JsString ⇒ JsString(rs.getString(pos))
            case b: JsBoolean ⇒ JsBoolean(rs.getBoolean(pos))
            case n: JsNumber ⇒ JsNumber(rs.getLong(pos))
            case o: JsObject ⇒
              val raw = rs.getString(pos)
              Try(raw.parseJson).getOrElse(JsString(raw))
            case a: JsArray ⇒
              val data = rs.getArray(pos).getArray().asInstanceOf[Array[AnyRef]]
              JsArray(data.map(parseArrayVal).toVector)
            case e ⇒ JsString(rs.getString(pos))
          }
          if (rs.wasNull) {
            data.append(JsNull)
          } else data.append(value)
          pos += 1
        }
        if (data.isEmpty) {
          throw new Exception("could not extract any column from row. this is most likely an error in sonicd's JdbcSource")
        }
        onNext(OutputChunk(JsArray(data.toVector)))
        i -= 1
      }
      if (last && totalDemand > 0) {
        onNext(DoneWithQueryExecution(success = true))
        onCompleteThenStop()
      }
    } catch {
      case e: Exception ⇒
        onNext(DoneWithQueryExecution.error(e))
        onCompleteThenStop()
    }
  }

  // FIXME
  def isSelect(query: String): Boolean = {
    log.debug("determining if statement is select statement..")
    false //JdbcPublisher.IS_SQL_SELECT.pattern.matcher(query).matches
  }

  //rs should be pointing to a ResultSet at this point
  def streaming(demand: Int): Receive = {
    stream(demand)
    val recv: Receive = {
      case Request(n) ⇒ stream(n.toInt)
      case Cancel ⇒
        log.debug("client canceled")
        onCompleteThenStop()
    }
    recv
  }

  @tailrec
  final def update(demand: Int, pendingStream: List[SonicMessage]): Unit = {
    if (demand > 0 && pendingStream.nonEmpty) {
      onNext(pendingStream.head)
      update(demand - 1, pendingStream.tail)
    } else if (pendingStream.isEmpty) {
      onCompleteThenStop()
    }
  }

  def updating(demand: Int, pendingStream: List[SonicMessage]): Receive = {
    update(demand, pendingStream)
    val recv: Receive = {
      case Request(n) ⇒ update(n.toInt, pendingStream)
      case Cancel ⇒ onCompleteThenStop()
    }
    recv
  }

  val classLoader = this.getClass.getClassLoader

  def receive: Receive = {

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒
      connections ! GetJdbcHandle(isSelect(query), driver, dbUrl, user, password)
      log.debug("waiting for handle of {}", dbUrl)
      context.become({
        case j@JdbcHandle(conn, stmt) ⇒
          handle = j
          log.debug("received jdbc handle {}'", handle)
          try {
            initializationStmts.foreach { s ⇒
              log.debug("executing initialization statement: {}", s)
              stmt.execute(s.replace(";", ""))
            }
            val statements = splitBatch(query)

            //if multiple statements, don't bother with ResultSets
            if (statements.length > 1) {
              statements.foreach(stmt.execute)
              context.become(updating(n.toInt, List(DoneWithQueryExecution(success = true, Vector.empty))))
            } else {
              if (stmt.execute(query.replace(";", ""))) {
                rs = stmt.getResultSet
                val rsmd = rs.getMetaData
                val columnCount = rsmd.getColumnCount
                metadata = TypeMetadata(
                  // The column count starts from 1
                  (1 to rsmd.getColumnCount).foldLeft(Vector.empty[(String, JsValue)]) { (columns, i) ⇒
                    val typeHint = rsmd.getColumnClassName(i) match {
                      case "java.lang.String" ⇒ JsString("")
                      case "java.lang.Boolean" ⇒ JsBoolean(true)
                      case "java.lang.Object" ⇒ JsObject.empty
                      case "java.sql.Array" ⇒ JsArray.empty
                      case num if Try(classLoader.loadClass(num).getSuperclass.equals(classOf[Number])).getOrElse(false) ⇒ JsNumber(0)
                      case e ⇒ JsString(rsmd.getColumnClassName(i))
                    }
                    columns :+ (rsmd.getColumnLabel(i) → typeHint)
                  })
                onNext(metadata)
                context.become(streaming(n.toInt - 1))
              } else {
                metadata = TypeMetadata(Vector.empty)
                onNext(metadata)
                context.become(updating(n.toInt - 1, DoneWithQueryExecution(success = true, Vector.empty) :: Nil))
              }
            }
          } catch {
            case e: Exception ⇒
              val msg = "there was an error when running query"
              log.error(e, msg)
              self ! DoneWithQueryExecution.error(e)
          } finally {
            isDone = true
          }

        case r: DoneWithQueryExecution ⇒
          onNext(r)
          onCompleteThenStop()

      }, discardOld = false)

    case Cancel ⇒ onCompleteThenStop()
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
        //reuse connection or create a new one and store
        val conn: Connection = {
          //register driver
          Class.forName(driver)
          log.debug("registered driver {}", driver)
          val c = DriverManager.getConnection(url, user, password)
          log.debug("created new connection {}", c)
          c.setAutoCommit(true)
          c
        }
        var stmt: Statement = null
        //try to set streaming properties for each driver
        try {
          if (driver == "org.postgresql.Driver" && isQuery) {
            log.debug("setting streaming properties for postgresql")
            if (conn.getAutoCommit) {
              conn.setAutoCommit(false)
            }
            stmt = conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.FETCH_FORWARD
            )
            stmt.setFetchSize(SonicConfig.JDBC_FETCHSIZE)
          } else if (driver == "com.mysql.jdbc.Driver" && isQuery) {
            log.debug("setting streaming properties for mysql")
            stmt = conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY
            )
            stmt.setFetchSize(Integer.MIN_VALUE)
          } else if (isQuery) {
            stmt = conn.createStatement()
            stmt.setFetchSize(SonicConfig.JDBC_FETCHSIZE)
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
