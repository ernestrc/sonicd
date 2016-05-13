package build.unstable.sonicd.source

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import build.unstable.sonicd.SonicdConfig
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.JdbcConnectionsHandler.JdbcHandle
import build.unstable.tylog.Variation
import spray.json._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try

class JdbcSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  val jdbcConnectionsProps: Props =
    Props(classOf[JdbcConnectionsHandler]).withDispatcher("akka.actor.jdbc-dispatcher")

  //if no jdbc-conn-guardian actor has been initialized yet, initialize one
  lazy val jdbcConnectionsActor = actorContext.child(JdbcConnectionsHandler.actorName).getOrElse {
    actorContext.actorOf(jdbcConnectionsProps, JdbcConnectionsHandler.actorName)
  }
  val user: String = getOption[String]("user").getOrElse("sonicd")
  val initializationStmts: List[String] = getOption[List[String]]("pre").getOrElse(Nil)
  val password: String = getOption[String]("password").getOrElse("")
  val dbUrl: String = getConfig[String]("url")
  val driver: String = getConfig[String]("driver")
  val executorProps = (conn: Connection, stmt: Statement) ⇒
    Props(classOf[JdbcExecutor], query.query, conn, stmt, initializationStmts, context)
      .withDispatcher("akka.actor.jdbc-dispatcher")

  lazy val handlerProps: Props = Props(classOf[JdbcPublisher],
    query.query, dbUrl, user, password, driver,
    executorProps, jdbcConnectionsActor, initializationStmts, context)
    .withDispatcher("akka.actor.jdbc-dispatcher")

}

object JdbcPublisher {

  val IS_SQL_SELECT = "^(\\s*?)(?i)select\\s*?.*?\\s*?(?i)from(.*)*?".r
}

class JdbcPublisher(query: String,
                    dbUrl: String,
                    user: String,
                    password: String,
                    driver: String,
                    executorProps: (Connection, Statement) ⇒ Props,
                    connections: ActorRef,
                    initializationStmts: List[String],
                    ctx: RequestContext)
  extends ActorPublisher[SonicMessage] with SonicdLogging {

  import akka.stream.actor.ActorPublisherMessage._

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    info(log, "stopping jdbc publisher of '{}'", ctx.traceId)
    if (handle != null & !isDone) {
      try {
        handle.stmt.cancel()
        debug(log, "successfully canceled query '{}'", ctx.traceId)
      } catch {
        case e: Exception ⇒ warning(log, "could not cancel query '{}': {}", ctx.traceId, e.getMessage)
      }
    }
    connections ! handle
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    info(log, "starting jdbc publisher of '{}' on '{}'", ctx.traceId, dbUrl)
  }


  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case e: Exception ⇒ SupervisorStrategy.Escalate
    }


  /* HELPERS */

  // FIXME
  def isSelect(query: String): Boolean = {
    warning(log, "could not determinte if statement is select: implementation missing")
    false //JdbcPublisher.IS_SQL_SELECT.pattern.matcher(query).matches
  }


  /* STATE */

  var handle: JdbcHandle = null
  var isDone: Boolean = false


  /* BEHAVIOUR */

  def streaming(executor: ActorRef): Receive = {
    executor ! Request(totalDemand)

    {
      case r: Request ⇒ executor ! r
      case Terminated(ref) ⇒
        isDone = true
        onCompleteThenStop()
      case s: SonicMessage ⇒ onNext(s)
      case Cancel ⇒ onCompleteThenStop()
    }
  }

  def waitingForHandle: Receive = {
    case j@JdbcHandle(conn, stmt) ⇒
      trace(log, ctx.traceId, GetConnectionHandle, Variation.Success, "received jdbc handle")
      handle = j
      val executor = context.actorOf(executorProps(conn, stmt))
      context.watch(executor)
      context.become(streaming(executor))

    case r: DoneWithQueryExecution ⇒
      trace(log, ctx.traceId, GetConnectionHandle, Variation.Failure(r.error.get), "could not get jdbc handle")
      onNext(r)
      onCompleteThenStop()
  }

  def receive: Receive = {

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒
      trace(log, ctx.traceId, GetConnectionHandle, Variation.Attempt, "")
      connections ! JdbcConnectionsHandler.GetJdbcHandle(isSelect(query), driver, dbUrl, user, password)
      log.debug("waiting for handle of {}", dbUrl)
      context.become(waitingForHandle, discardOld = false)

    case Cancel ⇒ onCompleteThenStop()
  }

}

//decoupled from publisher so that we can cancel the query
class JdbcExecutor(query: String,
                   conn: Connection,
                   stmt: Statement,
                   initializationStmts: List[String],
                   ctx: RequestContext) extends Actor with SonicdLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting jdbc executor of '{}'", ctx.traceId)
  }

  override def postStop(): Unit = {
    debug(log, "stopping jdbc executor of {}", ctx.traceId)
    //close resources
    try {
      rs.close()
      log.debug("closed result set {}", rs)
    } catch {
      case e: Exception ⇒
    }
  }

  /* HELPERS */

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

  def execute(u: String): Boolean = {
    try {
      stmtN += 1
      trace(log, ctx.traceId, ExecuteStatement,
        Variation.Attempt, "running statement n {}", stmtN)
      val isResultSet = stmt.execute(u)
      trace(log, ctx.traceId, ExecuteStatement,
        Variation.Success, "finished running statement n {}", stmtN)
      isResultSet
    } catch {
      case e: Exception ⇒
        trace(log, ctx.traceId, ExecuteStatement,
          Variation.Failure(e), "error when running statement n {}", stmtN)
        throw e
    }
  }

  val canWrite = ctx.user.exists(_.mode.canWrite)
  val classLoader = this.getClass.getClassLoader


  /* STATE */

  var stmtN = 0
  var rs: ResultSet = null
  var classMeta: Vector[Class[_]] = null
  var isDone = false


  /* BEHAVIOUR */

  def streaming(): Receive = {
    case Request(n) ⇒
      var i = n
      while (i > 0 && (if (rs.next()) true else { isDone = true; false })) {
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

      trace(log, ctx.traceId, RunInitializationStatements, Variation.Attempt,
        "running {} initialization statements", initializationStmts.size)
      initializationStmts.foreach { s ⇒
        stmt.execute(s.replace(";", ""))
      }
      trace(log, ctx.traceId, RunInitializationStatements,
        Variation.Success, "finished initialization statements")

      val statements = splitBatch(query)
      debug(log, "split query into {} statements", statements.size)

      if (statements.isEmpty) {
        terminate(DoneWithQueryExecution.error(new Exception("nothing to run")))
      } else if (statements.foldLeft(true)((acc, u) ⇒ { val isResultSet = execute(u); acc && isResultSet })) {
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
            (columns :+ ((rsmd.getColumnLabel(i), typeHint)), met :+ clazz)
        }
        classMeta = m._2
        context.parent ! TypeMetadata(m._1)
        self ! Request(n - 1L)
        context.become(streaming())
      } else {
        if (canWrite && !conn.getAutoCommit) {
          debug(log, "user has write access and auto-commit is false. Committing now...")
          conn.commit()
        } else if (!canWrite && !conn.getAutoCommit){
          warning(log, "user tried to run update statements but this token doesn't grant write access")
        } else if (canWrite){
          debug(log, "user has write access and auto-commit was true")
        } else if (!canWrite){
          log.error("user ran update statements successfully without write access")
        }
        context.parent ! TypeMetadata(Vector.empty) //n at least will be 1
        if (n - 1 > 0) terminate(DoneWithQueryExecution.success)
        else context.become({
          case r: Request ⇒ terminate(DoneWithQueryExecution.success)
        })
      }
  }
}

class JdbcConnectionsHandler extends Actor with SonicdLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    info(log, "starting jdbc connection handler")
  }

  override def receive: Actor.Receive = {
    case JdbcHandle(conn, stmt) ⇒
      try {
        stmt.close()
        debug(log, "closed statement {} of connection {}", stmt, conn)
      } catch {
        case e: Exception ⇒
      }

      try {
        conn.close()
        debug(log, "closed connection {}", conn)
      } catch {
        case e: Exception ⇒
      }

    case cmd@JdbcConnectionsHandler.GetJdbcHandle(isQuery, driver, url, user, password) ⇒
      try {
        val conn: Connection = {
          //register driver
          Class.forName(driver)
          debug(log, "registered driver {}", driver)
          val c = DriverManager.getConnection(url, user, password)
          debug(log, "created new connection {}", c)
          try {
            c.setAutoCommit(false)
          } catch {
            case e: Exception ⇒
              if (c.getAutoCommit) {
                warning(log, "{} doesn't support setting auto-commit to false: {}", driver, e.getMessage)
              }
          }
          c
        }
        var stmt: Statement = null
        //try to set streaming properties for each driver
        try {
          if (driver == "org.postgresql.Driver" && isQuery) {
            debug(log, "setting streaming properties for PostgreSQL")
            stmt = conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.FETCH_FORWARD
            )
            stmt.setFetchSize(SonicdConfig.JDBC_FETCHSIZE)
          } else if (driver == "com.mysql.jdbc.Driver" && isQuery) {
            debug(log, "setting streaming properties for MySQL")
            stmt = conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY
            )
            stmt.setFetchSize(Integer.MIN_VALUE)
          } else if (isQuery) {
            debug(log, "setting streaming properties for driver")
            stmt = conn.createStatement()
            stmt.setFetchSize(SonicdConfig.JDBC_FETCHSIZE)
          } else {
            stmt = conn.createStatement()
          }
          debug(log, "successfully set streaming properties")
        } catch {
          case e: Exception ⇒
            warning(log, "could not set streaming properties for driver '{}'", driver)
            stmt = conn.createStatement()
        }
        sender() ! JdbcHandle(conn, stmt)
      } catch {
        case e: Exception ⇒
          error(log, e, "error when preparing connection/statement")
          sender() ! DoneWithQueryExecution.error(e)
      }
  }
}

object JdbcConnectionsHandler {
  val actorName = "jdbconn"

  case class GetJdbcHandle(isQuery: Boolean, driver: String, url: String, user: String, password: String)

  case class JdbcHandle(conn: Connection, stmt: Statement)

}
