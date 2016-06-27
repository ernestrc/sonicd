package build.unstable.sonicd.source

import java.io.IOException

import akka.actor._
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.pattern.pipe
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.ZuoraService._
import build.unstable.sonicd.{Sonicd, SonicdConfig}
import spray.json._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import scala.xml.parsing.XhtmlParser

class ZuoraObjectQueryLanguageSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends DataSource(config, queryId, query, context) {

  val MIN_RECORDS = 100

  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))(context)

  def newConnectionPool(host: String): ConnectionPool = {
    Sonicd.http.newHostConnectionPoolHttps[String](host = host,
          settings = ConnectionPoolSettings(SonicdConfig.ZUORA_CONNECTION_POOL_SETTINGS),
        connectionContext = ConnectionContext.https(sslContext = Sonicd.sslContext,
          enabledProtocols = Some(scala.collection.immutable.Vector("TLSv1.2")), //zuora only allows TLSv1.2 and TLSv.1.1
          sslParameters = Some(Sonicd.sslContext.getDefaultSSLParameters)))
  }

  override lazy val handlerProps: Props = {
    val user: String = getConfig[String]("username")
    val password: String = getConfig[String]("password")
    val host: String = getConfig[String]("host")
    val batchSize: Int =
      getOption[Int]("batch-size").map { i ⇒
        if (i > SonicdConfig.ZUORA_MAX_NUMBER_RECORDS || i <= MIN_RECORDS)
          throw new Exception(s"'batch-size' must be between ${SonicdConfig.ZUORA_MAX_NUMBER_RECORDS} and $MIN_RECORDS")
        i
      }.getOrElse(SonicdConfig.ZUORA_MAX_NUMBER_RECORDS)

    val auth = ZuoraAuth(user, password, host)
    val zuoraServiceActorName = ZuoraService.getZuoraServiceActorName(auth)

    val zuoraService = context.child(zuoraServiceActorName).getOrElse {
      val pool: ConnectionPool = newConnectionPool(auth.host)
      context.actorOf(Props(classOf[ZuoraService], pool, materializer), zuoraServiceActorName)
    }

    Props(classOf[ZOQLPublisher], query, queryId, zuoraService, auth, batchSize)
  }
}

class ZOQLPublisher(query: String, queryId: String, service: ActorRef, auth: ZuoraAuth, batchSize: Int)
  extends ActorPublisher[SonicMessage] with ActorLogging {

  import ZuoraObjectQueryLanguageSource._
  import akka.stream.actor.ActorPublisherMessage._

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"starting ZOQLPublisher of '$queryId'")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug(s"stopping ZOQLPublisher of '$queryId'")
  }

  // STATE
  val buffer = scala.collection.mutable.Queue.empty[SonicMessage]
  var streamed = 0
  var effectiveBatchSize: Int = batchSize //can be overriden if query has limit
  var metaSent = false

  // HELPERS
  def stream() {
    while (buffer.nonEmpty && totalDemand > 0) {
      onNext(buffer.dequeue())
    }
  }

  def zObjectsToOutputChunk(fields: Vector[String], v: Vector[ZuoraService.RawZObject]): Vector[SonicMessage] =
    Try {
      val buf: ListBuffer[SonicMessage] = if (!metaSent) {
        metaSent = true
        ListBuffer(TypeMetadata(fields.map(_ → JsString.empty)))
      } else ListBuffer.empty[SonicMessage]
      // unfortunately zuora doesn't give us any type information at runtime
      // the only way to pass type information would be to hardcode types in the table describes
       v.foreach { n ⇒
        val child = n.xml.child
        val values = fields.map(k ⇒ child.find(_.label equalsIgnoreCase k).map(_.text).getOrElse(""))
        buf.append(OutputChunk(values))
      }
      buf.to[Vector]
    }.recover {
      case e: Exception ⇒
        throw new Exception(s"could not parse ZObject: ${e.getMessage}", e)
    }.get

  // RECEIVE
  def streaming(streamLimit: Option[Int], completeBufEmpty: Boolean, zoql: String, colNames: Vector[String]): Receive = {

    case Request(n) ⇒
      stream()
      if (completeBufEmpty && buffer.isEmpty) {
        onCompleteThenStop()
      }

    case res: QueryResult ⇒
      val totalSize = res.size

      if (res.done || streamLimit.isDefined && { streamed += effectiveBatchSize; streamed == streamLimit.get }) {
        log.info(s"successfully fetched $totalSize zuora objects")
        self ! DoneWithQueryExecution(success = true)
      } else {
        val percPerBatch = 100.0 * effectiveBatchSize / totalSize
        buffer.enqueue(QueryProgress(Some(percPerBatch), Some(s"querying for $effectiveBatchSize more objects")))

        //query-ahead
        if (buffer.size < effectiveBatchSize * 5) {
          service ! RunQueryMore(QueryMore(zoql, res.queryLocator.get, queryId), effectiveBatchSize, auth)
          log.debug("querying ahead, buffer size is {}", buffer)
        }
      }

      try {
        val toQueue =
          if (streamLimit.isDefined) res.records.slice(0, streamLimit.get)
          else res.records
        buffer.enqueue(zObjectsToOutputChunk(colNames, toQueue): _*)
      } catch {
        case e: Exception ⇒
          log.error(e, "error when building output chunks")
          self ! DoneWithQueryExecution.error(e)
      }
      stream()

    case res: ZuoraService#QueryFailed ⇒
      self ! DoneWithQueryExecution.error(res.error)

    case r: DoneWithQueryExecution ⇒
      if (totalDemand > 0) {
        onNext(r)
        onCompleteThenStop()
      } else {
        buffer.enqueue(r)
        context.become(streaming(streamLimit, completeBufEmpty = true, zoql, colNames))
      }

    case Cancel ⇒
      log.debug("client canceled")
      onCompleteThenStop()
  }

  override def receive: Receive = {

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case r@Request(n) ⇒
      val trim = query.trim().toLowerCase
      lazy val nothing = (true, Vector.empty, None)
      val (isComplete, colNames, limit): (Boolean, Vector[String], Option[Int]) =
        if (trim.startsWith("show")) {
          log.debug("showing table names")
          buffer.enqueue(ZuoraService.ShowTables.output: _*)
          buffer.enqueue(DoneWithQueryExecution(success = true))
          nothing
        } else if (trim.startsWith("desc") || trim.startsWith("describe")) {
          log.debug("describing table {}", trim)
          query.split(" ").lastOption.map { parsed ⇒
            ZuoraService.tables.find(t ⇒ t.name == parsed || t.nameLower == parsed)
              .map { table ⇒
                val msgs = table.description.map(s ⇒ OutputChunk.apply(Vector(s))) :+ DoneWithQueryExecution(success = true)
                buffer.enqueue(msgs: _*)
                nothing
              }.getOrElse {
              buffer.enqueue(DoneWithQueryExecution.error(new Exception(s"table '$parsed' not found")))
              nothing
            }
          }.getOrElse {
            buffer.enqueue(DoneWithQueryExecution.error(new Exception(s"error parsing $query")))
            nothing
          }
        } else {
          log.debug("running query with id '{}'", queryId)
          val lim: Option[Int] =
            Try(ZuoraService.LIMIT.findFirstMatchIn(query).map(_.group("lim").toInt)) match {
              case Success(Some(i)) ⇒
                log.debug("parsed query limit {}", i)
                effectiveBatchSize = Math.min(i, batchSize)
                Some(i)
              case _ ⇒ None
            }

          val col = extractSelectColumnNames(query)
          log.debug("extracted column names: {}", col)

          service ! ZuoraService.RunZOQLQuery(queryId, query, effectiveBatchSize, auth)

          (false, col, lim)
        }

      self ! r
      context.become(streaming(limit, completeBufEmpty = isComplete, query, colNames))

    case Cancel ⇒
      log.debug("client cancelled")
      onCompleteThenStop()
  }
}

object ZuoraObjectQueryLanguageSource {
  val COLR = "(?i)(?<=select)(\\s*)(\\s*\\w\\s*,?)*(?=from\\s*(\\w*))".r

  def extractSelectColumnNames(sql: String): Vector[String] =
    COLR.findAllIn(sql)
      .matchData.toVector.headOption
      .map(_.group(0).split(',').map(_.trim).toVector)
      .getOrElse(Vector.empty)
}

class ZuoraService(implicit connectionPool: ConnectionPool, materializer: ActorMaterializer) extends Actor with ActorLogging {

  import ZuoraService._
  import context.dispatcher

  case class VoidSession(auth: ZuoraAuth)

  case class QueryFailed(error: Throwable)

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"starting ZuoraService actor")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug(s"stopping ZuoraService actor")
  }

  val validSessions = scala.collection.mutable.Map.empty[ZuoraAuth, Future[Session]]

  def memoizedSession(auth: ZuoraAuth): Future[Session] =
    validSessions.getOrElse(auth, {
      val h = getLogin(auth)
      context.system.scheduler.scheduleOnce(10.minutes, self, VoidSession(auth))
      validSessions.update(auth, h)
      h
    })

  def xmlRequest(payload: scala.xml.Node, queryId: String, pool: ConnectionPool, auth: ZuoraAuth): Future[HttpResponse] = Future {
    val data = payload.buildString(true)
    HttpRequest(
      method = HttpMethods.POST,
      uri = SonicdConfig.ZUORA_ENDPOINT,
      entity = HttpEntity.Strict(ContentTypes.`text/xml(UTF-8)`, ByteString.fromString(data))
    )
  }.flatMap { request ⇒
    Source.single(request → queryId).via(pool).runWith(Sink.head).flatMap {
      case (Success(response), _) if response.status.isSuccess() =>
        log.debug("http req query {} is successful", queryId)
        Future.successful(response)
      case (Success(response), _) ⇒
        log.debug("http query {} failed", queryId)
        val parsed = response.entity.toStrict(10.seconds)
        parsed.recoverWith {
          case e: Exception ⇒
            val error = new IOException(s"request failed with status ${response.status}")
            log.error(error, s"unsuccessful response from server")
            Future.failed(error)
        }
        parsed.flatMap { en ⇒
          val entity = (XhtmlParser(scala.io.Source.fromString(en.data.utf8String)) \\ "FaultMessage").text
          val error = new IOException(s"request failed with status ${response.status} and error: $entity")
          log.error(error, s"unsuccessful response from server: $entity")
          if (entity == "invalid session") {
            self ! VoidSession(auth)
          }
          Future.failed(error)
        }
      case (Failure(e), _) ⇒ Future.failed(e)
    }
  }

  var loginN: Int = 0

  def getLogin(auth: ZuoraAuth): Future[Session] = {
    loginN += 1
    log.debug("trying to login for the {} time", loginN)
    xmlRequest(auth.xml, loginN.toString, connectionPool, auth)
      .flatMap(r ⇒ Session.fromHttpEntity(r.entity, auth.host))
  }

  def runQueryMore(qMore: QueryMore, auth: ZuoraAuth, batchSize: Int)(sessionHeader: Session): Future[QueryResult] = {
    val xml = qMore.xml(batchSize, sessionHeader.id)
    xmlRequest(xml, qMore.queryId, connectionPool, auth)
      .flatMap(r ⇒ QueryResult.fromHttpEntity(r.entity))
  }

  def runQuery(queryId: String, zoql: String, batchSize: Int, auth: ZuoraAuth)(sessionHeader: Session): Future[QueryResult] = {
    log.debug("running query '{}': {}", queryId, zoql)

    val q = FirstQuery(zoql, queryId)
    val xml = q.xml(batchSize, sessionHeader.id)

    xmlRequest(xml, q.queryId, connectionPool, auth)
      .flatMap(r ⇒ QueryResult.fromHttpEntity(r.entity))
  }

  override def receive: Actor.Receive = {

    case VoidSession(auth) ⇒
      log.info(s"voiding last zuora header $validSessions")
      validSessions.remove(auth)

    case RunQueryMore(queryMore, batchSize, auth) ⇒
      memoizedSession(auth)
        .flatMap(runQueryMore(queryMore, auth, batchSize))
        .recover {
          case e: Exception ⇒ QueryFailed(e)
        } pipeTo sender()

    case RunZOQLQuery(queryId, zoql, batchSize, auth) ⇒
      memoizedSession(auth)
        .flatMap(runQuery(queryId, zoql, batchSize, auth))
        .recover {
          case e: Exception ⇒ QueryFailed(e)
        } pipeTo sender()
  }

}

object ZuoraService {

  type ConnectionPool = Flow[(HttpRequest, String), (Try[HttpResponse], String), Http.HostConnectionPool]

  case class Session(id: String, host: String)

  object Session {

    def fromHttpEntity(entity: HttpEntity, host: String)(implicit mat: ActorMaterializer, ctx: ExecutionContext): Future[Session] = {
      entity.toStrict(10.seconds).map { e ⇒
        val xml = e.data.decodeString("UTF-8")
        val elem = XhtmlParser.apply(scala.io.Source.fromString(xml))
        val id = elem \\ "Session"
        if (id == null || id.text == null || id.text == "") {
          throw new Exception(s"protocol error: session is empty: $id")
        }
        Session(id.text, host)
      }
    }
  }

  case class ZuoraAuth(user: String, pwd: String, host: String) {

    override def toString: String = s"Auth($host)"

    val xml: scala.xml.Node = {
      <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns2="http://object.api.zuora.com/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ns1="http://api.zuora.com/">
        <SOAP-ENV:Body>
          <ns1:login>
            <ns1:username>{user}</ns1:username>
            <ns1:password>{pwd}</ns1:password>
          </ns1:login>
        </SOAP-ENV:Body>
      </SOAP-ENV:Envelope>
    }
  }

  trait Query {
    val queryId: String
    val zoql: String

    def xml(batchSize: Int, session: String): scala.xml.Node
  }

  case class FirstQuery(zoql: String, queryId: String) extends Query {
    def xml(batchSize: Int, session: String): scala.xml.Node = {
      <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns2="http://object.api.zuora.com/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ns1="http://api.zuora.com/">
        <SOAP-ENV:Header>
          <ns2:SessionHeader>
            <ns2:session>{session}</ns2:session>
          </ns2:SessionHeader>
          <ns2:QueryOptions>
            <ns2:batchSize>{batchSize}</ns2:batchSize>
          </ns2:QueryOptions>
        </SOAP-ENV:Header>
        <SOAP-ENV:Body>
          <ns1:query>
            <ns1:queryString>{zoql}</ns1:queryString>
          </ns1:query>
        </SOAP-ENV:Body>
      </SOAP-ENV:Envelope>
    }
  }

  case class QueryMore(zoql: String, queryLocator: String, queryId: String) extends Query {
    def xml(batchSize: Int, session: String): scala.xml.Node = {
      <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns2="http://object.api.zuora.com/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ns1="http://api.zuora.com/">
        <SOAP-ENV:Header>
          <ns2:SessionHeader>
            <ns2:session>{session}</ns2:session>
          </ns2:SessionHeader> <ns2:QueryOptions>
          <ns2:batchSize>{batchSize}</ns2:batchSize>
        </ns2:QueryOptions>
        </SOAP-ENV:Header> <SOAP-ENV:Body>
        <ns1:queryMore>
          <ns1:queryLocator>{queryLocator}</ns1:queryLocator>
        </ns1:queryMore>
      </SOAP-ENV:Body>
      </SOAP-ENV:Envelope>
    }
  }

  case class RawZObject(xml: scala.xml.Node)

  case class QueryResult(size: Int, done: Boolean, queryLocator: Option[String], records: Vector[RawZObject])

  object QueryResult {
    def fromHttpEntity(entity: HttpEntity)(implicit mat: ActorMaterializer, ctx: ExecutionContext): Future[QueryResult] = {
      entity.toStrict(SonicdConfig.ZUORA_HTTP_ENTITY_TIMEOUT).map { e ⇒
        val xml = e.data.decodeString("UTF-8")
        val elem = XhtmlParser.apply(scala.io.Source.fromString(xml))
        val size = (elem \\ "size").head.text.toInt
        val done = (elem \\ "done").head.text.toBoolean
        val queryLocator = (elem \\ "queryLocator").headOption.map(_.text)
        val records =
          if (size == 0) Vector.empty
          else (elem \\ "records").map(RawZObject.apply).toVector
        QueryResult(size, done, queryLocator, records)
      }
    }
  }

  def getZuoraServiceActorName(auth: ZuoraAuth) = s"zuora_service_${auth.host}"

  //https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E_SOAP_API_Calls/query_call
  val MAX_NUMBER_RECORDS = 2000

  case class RunZOQLQuery(queryId: String, zoql: String, batchSize: Int, user: ZuoraAuth)

  case class RunQueryMore(q: QueryMore, batchSize: Int, auth: ZuoraAuth)

  sealed abstract class Table(desc: Vector[String]) {
    val name = this.getClass.getSimpleName.dropRight(1)
    val nameLower = name.toLowerCase()
    val description = desc :+ s"More info at https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E1_SOAP_API_Object_Reference/$name"
  }

  case object Account extends Table("AccountNumber\nAdditionalEmailAddresses\nAllowInvoiceEdit\nAutoPay\nBalance\nBatch\nBcdSettingOption\nBillCycleDay\nBillTold\nCommunicationProfileId\nCreateById\nCreatedDate\nCreditBalance\nCrmId\nCurrency\nDefaultPaymentMethodId\nGateway\nId\nInvoiceDeliveryPrefsEmail\nInvoiceDeliveryPrefsPrint\nInvoiceTemplateId\nLastInvoiceDate\nName\nNotes\nParentId\nPaymentGateway\nPaymentTerm\nPurchaseOrderNumber\nSalesRepName\nSoldTold\nStatus\nTaxCompanyCode\nTaxExemptCertificateID\nTaxExemptCertificateType\nTaxExemptDescription\nTaxExemptEffectiveDate\nTaxExemptExpirationDate\nTaxExemptIssuingJurisdiction\nTaxExemptStatus\nTotalInvoiceBalance\nUpdatedById\nUpdatedDate\nVATId".split('\n').toVector)

  case object AccountingPeriod extends Table(Vector.empty)

  case object Amendment extends Table(Vector.empty)

  case object CommunicationProfile extends Table(Vector.empty)

  case object Contact extends Table(Vector.empty)

  case object Import extends Table(Vector.empty)

  case object Invoice extends Table(Vector.empty)

  case object InvoiceAdjustment extends Table(Vector.empty)

  case object InvoiceItem extends Table(Vector.empty)

  case object InvoiceItemAdjustment extends Table(Vector.empty)

  case object InvoicePayment extends Table(Vector.empty)

  case object Payment extends Table(Vector.empty)

  case object PaymentMethod extends Table(Vector.empty)

  case object Product extends Table(Vector.empty)

  case object ProductRatePlan extends Table(Vector.empty)

  case object ProductRatePlanCharge extends Table(Vector.empty)

  case object ProductRatePlanChargeTier extends Table(Vector.empty)

  case object RatePlan extends Table(Vector.empty)

  case object RatePlanCharge extends Table(Vector.empty)

  case object RatePlanChargeTier extends Table(Vector.empty)

  case object Refund extends Table(Vector.empty)

  case object Subscription extends Table(Vector.empty)

  case object TaxationItem extends Table(Vector.empty)

  case object Usage extends Table(Vector.empty)

  import scala.reflect.runtime.universe._

  val mirror = runtimeMirror(this.getClass.getClassLoader)

  val tables: Vector[Table] = {
    val symbol = typeOf[Table].typeSymbol
    val internal = symbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol]
    (internal.sealedDescendants.map(_.asInstanceOf[Symbol]) - symbol)
      .map(t ⇒ mirror.runtimeClass(t.asClass).getConstructors()(0).newInstance().asInstanceOf[Table])
      .toVector
  }

  case object ShowTables {
    val output: Vector[OutputChunk] = tables.map(t ⇒ OutputChunk(Vector(t.name)))
  }

  val LIMIT = new Regex(".*LIMIT|limit ([0-9]*)", "lim")

}
