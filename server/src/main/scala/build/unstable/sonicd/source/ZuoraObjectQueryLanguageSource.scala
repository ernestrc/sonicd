package build.unstable.sonicd.source

import java.io.IOException

import akka.actor._
import akka.http.scaladsl.ConnectionContext
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.{Sonic, SonicConfig}
import spray.json.{JsString, JsBoolean, JsObject}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import scala.xml.parsing.XhtmlParser

class ZuoraObjectQueryLanguageSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends DataSource(config, queryId, query, context) {

  override lazy val handlerProps: Props = {
    val service = context.child(ZuoraService.actorName).getOrElse {
      context.actorOf(Props[ZuoraService], ZuoraService.actorName)
    }
    Props(classOf[ZOQLPublisher], query, queryId, service)
  }
}

class ZOQLPublisher(query: String, queryId: String, service: ActorRef)
  extends ActorPublisher[SonicMessage] with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._
  import ZuoraObjectQueryLanguageSource._

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"starting ZOQLPublisher of '$queryId'")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug(s"stopping ZOQLPublisher of '$queryId'")
  }

  def zObjectsToOutputChunk(fields: Vector[String], v: Vector[ZuoraService.RawZObject]): Vector[SonicMessage] =
    Try {
      // unfortunately zuora doesn't give us any type information at runtime
      // the only way to pass type information would be to hardcode types in the table describes
      val meta = TypeMetadata(fields.map(_ → JsString.empty))
      Vector(meta) ++ v.map { n ⇒
        val child = n.xml.child
        val values = fields.map(k ⇒ child.find(_.label equalsIgnoreCase k).map(_.text).getOrElse(""))
        OutputChunk(values)
      }
    }.recover {
      case e: Exception ⇒
        throw new Exception(s"could not parse ZObject: ${e.getMessage}", e)
    }.get

  def running(buffer: Vector[SonicMessage], completeBufEmpty: Boolean): Receive = {
    case Request(n) ⇒
      val buf = ListBuffer.empty ++ buffer
      while (buf.nonEmpty && totalDemand > 0) {
        onNext(buf.remove(0))
      }
      if (completeBufEmpty && buf.isEmpty) {
        onCompleteThenStop()
      } else context.become(running(buf.toVector, completeBufEmpty))

    case more: ZuoraService#QueryingForMore ⇒
      log.debug("received query progress {}", more)
      val msg = QueryProgress(Some(more.percCompletedLastIter), None)
      if (totalDemand > 0) {
        log.debug("sending progress downstream {}", msg)
        onNext(msg)
      } else context.become(running(buffer :+ msg, completeBufEmpty = false))

    case res: ZuoraService#Records ⇒
      try {
        val size = res.size
        log.info(s"successfully executed query and returned $size zuora objects")
        val buf = ListBuffer.empty[SonicMessage] ++ buffer

        val colNames = extractSelectColumnNames(query)
        buf.appendAll(zObjectsToOutputChunk(colNames, res.records))
        buf.append(DoneWithQueryExecution(success = true))

        context.become(running(buf.toVector, completeBufEmpty = true))
        self ! Request(totalDemand)
      } catch {
        case e: Exception ⇒
          log.error(e, "error when building output chunks")
          self ! DoneWithQueryExecution.error(e)
      }

    case res: ZuoraService#QueryFailed ⇒
      self ! DoneWithQueryExecution.error(res.error)

    case r: DoneWithQueryExecution ⇒
      if (totalDemand > 0) {
        onNext(r)
        onCompleteThenStop()
      } else context.become(running(Vector(r), completeBufEmpty = true))

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
      val (init, complete): (Vector[SonicMessage], Boolean) =
        if (trim.startsWith("show")) {
          log.debug("showing table names")
          (ZuoraService.ShowTables.output :+ DoneWithQueryExecution(success = true)) → true
        } else if (trim.startsWith("desc") || trim.startsWith("describe")) {
          log.debug("describing table {}", trim)
          query.split(" ").lastOption.map { parsed ⇒
            ZuoraService.tables.find(t ⇒ t.name == parsed || t.nameLower == parsed)
              .map { table ⇒
                Vector(OutputChunk(Vector(table.description)), DoneWithQueryExecution(success = true)) → true
              }
              .getOrElse {
                val done = DoneWithQueryExecution.error(new Exception(s"table '$parsed' not found"))
                Vector(done) → true
              }
          }.getOrElse {
            val done = DoneWithQueryExecution.error(new Exception(s"error parsing $query"))
            Vector(done) → true
          }
        } else {
          log.debug("running query with id '{}'", queryId)
          val limit: Option[Int] =
            Try(ZuoraService.LIMIT.findFirstMatchIn(query).map(_.group("lim").toInt)) match {
              case Success(i) ⇒ i
              case Failure(e) ⇒
                log.warning(s"could not parse limit: $query")
                None
            }
          service ! ZuoraService.RunZOQLQuery(queryId, query, limit)
          Vector.empty → false
        }
      self ! r
      context.become(running(init, completeBufEmpty = complete))

    case Cancel ⇒
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

class ZuoraService extends Actor with ActorLogging {

  import ZuoraService._
  import context.dispatcher

  case object VoidLogin

  case class Records(size: Int, records: Vector[RawZObject])

  case class QueryingForMore(percCompletedLastIter: Int)

  case class QueryFailed(error: Throwable)

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"starting ZuoraService actor")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug(s"stopping ZuoraService actor")
  }

  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  lazy val connectionPool = Sonic.http.newHostConnectionPoolHttps[String](
    host = SonicConfig.ZUORA_HOST,
    settings = ConnectionPoolSettings(SonicConfig.ZUORA_CONNECTION_POOL_SETTINGS),
    connectionContext = ConnectionContext.https(
      sslContext = Sonic.sslContext,
      enabledProtocols = Some(scala.collection.immutable.Vector("TLSv1.2")), //zuora only allows TLSv1.2 and TLSv.1.1
      sslParameters = Some(Sonic.sslContext.getDefaultSSLParameters)
    )
  )

  def xmlRequest(payload: scala.xml.Node, queryId: String): Future[HttpResponse] = Future {
    val data = payload.buildString(true)
    HttpRequest(
      method = HttpMethods.POST,
      uri = SonicConfig.ZUORA_ENDPOINT,
      entity = HttpEntity.Strict(ContentTypes.`text/xml(UTF-8)`, ByteString.fromString(data))
    )
  }.flatMap { request ⇒
    Source.single(request → queryId).via(connectionPool).runWith(Sink.head).flatMap {
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
          Future.failed(error)
        }
      case (Failure(e), _) ⇒ Future.failed(e)
    }
  }

  def query(q: Query,
            ses: Session,
            batchSize: Int,
            buf: Vector[RawZObject] = Vector.empty): Future[QueryResult] =
    xmlRequest(q.xml(batchSize, ses.id), q.queryId).flatMap(r ⇒ QueryResult.fromHttpEntity(r.entity))


  var loginN: Int = 0

  def login: Future[Session] = {
    loginN += 1
    log.debug("trying to login for the {} time", loginN)
    xmlRequest(Login.xml, loginN.toString).flatMap(r ⇒ Session.fromHttpEntity(r.entity))
  }

  //BLOCKING: we want to serialize all calls to zuora
  def runQuery(queryId: String, zoql: String, requester: ActorRef, limit: Option[Int])
              (sessionHeader: Session): Try[Vector[RawZObject]] = Try {
    val q = FirstQuery(zoql, queryId)
    log.debug("running query {}: {}", queryId, zoql)

    var lastQuery: QueryResult = Await.result(query(q, sessionHeader,
      limit.getOrElse(SonicConfig.ZUORA_MAX_NUMBER_RECORDS)), SonicConfig.ZUORA_QUERY_TIMEOUT)
    val items = ListBuffer.empty ++ lastQuery.records
    var batches = 1
    val totalSize: Int = lastQuery.size
    log.debug("query finished successfully, result total size is {}", totalSize)

    lazy val percPerBatch = 100 * SonicConfig.ZUORA_MAX_NUMBER_RECORDS / totalSize

    while (!lastQuery.done && limit.isEmpty) {

      batches += 1

      log.debug("{} querying for more batch {}; every batch completes {}", queryId, batches, percPerBatch)
      requester ! QueryingForMore(percPerBatch)

      val queryMore = QueryMore(zoql, lastQuery.queryLocator.get, queryId)

      lastQuery = Await.result(query(queryMore, sessionHeader,
        SonicConfig.ZUORA_MAX_NUMBER_RECORDS), SonicConfig.ZUORA_QUERY_TIMEOUT)
      items.appendAll(lastQuery.records)
    }

    items.toVector
  } match {
    case s: Success[_] ⇒ s
    case f @ Failure(e) ⇒
      val msg = "ZOQL query failed: " + e.getMessage
      log.error(e, msg)
      f
  }

  var lastHeader: Option[Session] = None

  //BLOCKING: we want to serialize all calls to zuora
  def memoizedHeader: Try[Session] = {

    def getNewHeader: Try[Session] = {

      Try(Await.result(login, SonicConfig.ZUORA_QUERY_TIMEOUT)) match {
        case t@Success(s) ⇒
          log.debug("successfully generated zuora session header {}", s.id)
          //void login after 10 minutes
          lastHeader = Some(s)
          context.system.scheduler.scheduleOnce(10.minutes, self, VoidLogin)
          t
        case t@Failure(e) ⇒
          val msg = "zuora login failed: " + e.getMessage
          log.error(e, msg)
          Failure(new Exception(msg, e))
      }
    }

    lastHeader.map(Success.apply).getOrElse(getNewHeader)
  }

  override def receive: Actor.Receive = {

    case VoidLogin ⇒
      log.info(s"voiding last zuora header $lastHeader")
      lastHeader = None

    case RunZOQLQuery(queryId, zoql, limit) ⇒
      val requester = sender()
      memoizedHeader
        .flatMap(runQuery(queryId, zoql, requester, limit))
        .map(res ⇒ requester ! Records(res.size, res))
        .recover {
          case e: Exception ⇒ sender() ! QueryFailed(e)
        }
  }

}

object ZuoraService {

  case class Session(id: String)

  object Session {

    def fromHttpEntity(entity: HttpEntity)(implicit mat: ActorMaterializer, ctx: ExecutionContext): Future[Session] = {
      entity.toStrict(10.seconds).map { e ⇒
        val xml = e.data.decodeString("UTF-8")
        val elem = XhtmlParser.apply(scala.io.Source.fromString(xml))
        val id = elem \\ "Session"
        if (id == null || id.text == null || id.text == "") {
          throw new Exception(s"protocol error: session is empty: $id")
        }
        Session(id.text)
      }
    }
  }

  case object Login {
    val xml: scala.xml.Node = {
      <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns2="http://object.api.zuora.com/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ns1="http://api.zuora.com/">
        <SOAP-ENV:Body>
          <ns1:login>
            <ns1:username>{SonicConfig.ZUORA_USERNAME}</ns1:username>
            <ns1:password>{SonicConfig.ZUORA_PASSWORD}</ns1:password>
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
      entity.toStrict(10.seconds).map { e ⇒
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

  val actorName = "zuoraservice"

  //https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E_SOAP_API_Calls/query_call
  val MAX_NUMBER_RECORDS = 2000

  case class RunZOQLQuery(queryId: String, zoql: String, limit: Option[Int])

  sealed abstract class Table(desc: Vector[String]) {
    val name = this.getClass.getSimpleName.dropRight(1)
    val nameLower = name.toLowerCase()
    val description = s"$desc\nMore info at https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E1_SOAP_API_Object_Reference/$name"
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
    val symbol = typeOf[Table] .typeSymbol
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
