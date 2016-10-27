package build.unstable.sonicd.service.source

import akka.actor.{ActorSystem, Props}
import akka.stream.actor.ActorPublisher
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic.{Query, RequestContext}
import build.unstable.sonicd.model.{ImplicitSubscriber, TestController}
import build.unstable.sonicd.source.{ZOQLPublisher, ZuoraObjectQueryLanguageSource}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._

class ZOQLSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
    with Matchers with BeforeAndAfterAll with ImplicitSender
    with ImplicitSubscriber {

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def this() = this(ActorSystem("ZOQLSpec"))

  val config = """{ "class" : "ZuoraObjectQueryLanguageSource" }""".parseJson.asJsObject

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).withDispatcher(CallingThreadDispatcher.Id))

  def newPublisher(query: String): TestActorRef[ZOQLPublisher] = {
    val src = new ZuoraObjectQueryLanguageSource(Query("", config, "test", None),
      controller.underlyingActor.context, RequestContext("", None))
    val ref = TestActorRef[ZOQLPublisher](src.publisher.withDispatcher(CallingThreadDispatcher.Id))
    ActorPublisher(ref).subscribe(subs)
    ref
  }

  "ZuoraObjectQueryLanguageSource" should {
    "extract fields from query" in {
      val q = "select a,b,c from usage;"
      val q2 = "SELECT   fish ,  b, C     FROM usage where nonesense is true;"
      ZuoraObjectQueryLanguageSource
        .extractSelectColumnNames(q) should contain theSameElementsAs Seq("a", "b", "c")
      ZuoraObjectQueryLanguageSource
        .extractSelectColumnNames(q2) should contain theSameElementsAs Seq("fish", "b", "C")

      // test java.lang.StackOverflowError
      val q3 =
        """select
          AccountId, AchAbaCode, AchAccountName, AchAccountNumberMask, AchAccountType, AchAddress1, AchAddress2, AchBankName, Active, BankBranchCode, BankCheckDigit, BankCity, BankCode, BankIdentificationNumber, BankName, BankPostalCode, BankStreetName, BankStreetNumber, BankTransferAccountName, BankTransferAccountNumberMask, BankTransferAccountType, BankTransferType, BusinessIdentificationCode, City, Country, CreatedById, CreatedDate, CreditCardAddress1, CreditCardAddress2, CreditCardCity, CreditCardCountry, CreditCardExpirationMonth, CreditCardExpirationYear, CreditCardHolderName, CreditCardMaskNumber, CreditCardState, CreditCardType, DeviceSessionId, Email, ExistingMandate, FirstName, IBAN, Id, IPAddress, LastFailedSaleTransactionDate, LastName, LastTransactionDateTime, LastTransactionStatus, MandateCreationDate, MandateID, MandateReceived, MandateUpdateDate, MaxConsecutivePaymentFailures, PaymentRetryWindow, PaypalBaid, PaypalEmail, PaypalPreapprovalKey, PaypalType,Phone, PostalCode, State, StreetName, StreetNumber, TotalNumberOfErrorPayments,TotalNumberOfProcessedPayments, Type, UpdatedById, UpdatedDate, UseDefaultRetryRule,
          AccountId, AchAbaCode, AchAccountName, AchAccountNumberMask, AchAccountType, AchAddress1, AchAddress2, AchBankName, Active, BankBranchCode, BankCheckDigit, BankCity, BankCode, BankIdentificationNumber, BankName, BankPostalCode, BankStreetName, BankStreetNumber, BankTransferAccountName, BankTransferAccountNumberMask, BankTransferAccountType, BankTransferType, BusinessIdentificationCode, City, Country, CreatedById, CreatedDate, CreditCardAddress1, CreditCardAddress2, CreditCardCity, CreditCardCountry, CreditCardExpirationMonth, CreditCardExpirationYear, CreditCardHolderName, CreditCardMaskNumber, CreditCardState, CreditCardType, DeviceSessionId, Email, ExistingMandate, FirstName, IBAN, Id, IPAddress, LastFailedSaleTransactionDate, LastName, LastTransactionDateTime, LastTransactionStatus, MandateCreationDate, MandateID, MandateReceived, MandateUpdateDate, MaxConsecutivePaymentFailures, PaymentRetryWindow, PaypalBaid, PaypalEmail, PaypalPreapprovalKey, PaypalType,Phone, PostalCode, State, StreetName, StreetNumber, TotalNumberOfErrorPayments,TotalNumberOfProcessedPayments, Type, UpdatedById, UpdatedDate, UseDefaultRetryRule,
          AccountId, AchAbaCode, AchAccountName, AchAccountNumberMask, AchAccountType, AchAddress1, AchAddress2, AchBankName, Active, BankBranchCode, BankCheckDigit, BankCity, BankCode, BankIdentificationNumber, BankName, BankPostalCode, BankStreetName, BankStreetNumber, BankTransferAccountName, BankTransferAccountNumberMask, BankTransferAccountType, BankTransferType, BusinessIdentificationCode, City, Country, CreatedById, CreatedDate, CreditCardAddress1, CreditCardAddress2, CreditCardCity, CreditCardCountry, CreditCardExpirationMonth, CreditCardExpirationYear, CreditCardHolderName, CreditCardMaskNumber, CreditCardState, CreditCardType, DeviceSessionId, Email, ExistingMandate, FirstName, IBAN, Id, IPAddress, LastFailedSaleTransactionDate, LastName, LastTransactionDateTime, LastTransactionStatus, MandateCreationDate, MandateID, MandateReceived, MandateUpdateDate, MaxConsecutivePaymentFailures, PaymentRetryWindow, PaypalBaid, PaypalEmail, PaypalPreapprovalKey, PaypalType,Phone, PostalCode, State, StreetName, StreetNumber, TotalNumberOfErrorPayments,TotalNumberOfProcessedPayments, Type, UpdatedById, UpdatedDate, UseDefaultRetryRule,
          AccountId, AchAbaCode, AchAccountName, AchAccountNumberMask, AchAccountType, AchAddress1, AchAddress2, AchBankName, Active, BankBranchCode, BankCheckDigit, BankCity, BankCode, BankIdentificationNumber, BankName, BankPostalCode, BankStreetName, BankStreetNumber, BankTransferAccountName, BankTransferAccountNumberMask, BankTransferAccountType, BankTransferType, BusinessIdentificationCode, City, Country, CreatedById, CreatedDate, CreditCardAddress1, CreditCardAddress2, CreditCardCity, CreditCardCountry, CreditCardExpirationMonth, CreditCardExpirationYear, CreditCardHolderName, CreditCardMaskNumber, CreditCardState, CreditCardType, DeviceSessionId, Email, ExistingMandate, FirstName, IBAN, Id, IPAddress, LastFailedSaleTransactionDate, LastName, LastTransactionDateTime, LastTransactionStatus, MandateCreationDate, MandateID, MandateReceived, MandateUpdateDate, MaxConsecutivePaymentFailures, PaymentRetryWindow, PaypalBaid, PaypalEmail, PaypalPreapprovalKey, PaypalType,Phone, PostalCode, State, StreetName, StreetNumber, TotalNumberOfErrorPayments,TotalNumberOfProcessedPayments, Type, UpdatedById, UpdatedDate, UseDefaultRetryRule
          from PaymentMethod"""

      ZuoraObjectQueryLanguageSource
        .extractSelectColumnNames(q3) should contain theSameElementsAs Seq(
        "AccountId", "AchAbaCode", "AchAccountName",
        "AchAccountNumberMask", "AchAccountType", "AchAddress1", "AchAddress2", "AchBankName", "Active", "BankBranchCode",
        "BankCheckDigit", "BankCity", "BankCode", "BankIdentificationNumber", "BankName", "BankPostalCode", "BankStreetName",
        "BankStreetNumber", "BankTransferAccountName", "BankTransferAccountNumberMask", "BankTransferAccountType",
        "BankTransferType", "BusinessIdentificationCode", "City", "Country", "CreatedById", "CreatedDate",
        "CreditCardAddress1", "CreditCardAddress2", "CreditCardCity", "CreditCardCountry", "CreditCardExpirationMonth",
        "CreditCardExpirationYear", "CreditCardHolderName", "CreditCardMaskNumber", "CreditCardState",
        "CreditCardType", "DeviceSessionId", "Email", "ExistingMandate", "FirstName", "IBAN", "Id", "IPAddress",
        "LastFailedSaleTransactionDate", "LastName", "LastTransactionDateTime", "LastTransactionStatus",
        "MandateCreationDate", "MandateID", "MandateReceived", "MandateUpdateDate", "MaxConsecutivePaymentFailures",
        "PaymentRetryWindow", "PaypalBaid", "PaypalEmail", "PaypalPreapprovalKey", "PaypalType", "Phone", "PostalCode",
        "State", "StreetName", "StreetNumber", "TotalNumberOfErrorPayments", "TotalNumberOfProcessedPayments", "Type",
        "UpdatedById", "UpdatedDate", "UseDefaultRetryRule",
        "AccountId", "AchAbaCode", "AchAccountName",
        "AchAccountNumberMask", "AchAccountType", "AchAddress1", "AchAddress2", "AchBankName", "Active", "BankBranchCode",
        "BankCheckDigit", "BankCity", "BankCode", "BankIdentificationNumber", "BankName", "BankPostalCode", "BankStreetName",
        "BankStreetNumber", "BankTransferAccountName", "BankTransferAccountNumberMask", "BankTransferAccountType",
        "BankTransferType", "BusinessIdentificationCode", "City", "Country", "CreatedById", "CreatedDate",
        "CreditCardAddress1", "CreditCardAddress2", "CreditCardCity", "CreditCardCountry", "CreditCardExpirationMonth",
        "CreditCardExpirationYear", "CreditCardHolderName", "CreditCardMaskNumber", "CreditCardState",
        "CreditCardType", "DeviceSessionId", "Email", "ExistingMandate", "FirstName", "IBAN", "Id", "IPAddress",
        "LastFailedSaleTransactionDate", "LastName", "LastTransactionDateTime", "LastTransactionStatus",
        "MandateCreationDate", "MandateID", "MandateReceived", "MandateUpdateDate", "MaxConsecutivePaymentFailures",
        "PaymentRetryWindow", "PaypalBaid", "PaypalEmail", "PaypalPreapprovalKey", "PaypalType", "Phone", "PostalCode",
        "State", "StreetName", "StreetNumber", "TotalNumberOfErrorPayments", "TotalNumberOfProcessedPayments", "Type",
        "UpdatedById", "UpdatedDate", "UseDefaultRetryRule",
        "AccountId", "AchAbaCode", "AchAccountName",
        "AchAccountNumberMask", "AchAccountType", "AchAddress1", "AchAddress2", "AchBankName", "Active", "BankBranchCode",
        "BankCheckDigit", "BankCity", "BankCode", "BankIdentificationNumber", "BankName", "BankPostalCode", "BankStreetName",
        "BankStreetNumber", "BankTransferAccountName", "BankTransferAccountNumberMask", "BankTransferAccountType",
        "BankTransferType", "BusinessIdentificationCode", "City", "Country", "CreatedById", "CreatedDate",
        "CreditCardAddress1", "CreditCardAddress2", "CreditCardCity", "CreditCardCountry", "CreditCardExpirationMonth",
        "CreditCardExpirationYear", "CreditCardHolderName", "CreditCardMaskNumber", "CreditCardState",
        "CreditCardType", "DeviceSessionId", "Email", "ExistingMandate", "FirstName", "IBAN", "Id", "IPAddress",
        "LastFailedSaleTransactionDate", "LastName", "LastTransactionDateTime", "LastTransactionStatus",
        "MandateCreationDate", "MandateID", "MandateReceived", "MandateUpdateDate", "MaxConsecutivePaymentFailures",
        "PaymentRetryWindow", "PaypalBaid", "PaypalEmail", "PaypalPreapprovalKey", "PaypalType", "Phone", "PostalCode",
        "State", "StreetName", "StreetNumber", "TotalNumberOfErrorPayments", "TotalNumberOfProcessedPayments", "Type",
        "UpdatedById", "UpdatedDate", "UseDefaultRetryRule",
        "AccountId", "AchAbaCode", "AchAccountName",
        "AchAccountNumberMask", "AchAccountType", "AchAddress1", "AchAddress2", "AchBankName", "Active", "BankBranchCode",
        "BankCheckDigit", "BankCity", "BankCode", "BankIdentificationNumber", "BankName", "BankPostalCode", "BankStreetName",
        "BankStreetNumber", "BankTransferAccountName", "BankTransferAccountNumberMask", "BankTransferAccountType",
        "BankTransferType", "BusinessIdentificationCode", "City", "Country", "CreatedById", "CreatedDate",
        "CreditCardAddress1", "CreditCardAddress2", "CreditCardCity", "CreditCardCountry", "CreditCardExpirationMonth",
        "CreditCardExpirationYear", "CreditCardHolderName", "CreditCardMaskNumber", "CreditCardState",
        "CreditCardType", "DeviceSessionId", "Email", "ExistingMandate", "FirstName", "IBAN", "Id", "IPAddress",
        "LastFailedSaleTransactionDate", "LastName", "LastTransactionDateTime", "LastTransactionStatus",
        "MandateCreationDate", "MandateID", "MandateReceived", "MandateUpdateDate", "MaxConsecutivePaymentFailures",
        "PaymentRetryWindow", "PaypalBaid", "PaypalEmail", "PaypalPreapprovalKey", "PaypalType", "Phone", "PostalCode",
        "State", "StreetName", "StreetNumber", "TotalNumberOfErrorPayments", "TotalNumberOfProcessedPayments", "Type",
        "UpdatedById", "UpdatedDate", "UseDefaultRetryRule"
        )
    }
  }
}
