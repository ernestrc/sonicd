package build.unstable.sonicd.model

import java.io.{PrintWriter, StringWriter}

import scala.util.Try

/**
 * Service Task receipt. Used for basic communication between actors
 * and from the actor system to the outside world.
 *
 * @param success Boolean if whether task was successful or not
 * @param requestId Optional request id value
 * @param message Optional reply message
 * @param errors List of errors, if any.
 */
case class Receipt(success: Boolean,
                   requestId: Option[String] = None,
                   message: Option[String] = None,
                   errors: List[String] = List.empty)

object Receipt {

  def success: Receipt = Receipt(success = true)

  def success(message: Option[String], requestId: Option[String]): Receipt = {
    Receipt(
      success = true,
      requestId = requestId,
      message = message
    )
  }

  def getStackTrace(e: Throwable): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    sw.toString
  }

  def error(e: Throwable, message: String, requestId: Option[String] = None): Receipt = {
    val stackTrace = getStackTrace(e)
    val errors = stackTrace :: Nil
    Receipt(
      success = false,
      requestId = requestId,
      message = Some(message),
      errors = errors
    )
  }

  def reduce(s: Traversable[Receipt]): Receipt = s.reduce { (a, b) ⇒
    Receipt(
      success = a.success && b.success,
      message = a.message.map(m ⇒ b.message.map(m + "\n" + _).getOrElse(m)).orElse(b.message),
      requestId = a.requestId.orElse(b.requestId),
      errors = a.errors ::: b.errors
    )
  }

}



