package build.unstable

import java.io.{PrintWriter, StringWriter}

package object sonic {
  //fixme break down StackTraceElements
  def fromStackTrace(stackTrace: String): Throwable = {
    new Throwable(stackTrace)
  }

  def getStackTrace(e: Throwable): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    sw.toString
  }
}
