package build.unstable.sonic

object Exceptions {

  class ProtocolException(msg: String) extends Exception(s"Protocol exception: $msg")
}
