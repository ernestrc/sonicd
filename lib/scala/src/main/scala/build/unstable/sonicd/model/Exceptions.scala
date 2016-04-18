package build.unstable.sonicd.model

object Exceptions {

  class ProtocolException(msg: String) extends Exception(s"Protocol exception: $msg")
}
