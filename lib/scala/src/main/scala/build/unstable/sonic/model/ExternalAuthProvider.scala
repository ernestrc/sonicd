package build.unstable.sonic.model

import akka.actor.ActorSystem

import scala.concurrent.Future

/**
  * Can be extended to provide other auth mechanisms. Concrete type's constructors
  * should take no arguments and should implement the `validate` method as desired.
  *
  * Implementations should be added to Sonicd's classpath and clients
  * can then choose it via the `provider` key in their query configuration:
  *
  * {
  *    query: "",
  *    auth: {
  *      "provider": "com.myself.MyProvider",
  *      "myProviderKey1" : "",
  *      "myProviderKey2" : ""
  *    }
  *  }
  *
  *  Otherwise, auth config will be assumed to be of [[SonicdAuth]]
  *  which expect to contain a JWT `token`:
  * {
  *    query: "",
  *    auth: "mySonicdToken"
  *  }
  */
abstract class ExternalAuthProvider {
  def validate(auth: AuthConfig, system: ActorSystem, traceId: String): Future[ApiUser]
}
