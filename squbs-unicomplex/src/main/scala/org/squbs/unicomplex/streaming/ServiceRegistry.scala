package org.squbs.unicomplex.streaming

import javax.net.ssl.SSLContext

import akka.actor.Actor._
import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.agent.Agent
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.server.directives.PathDirectives
import akka.http.scaladsl.server._
import akka.http.scaladsl.{Http, HttpsContext}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.io.ClientAuth.{Need, Want}
import akka.util.Timeout
import com.typesafe.config.Config
import org.squbs.unicomplex._

import scala.concurrent.{Future, ExecutionContext}
import scala.util.Failure
import scala.util.Success
import akka.pattern.pipe
import scala.concurrent.duration._

class ServiceRegistry(val log: LoggingAdapter) extends ServiceRegistryBase[Path] {

  private var serverBindigs = Map.empty[String, Option[(ServerBinding)]] // Service actor and HttpListener actor

  var listenerRoutesVar = Map.empty[String, Agent[Seq[(Path, ActorWrapper)]]]

  override protected def listenerRoutes: Map[String, Agent[Seq[(Path, ActorWrapper)]]] = listenerRoutesVar

  override protected def listenerRoutes_=[B](newListenerRoutes: Map[String, Agent[Seq[(B, ActorWrapper)]]]): Unit =
    listenerRoutesVar = newListenerRoutes.asInstanceOf[Map[String, Agent[Seq[(Path, ActorWrapper)]]]]

  override private[unicomplex] def startListener(name: String, config: Config, notifySender: ActorRef)
                                                (implicit context: ActorContext): Unit = {

    val (interface, port, localPort, sslContext, needClientAuth) = bindConfig(config)

    implicit val am = ActorMaterializer()
    import context.system
    import context.dispatcher

    val bindingFuture = sslContext match {
      case Some(sslCtx) =>
        val httpsCtx = Some(HttpsContext(sslCtx, clientAuth = Some { if(needClientAuth) Need else Want }))
        Http().bindAndHandle(Handler(listenerRoutes(name)).flow, interface, port, httpsContext = httpsCtx )

      case None => Http().bindAndHandle(Handler(listenerRoutes(name)).flow, interface, port)
    }

    val uniSelf = context.self

    bindingFuture map { sb =>
      serverBindigs = serverBindigs + (name -> Some(sb))
      notifySender ! Ack
      HttpBindSuccess
    } pipeTo uniSelf

    bindingFuture recover { case e =>
      serverBindigs = serverBindigs + (name -> None)
      log.error(s"Failed to bind listener $name. Cleaning up. System may not function properly.")
      HttpBindFailed
    } pipeTo uniSelf

  }

  override private[unicomplex] def isListenersBound = serverBindigs.size == listenerRoutes.size

  override private[unicomplex] def prepListeners(listenerNames: Iterable[String])(implicit context: ActorContext) {
    import context.dispatcher
    listenerRoutes = listenerNames.map { listener =>
      listener -> Agent[Seq[(Path, ActorWrapper)]](Seq.empty)
    }.toMap

    import org.squbs.unicomplex.JMX._
    register(new ListenerBean(listenerRoutes), prefix + listenersName)
  }

  override private[unicomplex] def shutdownState: Receive = ???

  override private[unicomplex] def stopAll()(implicit context: ActorContext): Unit = ???

  override private[unicomplex] def isAnyFailedToInitialize: Boolean = serverBindigs.values exists (_ == None)

  override private[unicomplex] def isEmpty: Boolean = serverBindigs.isEmpty

  override private[unicomplex] def deregisterContext(webContexts: Seq[String])(implicit ec: ExecutionContext): Future[Ack.type] = ???

  override protected def pathCompanion(s: String): Path = Path(s)

  override protected def pathLength(p: Path) = p.length

  override private[unicomplex] def terminated(listenerActor: ActorRef): Unit = ???
}

private[unicomplex] class RouteActor(webContext: String, clazz: Class[RouteDefinition])
  extends Actor with ActorLogging {

  import scala.concurrent.duration._

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case e: Exception =>
        log.error(s"Received ${e.getClass.getName} with message ${e.getMessage} from ${sender().path}")
        Escalate
    }

  def actorRefFactory = context

  val routeDef =
    try {
      val d = RouteDefinition.startRoutes {
        WebContext.createWithContext[RouteDefinition](webContext) {
          clazz.newInstance
        }
      }
      context.parent ! Initialized(Success(None))
      d
    } catch {
      case e: Exception =>
        log.error(e, s"Error instantiating route from {}: {}", clazz.getName, e)
        context.parent ! Initialized(Failure(e))
        context.stop(self)
        RouteDefinition.startRoutes(new RejectRoute)
    }


  implicit val am = ActorMaterializer()
  implicit val rejectionHandler:RejectionHandler = routeDef.rejectionHandler.getOrElse(RejectionHandler.default)
  implicit val exceptionHandler:ExceptionHandler = routeDef.exceptionHandler.getOrElse(null)

  lazy val route = if (webContext.nonEmpty) {
    PathDirectives.pathPrefix(PathMatchers.separateOnSlashes(webContext)) {routeDef.route}
  } else {
    // don't append pathPrefix if webContext is empty, won't be null due to the top check
    routeDef.route
  }

  // TODO FIX ME
  import scala.concurrent.duration._
  implicit val askTimeOut: Timeout = 5 seconds
  import akka.pattern.pipe
  import context.dispatcher

  def receive = {
    case request: HttpRequest =>
      val origSender = sender()
      Route.asyncHandler(route).apply(request) pipeTo origSender
  }
}

object RouteDefinition {

  private[unicomplex] val localContext = new ThreadLocal[Option[ActorContext]] {
    override def initialValue(): Option[ActorContext] = None
  }

  def startRoutes[T](fn: => T)(implicit context: ActorContext): T = {
    localContext.set(Some(context))
    val r = fn
    localContext.set(None)
    r
  }
}

trait RouteDefinition {
  protected implicit final val context: ActorContext = RouteDefinition.localContext.get.get
  implicit final lazy val self = context.self

  def route: Route

  def rejectionHandler: Option[RejectionHandler] = None

  def exceptionHandler: Option[ExceptionHandler] = None
}

class RejectRoute extends RouteDefinition {

  import akka.http.scaladsl.server.directives.RouteDirectives.reject
  val route: Route = reject
}