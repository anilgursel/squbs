/*
 *  Copyright 2015 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.squbs.unicomplex.streaming

import akka.actor.Actor._
import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.agent.Agent
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.server.directives.PathDirectives
import akka.http.scaladsl.server._
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.http.scaladsl.model.HttpRequest
import akka.stream.{BindFailedException, ActorMaterializer}
import akka.stream.TLSClientAuth.{Want, Need}
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import org.squbs.pipeline.streaming.PipelineSetting
import org.squbs.unicomplex._
import org.squbs.unicomplex.streaming.StatsSupport.StatsHolder

import scala.util.Failure
import scala.util.Success
import akka.pattern.pipe
import akka.actor.Status.{Failure => ActorFailure}

/**
  * Akka HTTP based [[ServiceRegistryBase]] implementation.
  */
class ServiceRegistry(val log: LoggingAdapter) extends ServiceRegistryBase[Path] {

  private var serverBindigs = Map.empty[String, Option[(ServerBinding)]] // Service actor and HttpListener actor

  var listenerRoutesVar = Map.empty[String, Agent[Seq[(Path, ActorWrapper, PipelineSetting)]]]

  override protected def listenerRoutes: Map[String, Agent[Seq[(Path, ActorWrapper, PipelineSetting)]]] = listenerRoutesVar

  override protected def listenerRoutes_=[B](newListenerRoutes: Map[String, Agent[Seq[(B, ActorWrapper, PipelineSetting)]]]): Unit =
    listenerRoutesVar = newListenerRoutes.asInstanceOf[Map[String, Agent[Seq[(Path, ActorWrapper, PipelineSetting)]]]]

  override private[unicomplex] def startListener(name: String, config: Config, notifySender: ActorRef)
                                                (implicit context: ActorContext): Receive = {

    val (interface, port, localPort, sslContext, needClientAuth) = bindConfig(config)

    implicit val am = ActorMaterializer()
    import context.system
    import context.dispatcher

    val uniSelf = context.self
    val serverFlow = (sslContext match {
      case Some(sslCtx) =>
        val httpsCtx = ConnectionContext.https(sslCtx, clientAuth = Some { if(needClientAuth) Need else Want })
        Http().bind(interface, port, connectionContext = httpsCtx )

      case None => Http().bind(interface, port)
    })


    val statsHolder = new StatsHolder
    val handler = Handler(listenerRoutes(name), localPort)

    serverFlow.to(Sink.foreach { conn =>

      conn.flow.transform(() => statsHolder.watchRequests())
        .join(handler.flow.transform(() => statsHolder.watchResponses()))
        .run()

    }).run() pipeTo uniSelf

    {
      case sb: ServerBinding =>
        import org.squbs.unicomplex.JMX._
        JMX.register(new ServerStats(name, statsHolder), prefix + serverStats + name)
        serverBindigs = serverBindigs + (name -> Some(sb))
        notifySender ! Ack
        uniSelf ! HttpBindSuccess
      case ActorFailure(ex) if ex.isInstanceOf[BindFailedException] =>
        serverBindigs = serverBindigs + (name -> None)
        log.error(s"Failed to bind listener $name. Cleaning up. System may not function properly.")
        notifySender ! Ack
        uniSelf ! HttpBindFailed
    }
  }

  override private[unicomplex] def isListenersBound = serverBindigs.size == listenerRoutes.size

  override private[unicomplex] def prepListeners(listenerNames: Iterable[String])(implicit context: ActorContext) {
    import context.dispatcher
    listenerRoutes = listenerNames.map { listener =>
      listener -> Agent[Seq[(Path, ActorWrapper, PipelineSetting)]](Seq.empty)
    }.toMap

    import org.squbs.unicomplex.JMX._
    register(new ListenerBean(listenerRoutes), prefix + listenersName)
  }

  case class Unbound(sb: ServerBinding)

  override private[unicomplex] def shutdownState: Receive = {
    case Unbound(sb) =>
      serverBindigs = serverBindigs.filterNot {
        case (_, Some(`sb`)) => true
        case _ => false
      }
  }

  override private[unicomplex] def listenerTerminated(listenerActor: ActorRef): Unit =
    log.warning(s"Unexpected serviceRegistry.listenerTerminated(${listenerActor.toString()}) in streaming use case.")

  override private[unicomplex] def stopAll()(implicit context: ActorContext): Unit = {

    import context.dispatcher
    import context.system
    val uniSelf = context.self

//    Http().shutdownAllConnectionPools() andThen { case _ =>
      serverBindigs foreach {
        case (name, Some(sb)) =>
          listenerRoutes(name)() foreach {case (_, aw, _) => aw.actor ! PoisonPill}
          listenerRoutes = listenerRoutes - name
          sb.unbind() andThen { case _ => uniSelf ! Unbound(sb) }
          if (listenerRoutes.isEmpty) {
            Http().shutdownAllConnectionPools()
            // TODO Unregister "Listeners" JMX Bean.
          }
        // TODO Unregister "ServerStats,Listener=name" JMX Bean
        case _ =>
      }
//    }
  }

  override private[unicomplex] def isAnyFailedToInitialize: Boolean = serverBindigs.values exists (_ == None)

  override private[unicomplex] def isShutdownComplete: Boolean = serverBindigs.isEmpty

  override protected def pathCompanion(s: String): Path = Path(s)

  override protected def pathLength(p: Path) = p.length
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

  // TODO Hold on..  Why do we directly need a materializer here..  Should be passed down..
  implicit val am = ActorMaterializer()
  implicit val rejectionHandler:RejectionHandler = routeDef.rejectionHandler.getOrElse(RejectionHandler.default)
  implicit val exceptionHandler:ExceptionHandler = routeDef.exceptionHandler.getOrElse(PartialFunction.empty[Throwable, Route])

  lazy val route = if (webContext.nonEmpty) {
    PathDirectives.pathPrefix(PathMatchers.separateOnSlashes(webContext)) {routeDef.route}
  } else {
    // don't append pathPrefix if webContext is empty, won't be null due to the top check
    routeDef.route
  }

  import akka.pattern.pipe
  import context.dispatcher

  def receive: Receive = {
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

trait RouteDefinition extends Directives {
  protected implicit final val context: ActorContext = RouteDefinition.localContext.get.get
  implicit final lazy val self = context.self

  def route: Route

  def rejectionHandler: Option[RejectionHandler] = None

  def exceptionHandler: Option[ExceptionHandler] = None
}

class RejectRoute extends RouteDefinition {

  val route: Route = reject
}