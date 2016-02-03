package org.squbs.unicomplex

import javax.net.ssl.SSLContext
import akka.actor.Actor._
import akka.actor.{Props, ActorRef, ActorContext}
import akka.agent.Agent
import akka.event.LoggingAdapter
import akka.io.IO
import com.typesafe.config.Config
import org.squbs.unicomplex.ConfigUtil._

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.mutable.ListBuffer

trait ServiceRegistryBase[T] {

  val log: LoggingAdapter

  protected def listenerRoutes: Map[String, Agent[Seq[(T, ActorWrapper)]]]

  protected def listenerRoutes_=[B](newListenerRoutes: Map[String, Agent[Seq[(B, ActorWrapper)]]]): Unit

  private[unicomplex] def prepListeners(listenerNames: Iterable[String])(implicit context: ActorContext) {
    import context.dispatcher
    listenerRoutes = listenerNames.map { listener =>
      listener -> Agent[Seq[(T, ActorWrapper)]](Seq.empty)
    }.toMap

    import org.squbs.unicomplex.JMX._
    register(new ListenerBean(listenerRoutes), prefix + listenersName)
  }

  private[unicomplex] def registerContext(listeners: Iterable[String], webContext: String, servant: ActorWrapper) {
    listeners foreach { listener =>
      val agent = listenerRoutes(listener)
      agent.send {
        currentSeq =>
          merge(currentSeq, webContext, servant, {
            log.warning(s"Web context $webContext already registered on $listener. Override existing registration.")
          })
      }
    }
  }

  protected def pathCompanion(s: String): T

  protected def pathLength(p: T): Int

  private[unicomplex] def deregisterContext(webContexts: Seq[String])
                                           (implicit ec: ExecutionContext): Future[Ack.type] = {
    val futures = listenerRoutes flatMap {
      case (_, agent) => webContexts map { ctx => agent.alter {
        oldEntries =>
          val buffer = ListBuffer[(T, ActorWrapper)]()
          val path = pathCompanion(ctx)
          oldEntries.foreach {
            entry => if (!entry._1.equals(path)) buffer += entry
          }
          buffer.toSeq
      }
      }
    }
    Future.sequence(futures) map { _ => Ack}
  }

  private[unicomplex] def startListener(name: String, config: Config, notifySender: ActorRef)
                                       (implicit context: ActorContext): Unit

  private[unicomplex] def isEmpty: Boolean

  private[unicomplex] def stopAll()(implicit context: ActorContext): Unit

  private[unicomplex] def isAnyFailedToInitialize: Boolean

  private[unicomplex] def shutdownState: Receive

  private[unicomplex] def isListenersBound: Boolean

  private[unicomplex] def terminated(listenerActor: ActorRef): Unit

  protected def bindConfig(config: Config) = {
    val interface = if (config getBoolean "full-address") ConfigUtil.ipv4
    else config getString "bind-address"
    val port = config getInt "bind-port"
    // assign the localPort only if local-port-header is true
    val localPort = config getOptionalBoolean "local-port-header" flatMap { useHeader =>
      if (useHeader) Some(port) else None
    }

    val (sslContext, needClientAuth) =
      if (config.getBoolean("secure")) {

        val sslContextClassName = config.getString("ssl-context")
        implicit def sslContext: SSLContext =
          if (sslContextClassName == "default") SSLContext.getDefault
          else {
            try {
              val clazz = Class.forName(sslContextClassName)
              clazz.getMethod("getServerSslContext").invoke(clazz.newInstance()).asInstanceOf[SSLContext]
            } catch {
              case e: Throwable =>
                System.err.println(s"WARN: Failure obtaining SSLContext from $sslContextClassName. " +
                  "Falling back to default.")
                SSLContext.getDefault
            }
          }

        (Some(sslContext), config.getBoolean("need-client-auth"))
      } else (None, false)

    (interface, port, localPort, sslContext, needClientAuth)
  }

  private def merge[C](oldRegistry: Seq[(T, C)], webContext: String, servant: C,
                                             overrideWarning: => Unit = {}): Seq[(T, C)] = {
    val newMember = (pathCompanion(webContext), servant)
    if (oldRegistry.isEmpty) Seq(newMember)
    else {
      val buffer = ListBuffer[(T, C)]()
      var added = false
      oldRegistry foreach {
        entry =>
          if (added) buffer += entry
          else
          if (entry._1.equals(newMember._1)) {
            overrideWarning
            buffer += newMember
            added = true
          } else {
            if (pathLength(newMember._1) >= pathLength(entry._1)) {
              buffer += newMember += entry
              added = true
            } else buffer += entry
          }
      }
      if (!added) buffer += newMember
      buffer.toSeq
    }
  }
}


class ListenerBean[T](listenerRoutes: Map[String, Agent[Seq[(T, ActorWrapper)]]]) extends ListenerMXBean {

  override def getListeners: java.util.List[ListenerInfo] = {
    import scala.collection.JavaConversions._
    listenerRoutes.flatMap { case (listenerName, agent) =>
      agent() map { case (webContext, servant) =>
        ListenerInfo(listenerName, webContext.toString(), servant.actor.toString())
      }
    }.toSeq
  }
}