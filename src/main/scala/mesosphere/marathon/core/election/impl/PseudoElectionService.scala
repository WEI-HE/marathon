package mesosphere.marathon
package core.election.impl

import akka.actor.{ ActorSystem, ActorRef }
import akka.event.EventStream
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.election.{ ElectionEventListener, ElectionService, LocalLeadershipEvent }

/**
  * This a somewhat dummy implementation of [[ElectionService]]. It is used when
  * the high-availability mode is disabled.
  *
  * It assumes it is always leader stops Marathon when leadership is abdicated.
  */
class PseudoElectionService(
  hostPort: String,
  system: ActorSystem,
  val eventStream: EventStream)
    extends ElectionService with StrictLogging {

  override val isLeader: Boolean = true
  override def localHostPort: String = hostPort
  override val leaderHostPort: Option[String] = Some(hostPort)

  override def offerLeadership(listener: ElectionEventListener): Unit = {
    listener.startLeadership()
  }

  override def abdicateLeadership(): Unit = {
    // Ignore; abdicate in non-HA mode does not make sense.
  }

  private def startLeadership(): Unit = {}
  private def stopLeadership(): Unit = {}

  def subscribe(subscriber: ActorRef): Unit = {
    subscriber ! LocalLeadershipEvent.ElectedAsLeader
  }

  def unsubscribe(subscriber: ActorRef): Unit = {}
}
