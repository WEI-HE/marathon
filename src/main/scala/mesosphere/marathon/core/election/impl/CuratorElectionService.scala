package mesosphere.marathon
package core.election.impl

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ ExecutorService, Executors, TimeUnit }

import akka.actor.{ ActorSystem, ActorRef }
import akka.event.EventStream
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.instrument.Time
import mesosphere.marathon.core.async.ExecutionContexts
import mesosphere.marathon.core.base._
import mesosphere.marathon.core.election.{ ElectionEventListener, ElectionService, LocalLeadershipEvent }
import mesosphere.marathon.metrics.{ Metrics, ServiceMetric, Timer }
import org.apache.curator.framework.api.ACLProvider
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.leader.{ LeaderLatch, LeaderLatchListener }
import org.apache.curator.framework.{ AuthInfo, CuratorFramework, CuratorFrameworkFactory }
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL

import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * This class implements leader election using Curator and (in turn) Zookeeper. It is used
  * when the high-availability mode is enabled.
  *
  * One can become a leader only. If the leadership is lost due to some reason, it shuts down Marathon.
  * Marathon gets stopped on leader abdication too.
  */

class CuratorElectionService(
  config: MarathonConf,
  hostPort: String,
  system: ActorSystem,
  val eventStream: EventStream,
  lifecycleState: LifecycleState)
    extends ElectionService with StrictLogging {

  system.registerOnTermination {
    logger.info("Stopping leadership on shutdown")
    stop(exit = false)
  }

  private val threadExecutor: ExecutorService = Executors.newSingleThreadExecutor()
  /** We re-use the single thread executor here because some methods of this class might get blocked for a long time. */
  // private implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadExecutor)

  private val leadershipOffered = new AtomicBoolean(false)
  private val leaderLatchStarted = new AtomicBoolean(false)
  private[this] var electionListener: Option[ElectionEventListener] = None
  @volatile private var leaderLatch: Option[LeaderLatch] = None
  @volatile private[this] var isCurrentlyLeading: Boolean = false

  private lazy val client: CuratorFramework = CuratorElectionService.newCuratorConnection(config)

  override def isLeader: Boolean =
    isCurrentlyLeading

  override def localHostPort: String = hostPort

  override def leaderHostPort: Option[String] = leaderHostPortMetric.blocking {
    if (client.getState == CuratorFrameworkState.STOPPED) None
    else {
      try {
        leaderLatch.flatMap { latch =>
          val participant = latch.getLeader
          if (participant.isLeader) Some(participant.getId) else None
        }
      } catch {
        case NonFatal(ex) =>
          logger.error("Error while getting current leader", ex)
          None
      }
    }
  }

  override def offerLeadership(electionListener: ElectionEventListener): Unit = {
    logger.info("offered leadership")
    if (leadershipOffered.compareAndSet(false, true)) {
      if (lifecycleState.isRunning) {
        this.electionListener = Some(electionListener)
        logger.info("Going to acquire leadership")
        try {
          startLeaderLatch()
        } catch {
          case NonFatal(ex) =>
            logger.error("Fatal error while acquiring leadership. Exiting now", ex)
            stop(exit = true)
        }
      } else {
        logger.info("Not trying to get leadership since Marathon is shutting down")
      }
    } else {
      logger.error("Error! Leadership already offered! Exiting now")
      stop(exit = true)
    }
  }

  private def startLeaderLatch(): Unit = if (leaderLatchStarted.compareAndSet(false, true)) {
    require(leaderLatch.isEmpty, "leaderLatch is not empty")

    val latch = new LeaderLatch(
      client, config.zooKeeperLeaderPath + "-curator", hostPort)
    latch.addListener(LeaderChangeListener, threadExecutor)
    latch.start()
    leaderLatch = Some(latch)
  } else {
    logger.info("Not starting leader latch; either duplicate call or shutting down")
  }

  private[this] def leadershipAcquired(): Unit = electionListener match {
    case Some(c) =>
      try {
        isCurrentlyLeading = true
        logger.info(s"Starting $electionListener's leadership")
        c.startLeadership()
        logger.info(s"Started $electionListener's leadership")
        // Note - it is vitally important that this come after isCurrentlyLeading = true in order to guarantee state
        // receipt by subscriber
        eventStream.publish(LocalLeadershipEvent.ElectedAsLeader)
        logger.info(s"$electionListener has started")
        startMetrics()
      } catch {
        case NonFatal(ex) =>
          logger.error(s"Fatal error while starting leadership of $electionListener. Exiting now", ex)
          stop(exit = true)
      }
    case _ =>
      logger.error("Illegal state! Acquired leadership but no listener. Exiting now")
      stop(exit = true)
  }

  override def abdicateLeadership(): Unit = {
    logger.info("Abdicating leadership")
    stop(exit = true)
  }

  private def stop(exit: Boolean): Unit = {
    logger.info("Stopping the election service")
    try {
      stopLeadership()
    } catch {
      case NonFatal(ex) =>
        logger.error("Fatal error while stopping", ex)
    } finally {
      isCurrentlyLeading = false
      if (exit) {
        import ExecutionContexts.global
        system.scheduler.scheduleOnce(500.milliseconds) {
          Runtime.getRuntime.asyncExit()
        }
      }
    }
  }

  private def stopLeadership(): Unit = {
    stopMetrics()
    if (isLeader) {
      electionListener.foreach { l =>
        logger.info(s"Informing election listener of loss leadership")
        l.stopLeadership()
        logger.info(s"Stopped leadership")
      }
      eventStream.publish(LocalLeadershipEvent.Standby)
    }

    if (leaderLatchStarted.compareAndSet(false, true)) {
      () // do nothing; we just prevented it from ever starting
    } else {
      leaderLatch.foreach { latch =>
        try {
          if (client.getState != CuratorFrameworkState.STOPPED) latch.close()
        } catch {
          case NonFatal(ex) =>
            logger.error("Could not close leader latch", ex)
        }
      }
      leaderLatch = None
    }
  }

  /**
    * Listener which forwards leadership status events asynchronously via the provided function.
    *
    * We delegate the methods asynchronously so they are processed outside of the synchronized lock
    * for LeaderLatch.setLeadership
    */
  private object LeaderChangeListener extends LeaderLatchListener {
    override def notLeader(): Unit = {
      logger.info(s"Leader defeated. New leader: ${leaderHostPort.getOrElse("-")}")
      stop(exit = true)
    }

    override def isLeader(): Unit = {
      logger.info("Leader elected")
      leadershipAcquired()
    }
  }

  private val metricsStarted = new AtomicBoolean(false)
  private val leaderDurationMetric = "service.mesosphere.marathon.leaderDuration"
  private val leaderHostPortMetric: Timer = Metrics.timer(ServiceMetric, getClass, "current-leader-host-port")

  protected def startMetrics(): Unit =
    if (metricsStarted.compareAndSet(false, true)) {
      val startedAt = System.currentTimeMillis()
      Kamon.metrics.gauge(leaderDurationMetric, Time.Milliseconds)(System.currentTimeMillis() - startedAt)
    }

  protected def stopMetrics(): Unit =
    if (metricsStarted.compareAndSet(true, false))
      Kamon.metrics.removeGauge(leaderDurationMetric)

  def subscribe(subscriber: ActorRef): Unit = {
    eventStream.subscribe(subscriber, classOf[LocalLeadershipEvent])
    val currentState = if (isCurrentlyLeading) LocalLeadershipEvent.ElectedAsLeader else LocalLeadershipEvent.Standby
    subscriber ! currentState
  }

  def unsubscribe(subscriber: ActorRef): Unit = {
    eventStream.unsubscribe(subscriber, classOf[LocalLeadershipEvent])
  }
}

object CuratorElectionService extends StrictLogging {
  def newCuratorConnection(config: ZookeeperConf) = {
    logger.info(s"Will do leader election through ${config.zkHosts}")

    // let the world read the leadership information as some setups depend on that to find Marathon
    val defaultAcl = new util.ArrayList[ACL]()
    defaultAcl.addAll(config.zkDefaultCreationACL)
    defaultAcl.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)

    val aclProvider = new ACLProvider {
      override def getDefaultAcl: util.List[ACL] = defaultAcl
      override def getAclForPath(path: String): util.List[ACL] = defaultAcl
    }

    val retryPolicy = new ExponentialBackoffRetry(1.second.toMillis.toInt, 10)
    val builder = CuratorFrameworkFactory.builder().
      connectString(config.zkHosts).
      sessionTimeoutMs(config.zooKeeperSessionTimeout().toInt).
      connectionTimeoutMs(config.zooKeeperConnectionTimeout().toInt).
      aclProvider(aclProvider).
      retryPolicy(retryPolicy)

    // optionally authenticate
    val client = (config.zkUsername, config.zkPassword) match {
      case (Some(user), Some(pass)) =>
        builder.authorization(Collections.singletonList(
          new AuthInfo("digest", (user + ":" + pass).getBytes("UTF-8"))))
          .build()
      case _ =>
        builder.build()
    }

    client.start()
    client.blockUntilConnected(config.zkTimeoutDuration.toMillis.toInt, TimeUnit.MILLISECONDS)
    client

  }
}
