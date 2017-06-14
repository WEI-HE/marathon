package mesosphere.marathon
package core.deployment.impl

import akka.Done
import akka.actor.Actor
import akka.event.EventStream
import mesosphere.marathon.core.condition.Condition.Terminal
import mesosphere.marathon.core.deployment.impl.StartingBehavior.Sync
import mesosphere.marathon.core.event.{ InstanceChanged, InstanceHealthChanged }
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.task.tracker.InstanceTracker
import org.slf4j.LoggerFactory

import scala.async.Async.{ async, await }
import scala.concurrent.Future
import scala.concurrent.duration._

trait StartingBehavior extends ReadinessBehavior { this: Actor =>
  import context.dispatcher

  def eventBus: EventStream
  def scaleTo: Int
  def nrToStart: Future[Int]
  def launchQueue: LaunchQueue
  def scheduler: SchedulerActions
  def instanceTracker: InstanceTracker

  def initializeStart(): Future[Done]

  private[this] val log = LoggerFactory.getLogger(getClass)

  @SuppressWarnings(Array("all")) // async/await
  final override def preStart(): Unit = {
    if (hasHealthChecks) eventBus.subscribe(self, classOf[InstanceHealthChanged])
    eventBus.subscribe(self, classOf[InstanceChanged])

    async {
      await(initializeStart())
      checkFinished()
      context.system.scheduler.scheduleOnce(1.seconds, self, Sync)
    }
  }

  final override def receive: Receive = readinessBehavior orElse commonBehavior

  @SuppressWarnings(Array("all")) // async/await
  def commonBehavior: Receive = {
    case InstanceChanged(id, `version`, `pathId`, _: Terminal, _) =>
      log.warn(s"New instance [$id] failed during app ${runSpec.id.toString} scaling, queueing another instance")
      instanceTerminated(id)
      launchQueue.addAsync(runSpec)

    case Sync => async {
      val launchedInstances = await(instanceTracker.countLaunchedSpecInstances(runSpec.id))
      val actualSize = await(launchQueue.getAsync(runSpec.id)).fold(launchedInstances)(_.finalInstanceCount)
      val instancesToStartNow = Math.max(scaleTo - actualSize, 0)
      log.debug(s"Sync start instancesToStartNow=$instancesToStartNow appId=${runSpec.id}")
      if (instancesToStartNow > 0) {
        log.info(s"Reconciling app ${runSpec.id} scaling: queuing $instancesToStartNow new instances")
        await(launchQueue.addAsync(runSpec, instancesToStartNow))
      }
      context.system.scheduler.scheduleOnce(5.seconds, self, Sync)
    }
  }

  override def instanceConditionChanged(instanceId: Instance.Id): Unit = {
    log.debug(s"New instance $instanceId changed during app ${runSpec.id} scaling, " +
      s"${readyInstances.size} ready ${healthyInstances.size} healthy need ${nrToStart.value}")
    checkFinished()
  }

  def checkFinished(): Unit = {
    nrToStart.foreach{ n =>
      if (targetCountReached(n)) success()
    }
  }

  def success(): Unit
}

object StartingBehavior {
  case object Sync
}

