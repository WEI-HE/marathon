package mesosphere.marathon
package core.deployment.impl

import akka.Done
import akka.actor.{OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.testkit.{TestActorRef, TestProbe}
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.condition.Condition.{Failed, Running}
import mesosphere.marathon.core.event.{DeploymentStatus, _}
import mesosphere.marathon.core.health.MesosCommandHealthCheck
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.launcher.impl.LaunchQueueTestHelper
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.readiness.ReadinessCheckExecutor
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.{AppDefinition, Command}

import scala.concurrent.{Future, Promise}

class TaskStartActorTest extends AkkaUnitTest {
  "TaskStartActor" should {
    for (
      (counts, description) <- Seq(
        None -> "with no item in queue",
        Some(LaunchQueueTestHelper.zeroCounts) -> "with zero count queue item"
      )
    ) {
      s"Start success $description" in {
        val f = new Fixture
        val promise = Promise[Unit]()
        val app = AppDefinition("/myApp".toPath, instances = 5)

        f.launchQueue.getAsync(app.id) returns Future.successful(counts)
        f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(0)
        val ref = f.startActor(app, app.instances, promise)
        watch(ref)

        verify(f.launchQueue, timeout(3000)).addAsync(app, app.instances)

        for (i <- 0 until app.instances)
          system.eventStream.publish(f.instanceChange(app, Instance.Id.forRunSpec(app.id), Running))

        promise.future.futureValue should be(())

        expectTerminated(ref)
      }
    }

    "Start success with one task left to launch" in {
      val f = new Fixture
      val counts = Some(LaunchQueueTestHelper.zeroCounts.copy(instancesLeftToLaunch = 1, finalInstanceCount = 1))
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 5)

      f.launchQueue.getAsync(app.id) returns Future.successful(counts)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      verify(f.launchQueue, timeout(3000)).addAsync(app, app.instances - 1)

      for (i <- 0 until (app.instances - 1))
        system.eventStream.publish(f.instanceChange(app, Instance.Id(s"task-$i"), Running))

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start success with existing task in launch queue" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 5)

      f.launchQueue.getAsync(app.id) returns Future.successful(None)
      f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(1)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      verify(f.launchQueue, timeout(3000)).addAsync(app, app.instances - 1)

      for (i <- 0 until (app.instances - 1))
        system.eventStream.publish(f.instanceChange(app, Instance.Id(s"task-$i"), Running))

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start success with no instances to start" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 0)
      f.launchQueue.getAsync(app.id) returns Future.successful(None)
      f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(0)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start with health checks" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition(
        "/myApp".toPath,
        instances = 5,
        healthChecks = Set(MesosCommandHealthCheck(command = Command("true")))
      )
      f.launchQueue.getAsync(app.id) returns Future.successful(None)
      f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(0)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      verify(f.launchQueue, timeout(3000)).addAsync(app, app.instances)

      for (i <- 0 until app.instances)
        system.eventStream.publish(f.healthChange(app, Instance.Id(s"task_$i"), healthy = true))

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start with health checks with no instances to start" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition(
        "/myApp".toPath,
        instances = 0,
        healthChecks = Set(MesosCommandHealthCheck(command = Command("true")))
      )
      f.launchQueue.getAsync(app.id) returns Future.successful(None)
      f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(0)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Task fails to start" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 1)

      f.launchQueue.getAsync(app.id) returns Future.successful(None)
      f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(0)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      verify(f.launchQueue, timeout(3000)).addAsync(app, app.instances)

      system.eventStream.publish(f.instanceChange(app, Instance.Id.forRunSpec(app.id), Failed))

      verify(f.launchQueue, timeout(3000)).addAsync(app, 1)

      for (i <- 0 until app.instances)
        system.eventStream.publish(f.instanceChange(app, Instance.Id.forRunSpec(app.id), Running))

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start success with dying existing task, reschedules, but finishes early" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 5)

      f.launchQueue.getAsync(app.id) returns Future.successful(None)
      f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(1)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      // wait for initial sync
      verify(f.launchQueue, timeout(3000)).getAsync(app.id)
      verify(f.launchQueue, timeout(3000)).addAsync(app, app.instances - 1)

      noMoreInteractions(f.launchQueue)
      reset(f.launchQueue)

      // let existing task die
      f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(0)
      f.launchQueue.getAsync(app.id) returns Future.successful(Some(LaunchQueueTestHelper.zeroCounts.copy(instancesLeftToLaunch = 4, finalInstanceCount = 4)))

      system.eventStream.publish(f.instanceChange(app, Instance.Id(s"task-4"), Condition.Error))
      verify(f.launchQueue, timeout(3000)).addAsync(app, 1)

      // sync will reschedule task
      ref ! StartingBehavior.Sync
//      verify(f.launchQueue, timeout(3000)).getAsync(app.id)
      verify(f.launchQueue, timeout(3000)).addAsync(app, 1)

      noMoreInteractions(f.launchQueue)
      reset(f.launchQueue)
      f.launchQueue.addAsync(any, any) returns Future.successful(Done)

      // launch 4 of the tasks
      f.launchQueue.getAsync(app.id) returns Future.successful(Some(LaunchQueueTestHelper.zeroCounts.copy(instancesLeftToLaunch = app.instances, finalInstanceCount = 4)))
      f.taskTracker.countLaunchedSpecInstances(app.id) returns Future.successful(4)
      List(0, 1, 2, 3) foreach { i =>
        system.eventStream.publish(f.instanceChange(app, Instance.Id(s"task-$i"), Running))
      }

      // it finished early
      promise.future.futureValue should be(())

      noMoreInteractions(f.launchQueue)

      expectTerminated(ref)
    }
  }
  class Fixture {

    val scheduler: SchedulerActions = mock[SchedulerActions]
    val launchQueue: LaunchQueue = mock[LaunchQueue]
    val taskTracker: InstanceTracker = mock[InstanceTracker]
    val deploymentManager = TestProbe()
    val status: DeploymentStatus = mock[DeploymentStatus]
    val readinessCheckExecutor: ReadinessCheckExecutor = mock[ReadinessCheckExecutor]

    launchQueue.addAsync(any, any) returns Future.successful(Done)

    def instanceChange(app: AppDefinition, id: Instance.Id, condition: Condition): InstanceChanged = {
      val instance: Instance = mock[Instance]
      instance.instanceId returns id
      InstanceChanged(id, app.version, app.id, condition, instance)
    }

    def healthChange(app: AppDefinition, id: Instance.Id, healthy: Boolean): InstanceHealthChanged = {
      InstanceHealthChanged(id, app.version, app.id, Some(healthy))
    }

    def startActor(app: AppDefinition, scaleTo: Int, promise: Promise[Unit]): TestActorRef[TaskStartActor] =
      TestActorRef(childSupervisor(TaskStartActor.props(
        deploymentManager.ref, status, scheduler, launchQueue, taskTracker, system.eventStream, readinessCheckExecutor,
        app, scaleTo, promise), "Test-TaskStartActor"))

    // Prevents the TaskActor from restarting too many times (filling the log with exceptions) similar to how it's
    // parent actor (DeploymentActor) does it.
    def childSupervisor(props: Props, name: String): Props = {
      import scala.concurrent.duration._

      BackoffSupervisor.props(
        Backoff.onFailure(
          childProps = props,
          childName = name,
          minBackoff = 5.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
        ).withSupervisorStrategy(
          OneForOneStrategy() {
            case _ => SupervisorStrategy.Restart
          }
        ))
    }
  }
}
