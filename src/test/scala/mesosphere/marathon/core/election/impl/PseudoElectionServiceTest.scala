
package mesosphere.marathon
package core.election.impl

import akka.event.EventStream
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Keep, Sink, Source }
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.election.{ ElectionEventListener, ElectionService, LocalLeadershipEvent }
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Seconds, Span }

class PseudoElectionServiceTest extends AkkaUnitTest with Eventually {
  override implicit lazy val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  class Fixture {
    val hostPort: String = "unresolvable:2181"
    val events: EventStream = new EventStream(system)
    val electionService: ElectionService = new PseudoElectionService(hostPort, system, events)
  }

  class StubListener extends ElectionEventListener {
    var started = false
    var stopped = false
    def stopLeadership(): Unit = { stopped = true }
    def startLeadership(): Unit = { started = true }
  }

  "PseudoElectionService" should {
    "we immediately call startLeadership on offerLeadership" in {
      val f = new Fixture
      val c = new StubListener
      f.electionService.offerLeadership(c)
      c.started shouldBe true
    }

    "we receive an ElectedAsLeader event on subscribe" in {
      val (input, results) = Source.actorRef[LocalLeadershipEvent](1, OverflowStrategy.dropTail)
        .toMat(Sink.head)(Keep.both)
        .run
      val f = new Fixture
      f.electionService.subscribe(input)
      results.futureValue shouldBe (LocalLeadershipEvent.ElectedAsLeader)
    }
  }
}
