package part3.techniques

import akka.actor.ActorSystem
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class L5_TestingStreamsSpec extends TestKit(ActorSystem("TestingAkkaStreams")) with WordSpecLike with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A simple Stream" should {
    "Satisfy basic assertions" in {
      // Basic assertions using materialized values
      val simpleSource = Source(1 to 10)
      val simpleSink = Sink.fold(0)((a: Int, b: Int) => a + b)

      val sumFuture = simpleSource.toMat(simpleSink)(Keep.right).run()
      val completedValue = Await.result(sumFuture, 2 seconds)

      assert(completedValue == 55)
    }

    "integrate with test actors via materialized values" in {
      val simpleSource = Source(1 to 10)
      val simpleSink = Sink.fold(0)((a: Int, b: Int) => a + b)

      val probe = TestProbe()
      simpleSource.toMat(simpleSink)(Keep.right).run().pipeTo(probe.ref)

      probe.expectMsg(55)
    }

    "integrate with a test actor based sink" in {
      val simpleSource = Source(1 to 10)
      // scan is the same as fold, but every intermediate value will also be emitted forward
      val flow = Flow[Int].scan[Int](0)((a, b) => a + b) // emits 0,1,3,6,10,15
      val streamUnderTest = simpleSource.via(flow)

      val probe = TestProbe()
      val probeSink = Sink.actorRef(probe.ref, "complete ACK")

      streamUnderTest.to(probeSink).run()
      probe.expectMsgAllOf(0, 1, 3, 6, 10, 15)
    }

    "integrate with Streams TestKit Sink" in {
      val sourceUnderTest = Source(1 to 5).map(_ * 2)
      val testSink = TestSink.probe[Int](system)
      // testSink selects the materializer value of the testSink. If we inspect the materialized test value is:
      // TestSubscriber.TestProbe[Int]
      val materialized = sourceUnderTest.runWith(testSink)
      materialized.request(5) // Request some demand from the source under test
        .expectNext(2, 4, 6, 8, 10)
        .expectComplete()
      // If you request less numbers than the one the source will create, then it'll fail on expectComplete method with a
      // timeout, same if you request more elements
    }

    "integrate with a test Kit source" in {
      val sinkUnderTest = Sink.foreach[Int] {
        case 13 => throw new RuntimeException("Bad Luck")
        case _ =>
      }

      val testSource = TestSource.probe[Int]
      val materialized = testSource.toMat(sinkUnderTest)(Keep.both).run()
      val (testPublisher, resultFuture) = materialized
      // TestPublisher has some convenient methods like
      testPublisher.sendNext(1)
        .sendNext(5)
        .sendNext(8)
        .sendNext(13)
        .sendComplete()
      resultFuture.onComplete {
        case Success(_) => fail("Should have thrown an exception on 13, and never completed")
        case Failure(_) => //ok
      }
    }

    "test flows with a test source and a test sink " in {
      val flowUnderTest = Flow[Int].map(_ * 2)
      val testSource = TestSource.probe[Int]
      val testSink = TestSink.probe[Int](system)

      val (publisher, subscriber) = testSource.via(flowUnderTest).toMat(testSink)(Keep.both).run()
      publisher.sendNext(1)
        .sendNext(5)
        .sendNext(13)
        .sendNext(28)
        .sendComplete()

      subscriber.request(4) // it needs to match the number of elements sent
        .expectNext(2)
        .expectNext(10)
        .expectNext(26)
        .expectNext(56)
        .expectComplete()
    }
  }
}
