package part4.advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.stream.{ActorMaterializer, KillSwitches}

object L1_DinamicStringHandling extends App{
  implicit val system = ActorSystem("DinamicStringHandling")
  implicit val materializer = ActorMaterializer()

  // A kill switch is a special kind of flow that emits the same elements that go through it but it materialize to a
  // special value that has some additional methods.
  val killSwitchFlow = KillSwitches.single[Int] // This is a flow that receives elements as Ints and can kill 1 stream

  val counter = Source(Stream.from(1)).throttle(1, 1 second).log("TagCounter")
  val sink = Sink.ignore

  // This returns a Runnable graph of type UniqueKillSwitch, which has some special methods.
  val killSwitch = counter.viaMat(killSwitchFlow)(Keep.right).to(sink).run()

  // for example, we can do the following to shutdown the stream after 1 second
  system.scheduler.scheduleOnce(5 second){
    killSwitch.shutdown()
  }

  // If we need to finish more than one stream, we can use the following kill switch
  val anotherCounter = Source(Stream.from(1)).throttle(2, 1 second).log("anotherCounter")
  val sharedKillSwitch = KillSwitches.shared("oneButtonToRuleThemAll")

  counter.via(sharedKillSwitch.flow).runWith(sink)
  anotherCounter.via(sharedKillSwitch.flow).runWith(sink)

  system.scheduler.scheduleOnce(5 second){
    //This will kill both of the streams
    sharedKillSwitch.shutdown()
  }

  // With dynamic stream handling is how to dynamically add fan in and fan out ports to graph elements
  // MergeHub
  val dynamicMerge = MergeHub.source[Int] // This matherializes as a Sink!

  // below, dynamicMerge is a Source of Int that is plugged into a Sink of Int, and the matherialized value of this graph,
  // is the matherialized value of dynamicMerge. This matherialized value can be plugged into other components
  val matherializedSink = dynamicMerge.to(Sink.foreach(println)).run()
  // Example of dynamically adding fan in inputs to a consumer
  Source(1 to 10).runWith(matherializedSink)

  // To recap and breaking down the pattern here, what we have done here is:
  // 1 - We created the dynamic merged and we plugged it into a consumer.
  // 2 - We run the graph to expose a materialized sync which we can then plug into other components.
  // 3 - So in the end what we end up doing is plugging all these sources to the same consumer.

  // The opposite of the above is the BroadcastHub:
  val dynamicBroadcast = BroadcastHub.sink[Int]
  // Because the component is a sink, we need to plug it into the same producer:
  // The result of running this it the materialized value of the Dynamic broadcast (which as opposite to the Sink, it has
  // many outputs instead of many inputs)
  val materializedSource = Source(1 to 100).runWith(dynamicBroadcast)
  // Now we can plug this source n number of times into different components
  materializedSource.runWith(sink)
  materializedSource.runWith(Sink.foreach(println))


  /**
   * Combine a mergeHub and a broadcasthub. This is a publish-subscriber model. Every single element produced by any
   * source will be known by every single consumer.
   */
  val (publisherPort,subscriberPort) = dynamicMerge.toMat(dynamicBroadcast)(Keep.both).run()
  subscriberPort.runWith(Sink.foreach(s => println(s"Received element: ${s}")))
  subscriberPort.map(s => s *2).runWith(Sink.foreach(s => println(s"Received doubled element: ${s}")))

  Source(List(1,1,2,3,5,8,13)).runWith(publisherPort)
  Source(List(0,0,0,10,0,0,0)).runWith(publisherPort)



  Thread.sleep(5000)
  system.terminate()
}
