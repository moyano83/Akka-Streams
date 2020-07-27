package part3.techniques

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

import scala.concurrent.duration._
object L1_IntegratingWithActors extends App {

  implicit val system = ActorSystem("IntegratingWithActors")
  implicit val materializer = ActorMaterializer()

  class SimpleActor extends Actor with ActorLogging{
    override def receive: Receive = {
      case s:String =>
        log.info(s"Just received ${s}")
        sender() ! s + s
      case n:Int =>
        log.info(s"Just reveiced a number ${n}")
        sender() ! 2*n
    }
  }

  val simpleActor = system.actorOf(Props[SimpleActor])

  val source = Source(1 to 10)

  //Now the interesting bit, we can integrate an akka stream with an akka actor. We do this with the ask method on the
  // Flow class, which receives two parameters, the first is a number indicating the paralellism which is the number of
  // messages that the actor can have in the mailbox before starting to do backpressuring. The second is the actor
  // reference, this method under the hood uses the ask pattern which pipes a future from the actor. This future can be
  // completed with Any, so that's why we narrow the types by typing the Flow class (the first Int type is for the
  // receiving, the second is for the destination type).
  implicit val timeout = Timeout(3 seconds)
  val actorFlow = Flow[Int].ask[Int](parallelism = 4)(simpleActor)

  //source.via(actorFlow).to(Sink.foreach(s => println(s"Received message ${s}"))).run()
  // The above can also be achieved with
  //source.ask[Int](parallelism = 4)(simpleActor).to(Sink.foreach(println(_))).run()

  // We can use an actor as a source as well. Notice the type of the actor Source[Int, ActorRef]
  val actorPoweredSource = Source.actorRef[Int](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
  // If we connect the actor to a source, we can see that the return type is an actorRef, this is return as the
  // matherialized value when the actor is plugged into a graph
  val materializedValue = actorPoweredSource.to(Sink.foreach[Int](n => s"actor powered source got ${n}")).run()
  // When we get this reference, we can inject any value to the graph by sending messages to tge actor ref::
  materializedValue ! 20
  // If you want to terminate the stream you need to send an special message
  materializedValue ! akka.actor.Status.Success("complete")

  // Actor as a destination of messages. An actor powered will need to support some special signals it will need to
  // support an initialization message which will be sent first by whichever component ends up connected to this actor
  // powered sink:
  // Init message
  // Acknowledge message: Message sent to acknowledge reception and used for backpressure
  // A complete message
  // A function to generate a message in case this actor throws an Exception
  case object StreamInit
  case object StreamAck
  case object StreamComplete
  case class StreamFailed(ex:Throwable)

  class SinkActor extends Actor with ActorLogging{
    override def receive: Receive = {
      case StreamInit =>
        log.info("Actor Initialized")
        sender() ! StreamAck
      case StreamComplete =>
        log.info("Received complete, finishing actor")
        context.stop(self)
      case StreamFailed(ex) =>
        log.error("Actor failed", ex)
      case message =>
        log.info(s"Received message ${message}")
        // We need to send an StreamAck otherwise the lack of it will be interpreted as backpressure
        sender() ! StreamAck
    }
  }

  val destinationActor = system.actorOf(Props[SinkActor])
  // We create the ref, and then to create the sink, we pass the ref and we indicate the messages that would be used for
  // init, ack, complete and function from throwable
  val actorPoweredSink = Sink.actorRefWithAck[Int](
    destinationActor,
    onInitMessage = StreamInit,
    onCompleteMessage = StreamComplete,
    ackMessage = StreamAck,
    onFailureMessage = x => StreamFailed(x)
  )

  Source(1 to 10).to(actorPoweredSink).run()

  // If you remove the ack after receiving the message, it will be interpreted as backpressure and the source will stop
  // sending messages

  Thread.sleep(1000)
  system.terminate()
}
