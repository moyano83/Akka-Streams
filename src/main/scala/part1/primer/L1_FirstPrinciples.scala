package part1.primer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object L1_FirstPrinciples extends App {

  implicit val system= ActorSystem("FirstPrinciples")
  // Materializer allows the running of akka streams components
  implicit val materializer = ActorMaterializer() // needs an actor ref materializer

  // An akka stream is usually composed of a Source (emiter) a flow (processor) and a sink (receiver)
  // Sources can emit any type of elements as long as they are immutable and serializable (nulls are not allowed per
  // akka streams specification). The usual way to construct an Stream is Source -> Flow -> Flow -> ... -> Sink
  val source = Source(1 to 10)
  val sink = Sink.foreach[Int](println)

  val graph = source to sink // This expression is called a Graph, and it doesn't to anything until we call run on the result
  graph.run // needs an implicit materializer as parameter


  // A Flow is an akka element that serves to transform elements in an stream
  val flow = Flow[Int].map(x => x+1)
  // To attach a flow to a source (returning a new source in the process), we call the via method on source
  val newSource = source.via(flow)
   newSource to sink run

  //There are several types of sources
  Source.single(1)
  Source(List(1,2,3))
  Source.empty[String]
  Source(Stream.from(1, 1)) // Do not confuse an akka stream with a "collection" stream
  import scala.concurrent.ExecutionContext.Implicits.global
  Source.fromFuture(Future(42)) // Source from a future

  // There is also several types of Sinks
  Sink.ignore // Does nothing
  Sink.foreach[Int](x => x+1)
  val head = Sink.head[Int] // Retrieves the head of the stream and then it closes it
  Sink.fold[Int,Int](0)(_ + _) // Computes the sum of all elements pass to it

  // Same applies to Flows
  // There is no FlatMap operator in flow
  val mapFlow = Flow[Int].map(x => x + 2)
  val takeFlow = Flow[Int].take(10) // converts an infinite stream to a finite stream with n elements
  val dropFlow = Flow[String].drop(10)

  source.via(flow).via(takeFlow).to(sink)

  // There is also syntactic sugars in akka Streams:
  Source[Int](1 to 10).map( _ + 1) // equivalent to Source[Int](1 to 10).via(Flow[Int].map(_+1)
  //To run streams directly do:
  Source[Int](1 to 10).map( _ + 1).runForeach(println)

  /**
   * Exercise: create a stream that taks persons names and the keep the first two names with length > 8 characters and
   * print them
   */
  val names =List("John", "Samanta Jane", "Katherina", "Christina", "Paul", "Maria")

  Source[String](names).via(Flow[String].filter(_.length >= 8)).via(Flow[String].take(2)).runForeach(println)
  // or
  Source[String](names).filter(_.length >= 8).take(2).runForeach(println)


  Thread.sleep(100)
  materializer.shutdown()
  system.terminate()
}
