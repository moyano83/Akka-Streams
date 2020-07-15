package part1.primer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import part1.primer.L3_OperatorFusion.system

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object L2_MaterializingStreams extends App {
  // Materialization is the act of running a graph (A stream is static until you run it). The graph produces a single
  // matherialised value which we have to choose which is best. A Component can be run multiple times, and the materialise
  // value can be anything

  implicit val system = ActorSystem("MaterializingStreams")
  implicit val materializer = ActorMaterializer() // Allocates the right resources to run an akka stream

  val source = Source(1 to 10).to(Sink.foreach(println))

  val simpleMaterializeValue = source.run() // The return type is NotUsed which is a glorified Unit
  val anotherSource = Source[Int](1 to 10)
  val sink = Sink.reduce[Int]((a, b) => a + b) // The return is of type Future[Int]

  val sumFuture = anotherSource.runWith(sink) // Connects the graph and executes it

  // The problem with materialized values is that when you want to materialize a graph all the components
  // in that graph can return a materialized value, but the result of running the graph is a single materialized
  // value so you need to choose which value you want returned at the end.
  sumFuture.onComplete {
    case Success(value) => println(s"The value is ${value}")
    case Failure(exception) => println(s"Exception with ${exception}")
  }

  // When we construct a graph with the methods visa and 2 when we connect it flows and sings by default
  // the left most materialized value is kept.

  // We can transform the source with the viaMat method
  val simpleSink = Sink.foreach[Int](println)
  val simpleFlow = Flow[Int].map(x => x + 1)

  // This viaMAt method serves to combine the components from two sources and returns a third flow with the combined
  // components
  // in this case we just discard the values of anotherSource
  anotherSource.viaMat(simpleFlow)((sourceMat, flowMat) => flowMat)
  // The above is equivalent to the following, which can be combined with other components. The expression returns a
  // runnable graph which materialised value, which returns a Future of type "Done" (A signal of run ended)
  // There is a keep.left, Keep.both, Keep.None
  anotherSource.viaMat(simpleFlow)(Keep.right).toMat(simpleSink)(Keep.right) // to Mat serves to pipe the matherialised value to
  // another Mat

  // Akka streams has also a number of sugar syntax:
  Source(1 to 10).runWith(Sink.reduce[Int](_ + _))
  // This is equivalent to the following
  Source(1 to 10).runReduce[Int](_ + _)

  // It is also possible to connect a Flow with source and sink in a single expression:
  Flow[Int].map(x => x * x).runWith(anotherSource, sink)


  system.terminate()

}
