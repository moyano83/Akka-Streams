package part2.graphs

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, SinkShape}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object L4_GraphMaterializeValues extends App{
  implicit val system = ActorSystem("GraphMaterializeValues")
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher
  // A materialize is a value that a component exposes when is run in a graph

  val sourceWords = Source(List[String]("Akka", "is", "awesome", "it", "rules"))
  val printer = Sink.foreach(println)
  // The counter is a sink that exposed a materialized value =>Future[Int]
  val counter = Sink.fold[Int,String](0)((count, _)=> count + 1) // counts the number of words

  // Create a composite component that acts like a Sink, which prints out all lowercase strings and counts the short
  // strings (less than 5 characters).

  val complexWordSink = Sink.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[String](2))
      val lowerFilter = builder.add(Flow[String].filter(x => x == x.toLowerCase))
      val shortFilter = builder.add(Flow[String].filter(x => x.length <5))

      broadcast.out(0) ~> lowerFilter ~> printer
      broadcast.out(1) ~> shortFilter ~> counter

      SinkShape(broadcast.in)
    })

  //How do we change the return shape? at the moment is just a NotUsed value, but we can use the parameters passed to the
  // create method.  We can pass a list of actual components to the Graph. If the parameters are passed, then the function
  // after them instead of being a function from a builder to the shape that I care, it becomes a higher order function
  // between a builder and a function that takes the shape of the parameters passed in the argument list and returns the
  // shape that I care about. Check the signature:
  val complexWordSink2 = Sink.fromGraph(
    GraphDSL.create(counter) { implicit builder => counterShape =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[String](2))
      val lowerFilter = builder.add(Flow[String].filter(x => x == x.toLowerCase))
      val shortFilter = builder.add(Flow[String].filter(x => x.length <5))

      broadcast.out(0) ~> lowerFilter ~> printer
      broadcast.out(1) ~> shortFilter ~> counterShape

      SinkShape(broadcast.in)
    })
  // (Remember toMat is the function that allows us to choose the materialized value)
  val shortStringCounterFuture =  sourceWords.toMat(complexWordSink2)(Keep.right).run()
  shortStringCounterFuture.onComplete{
    case Success(value) => println(s"The total number of short strings is ${value}")
    case Failure(exception) => exception.printStackTrace()
  }
  // It is also possible to compose multiple materialize values, but then you need to add a second argument which is a
  // combination function (in this case it is a simple
  val complexWordSink3 = Sink.fromGraph(
    GraphDSL.create(printer, counter)((p,c) =>c) { implicit builder => (printerShape, counterShape) =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[String](2))
      val lowerFilter = builder.add(Flow[String].filter(x => x == x.toLowerCase))
      val shortFilter = builder.add(Flow[String].filter(x => x.length <5))

      broadcast.out(0) ~> lowerFilter ~> printerShape
      broadcast.out(1) ~> shortFilter ~> counterShape

      SinkShape(broadcast.in)
    })
  val shortStringCounterFuture2 =  sourceWords.toMat(complexWordSink3)(Keep.right).run()
  shortStringCounterFuture.onComplete{
    case Success(value) => println(s"Sink3 => The total number of short strings is ${value}")
    case Failure(exception) => exception.printStackTrace()
  }

  /**
   * Exercise: implement a method enhanceFlow(Flow[A, B, _]) : Flow[A,B,Future[Int]] this future int will contain the
   * number of elements that went throught the component
   */
  def enhanceFlow[A,B](f:Flow[A, B, _]) : Flow[A,B,Future[Int]] = {
    val counterSink = Sink.fold[Int, B](0)((counter,_) => counter+1 )
    Flow.fromGraph(GraphDSL.create(counterSink) { implicit builder => counterShape =>
        import GraphDSL.Implicits._
        val broadcast = builder.add(Broadcast[B](2))
        val originalFlowShape = builder.add(f)

        originalFlowShape ~> broadcast ~> counterShape

        FlowShape(originalFlowShape.in, broadcast.out(1))
    })
  }
  val source = Source(1 to 50)
  val simpleFlow = Flow[Int].map(x => x)
  val simpleSink = Sink.ignore

  val enhanceFlowCountFuture = source.viaMat(enhanceFlow(simpleFlow))(Keep.right).toMat(simpleSink)(Keep.left).run()
  enhanceFlowCountFuture.onComplete{
    case Success(value) => println(s"enhancedFlow => The total number of elements is ${value}")
    case Failure(exception) => exception.printStackTrace()
  }
  system.terminate()
}
