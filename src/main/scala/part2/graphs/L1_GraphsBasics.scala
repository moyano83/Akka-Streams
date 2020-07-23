package part2.graphs

import akka.NotUsed
import akka.actor.ActorSystem

import scala.concurrent.duration._
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}

object L1_GraphsBasics extends App {
  implicit val system = ActorSystem("GraphsBasics")
  implicit val materializer = ActorMaterializer()

  val input = Source(1 to 1000)
  val incrementer = Flow[Int].map(x => x + 1)
  val multiplier = Flow[Int].map(x => x * 10)

  // Imagine that we like to execute those two flows in parallel and then merge them back in a tuple
  // For that we have to use the fan out to create parallel computations like the above, and then fan in to merge back
  // together
  val output = Sink.foreach[(Int, Int)](x => println(s"${x._1} and ${x._2}"))
  // The runnable graph is a kind of object that we use to create before with sources and flows,
  // This graph DSL has two argument list which second argument is an implicit builder that has to create a static Graph
  // Step 1: Set the fundamentals for the graph (shape, static graph, and runnable graph)
  // Step 2: Add the necessary component for this graph
  // Step 3: Tying up the components
  // Step 4 - return a closed shape
  val graph = RunnableGraph.fromGraph(
    // The second argument to this function is a builder which is a MUTABLE data structure, everything that goes on this
    // function is to mutate this builder
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._ // brings some nice operators into scope

      val broadcast = builder.add(Broadcast[Int](2)) // fan-out operator
      val zip = builder.add(Zip[Int, Int]) // fan-in operator

      // step 3 - tying up the components
      input ~> broadcast

      broadcast.out(0) ~> incrementer ~> zip.in0
      broadcast.out(1) ~> multiplier ~> zip.in1

      zip.out ~> output
      // FREEZE the builder's shape making it immutable
      ClosedShape
      // shape
    } // graph
  ) // runnable graph

  //graph.run()

  val output1 = Sink.foreach[Int](x => println(s"${x + 1}"))
  val output2 = Sink.foreach[Int](x => println(s"${x * 10}"))
  // Exercise 1: Feed a source into 2 sinks at the same time
  val grapToMultipleSinks = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[Int](2))
      input ~> broadcast
      broadcast.out(0) ~> output1
      broadcast.out(1) ~> output2
      // The above can also be written like (known as implicit port numbering)
      // input ~> broadcast ~> output1
      // broadcast ~> output2

      ClosedShape
    }
  )
  //grapToMultipleSinks.run()

  // Exercise 2: There are 2 sources, fast and slow source which are merged in 1, which then feeds into a balance shape
  // which fans out again into two. This two streams are then output to independent sinks
  val fastSource = input.throttle(5, 1 second)
  val slowSource = input.throttle(2, 1 second)

  val sink1 = Sink.foreach[Int](x => println(s"Sink 1: ${x}"))
  val sink2 = Sink.foreach[Int](x => println(s"Sink 2: ${x}"))

  val exercise2 = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      // Merge is similar to Zip previously seen
      val merge = builder.add(Merge[Int](2))
      val balance =  builder.add(Balance[Int](2))

      //Tie all elements together
      fastSource ~> merge ~> balance ~> sink1
      slowSource~> merge // we don't need to add again ~> balance because it is already connected

      balance ~> sink2

      ClosedShape
    }
  )
  exercise2.run()

  system.terminate()
}