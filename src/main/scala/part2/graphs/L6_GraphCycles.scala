package part2.graphs

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, MergePreferred, RunnableGraph, Sink, Source, Zip, ZipWith}
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}

object L6_GraphCycles extends App {
  implicit val system = ActorSystem("GraphCycles")
  implicit val materializer = ActorMaterializer()

  val accelerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(Merge[Int](2))
    val incrementerShape = builder.add(Flow[Int].map(x => {
      println(s"Accelerating ${x}")
      x + 1
    }))

    sourceShape ~> mergeShape ~> incrementerShape
    mergeShape <~ incrementerShape
    ClosedShape
  }

  //RunnableGraph.fromGraph(accelerator).run()
  // Surprisingly, the output of the graph is a single line: Accelerating is 1
  // this is due to we always increase the values on the graph, but we never get rid of any, so the component ends up
  // buffering elements and quickly become full, backpressuring the whole graph since the backpressure is transmitted
  // backwards. This is called a "graph cycle deadlock". There are solutions to this problem:

  // Solution 1: MergePreferred
  // This is a special Merge component which has a preferred input, so whenever a new element is available in that input,
  // it takes that input and pass it on, irrespective of the elements that there are in the other output

  val acceleratorWithPreferrence = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(MergePreferred[Int](1))// This argument is one because it is MergePreferred port + 1 = 2
    val incrementerShape = builder.add(Flow[Int].map(x => {
      println(s"Accelerating ${x}")
      x + 1
    }))

    sourceShape ~> mergeShape ~> incrementerShape
    mergeShape.preferred <~ incrementerShape
    ClosedShape
  }
  //RunnableGraph.fromGraph(acceleratorWithPreferrence).run()

  // Solution 2: Overwriting Buffer strategy
  val bufferedAccelerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(Merge[Int](2))
    // With the following logic we drop the oldest elements in the buffer
    val repeaterShape = builder.add(Flow[Int].buffer(10, OverflowStrategy.dropHead).map(x => {
      println(s"Repeating ${x}")
      // we add a bit of a delay so if we see the output is incrementing, it means that it is dropping out the elements of
      // this flow and accepting the ones coming from the Source instead
      Thread.sleep(100)
      x
    }))

    sourceShape ~> mergeShape ~> repeaterShape
    mergeShape <~ repeaterShape
    ClosedShape
  }
  //RunnableGraph.fromGraph(bufferedAccelerator).run()

  // IMPORTANT LESSONS: IF YOU HAVE CYCLES IN YOUR GRAPH, YOU RISK DEADLOCKING, SPECIALLY WITH UNBOUNDEDNESS IN YOUR CYCLES
  // To avoid this, you can add bounds (boundedness) to the number of elements or give the graph the capacity to not
  // deadlock (liveness)

  /**
   * Exercise: Create a Fan in shape which takes two inputs which will be fed with exactly one number, and the output will
   * emit the fibonacci sequence based on this two numbers
   */
  val fibonacciGraph = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape1 = builder.add(Source.single(BigInt(1)))
    val sourceShape2 = builder.add(Source.single(BigInt(1)))
    val zipShape = builder.add(Zip[BigInt,BigInt]())
    val mergeShape = builder.add(MergePreferred[(BigInt,BigInt)](1))
    val fiboLogic = builder.add(Flow[(BigInt,BigInt)].map{tuple =>
      val last = tuple._1
      val previousValue = tuple._2
      Thread.sleep(100)
      (last + previousValue, last)
    })
    val broadcast = builder.add(Broadcast[(BigInt,BigInt)](2))
    val sink = builder.add(Sink.foreach[(BigInt,BigInt)](x => println(s"Fibonacci: ${x._1}")))

    sourceShape1 ~>  zipShape.in0
    sourceShape2 ~> zipShape.in1
    zipShape.out ~> mergeShape ~> fiboLogic ~> broadcast ~> sink
    mergeShape.preferred <~ broadcast.out(1)
    ClosedShape
  }

  RunnableGraph.fromGraph(fibonacciGraph).run()
  system.terminate()
}
