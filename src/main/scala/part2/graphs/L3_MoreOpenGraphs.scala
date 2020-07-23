package part2.graphs

import java.util.Date

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, FanOutShape, FanOutShape2, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, Zip, ZipWith}

object L3_MoreOpenGraphs extends App{
  implicit val system = ActorSystem("MoreOpenGraphs")
  implicit val materializer = ActorMaterializer()

  /**
   * Example: Design a shape for Max 3 operators, which will receive 3 ints and will push out the maximum of the 3
   */

  val max3StaticGraph = GraphDSL.create(){implicit builder =>
    import GraphDSL.Implicits._

    // ZipWith is a zipping-function from the input values to the output value
    val max1 = builder.add(ZipWith[Int,Int,Int]((a,b)=> Math.max(a,b)))
    val max2 = builder.add(ZipWith[Int,Int,Int]((a,b)=> Math.max(a,b)))

    max1.out ~> max2.in0
    //This below creates a shape which parameters are 1st the output port, and then an array with the input ports
    // It is called UniformFanInShape because all the inputs are of the same type, the same applies for the
    // UniformFanOutShape
    UniformFanInShape(max2.out, max1.in0, max1.in1, max2.in1)
  }

  val source1 = Source(1 to 10)
  val source2 = Source(1 to 10).map(_ => 5)
  val source3 = Source((1 to 10).reverse)
  val sink = Sink.foreach(println)

  val max3RunnableGraph = RunnableGraph.fromGraph(GraphDSL.create(){implicit builder =>
    import GraphDSL.Implicits._
    val flow = builder.add(max3StaticGraph)

    source1 ~> flow.in(0)
    source2 ~> flow.in(1)
    source3 ~> flow.in(2)
    flow.out ~> sink
    ClosedShape
  })

  max3RunnableGraph.run()

  // In the same way, we can create also a component which inputs and outputs can be of different types
  /**
   * We have a money laundering system that process bank transactions, it has to detect suspicious transactions based on:
   *  - Amount is bigger than 10.000$ (input)
   *  - Output can be either:
   *    1- The same transaction if it is valid
   *    2- Gives back only the transacition Id for further analysis
   */

  case class Transaction(id:String, source:String, recipient:String, amount:Int, date:Date = new Date())

  val transactionSource = Source(List[Transaction](
    Transaction("1", "Paul", "Ana", 5000),
    Transaction("2", "Tom", "Peter", 2000),
    Transaction("3", "John", "Clara", 7000),
    Transaction("4", "David", "Kate", 500000),
    Transaction("5", "Jim", "Alice", 200000),
  ))

  val bankProcessor = Sink.foreach[Transaction](println)
  val suspiciousTransactions = Sink.foreach[String](tx =>println(s"Suspicious transaction id ${tx}"))

  val suspiciousTxStaticGraph = GraphDSL.create(){implicit builder =>
    import GraphDSL.Implicits._
    val broadcast = builder.add(Broadcast[Transaction](2))
    val suspiciousFilter = builder.add(Flow[Transaction].filter(tx => tx.amount >= 10000))
    val idExtractor = builder.add(Flow[Transaction].map(tx => tx.id))

    broadcast.out(0) ~> suspiciousFilter ~> idExtractor

    // The first field is the input port (from the broadcast), then 1 of the output is the unconnected output from the
    // broadcast and the other output is going to be the ouput of the transaction extractor
    // notice the 'new' keyword, it is not a builder or an apply method!
    new FanOutShape2(broadcast.in, broadcast.out(1), idExtractor.out) // returns shapes of different types
  }

  val suspiciousTxRunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val suspiciousTxGraph = builder.add(suspiciousTxStaticGraph)

      transactionSource ~> suspiciousTxGraph.in
      suspiciousTxGraph.out0 ~> bankProcessor
      suspiciousTxGraph.out1 ~> suspiciousTransactions

      ClosedShape
    })
  suspiciousTxRunnableGraph.run()

  system.terminate()
}
