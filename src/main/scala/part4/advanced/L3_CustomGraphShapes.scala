package part4.advanced

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream._

import scala.collection.immutable
import scala.concurrent.duration._

object L3_CustomGraphShapes extends App {
  implicit val system = ActorSystem("CustomGraphShapes")
  implicit val materializer = ActorMaterializer()

  // The goal of this lecture  is to learn how to create components with an arbitrary number of inputs and outputs
  // and of arbitrary types
  // Balance 2x3 shape
  // Every class that implements Shape needs to have exposed input ports, which are instances of a class called Inlet, and
  // output ports which are outlets. The pattern if you want to create your own shape what you would need to do is:
  // 1 - Create a case class that extends from shape, and we aggregate the ports, then we implement the required methods
  // from the Shape class
  // 2 - Create the actual implementation of the component that will have that shape and define the logic inside and
  // return it as part of the GraphDSL create method
  // 3 - Use this shape inside a runnable graph

  /*
    Step 1
   */
  case class Balance2x3(in0: Inlet[Int], in1: Inlet[Int], out0: Outlet[Int], out1: Outlet[Int], out2: Outlet[Int]) extends Shape {
    // we modified def to val to avoid reevaluating it every time the method is called
    override val inlets: immutable.Seq[Inlet[_]] = List(in0, in1)

    override val outlets: immutable.Seq[Outlet[_]] = List(out0, out1, out2)

    // This method should return a new instance of Balance2x3 by copying the input and output ports
    override def deepCopy(): Shape = Balance2x3(in0.carbonCopy(),
      in1.carbonCopy(),
      out0.carbonCopy(),
      out1.carbonCopy(),
      out2.carbonCopy()
    )
  }


  /*
    Step 2
   */
  // To create the graph for the shape we just create, we need to do it using merge and balance, and then return an
  // instance of our class shape with all the ports connected
  val balance2x3Impl = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val merge = builder.add(Merge[Int](2))
    val balance = builder.add(Balance[Int](3))
    merge ~> balance

    Balance2x3(merge.in(0), merge.in(1), balance.out(0), balance.out(1), balance.out(2))
  }

  /*
    Step 2
   */
  val balance2x3Graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val slowSource = Source(Stream.from(1)).throttle(1, 1 second)
    val fastSource = Source(Stream.from(1)).throttle(2, 1 second)

    def createSink(index: Int) = Sink.fold[Int, Int](0)((count, element) => {
      println(s"Sink[${index}]: Received ${element}, current count is ${count}")
      count + 1
    })

    val sink1 = builder.add(createSink(1))
    val sink2 = builder.add(createSink(2))
    val sink3 = builder.add(createSink(3))

    val balance = builder.add(balance2x3Impl)

    slowSource ~> balance.in0
    fastSource ~> balance.in1
    balance.out0 ~> sink1
    balance.out1 ~> sink2
    balance.out2 ~> sink3

    ClosedShape
  })
  // As we see when we run this, the input from fast and slow source, is evenly distributed in the balancer regardless of
  // the rate of production of the producers

  //balance2x3Graph.run()

  /**
   * Exercise: Generalize the balance component
   */
  // Step 1
  case class BalanceMxN[T](override val inlets:List[Inlet[T]],override val outlets:List[Outlet[T]]) extends Shape{
    override def deepCopy(): Shape = BalanceMxN(inlets.map(_.carbonCopy()), outlets.map(_.carbonCopy()))
  }

  // Step 2
  object BalanceMxN{
    def apply[T](input:Int, out:Int):Graph[BalanceMxN[T], NotUsed] = GraphDSL.create(){ implicit builder =>
      import GraphDSL.Implicits._
      val merge = builder.add(Merge[T](input))
      val balance = builder.add(Balance[T](out))
      merge ~> balance

      BalanceMxN(merge.inlets.toList, balance.outlets.toList)
    }
  }

  val balanceMxNImpl = BalanceMxN(3,5)

  val runGraph = RunnableGraph.fromGraph(GraphDSL.create(){implicit builder =>
    import GraphDSL.Implicits._
    val slowSource = Source(Stream.from(1)).throttle(1, 1 second)
    val fastSource = Source(Stream.from(1)).throttle(2, 1 second)
    val ultraFastSource = Source(Stream.from(1)).throttle(4, 1 second)

    def createSink(index: Int) = Sink.fold[Int, Int](0)((count, element) => {
      println(s"Sink[${index}]: Received ${element}, current count is ${count}")
      count + 1
    })

    val sink1 = builder.add(createSink(1))
    val sink2 = builder.add(createSink(2))
    val sink3 = builder.add(createSink(3))

    val balance = builder.add(BalanceMxN[Int](3,3))

    slowSource ~> balance.inlets(0)
    fastSource ~> balance.inlets(1)
    ultraFastSource ~> balance.inlets(2)
    balance.outlets(0) ~> sink1
    balance.outlets(1) ~> sink2
    balance.outlets(2) ~> sink3

    ClosedShape
  }).run()

  Thread.sleep(5000)

  system.terminate()
}
