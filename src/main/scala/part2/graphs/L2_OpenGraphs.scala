package part2.graphs

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, SinkShape, SourceShape}

object L2_OpenGraphs extends App{
  implicit val system = ActorSystem("OpenGraphs")
  implicit val materializer = ActorMaterializer()

  // Let's create a composite source that concatenates two sources s1+s2 = s11+...+s1n + s21 + ... + s2n
  val firstSource = Source(1 to 10)
  val secondSource = Source(11 to 20)

  //Instead of creating the RunnableGraph, we can create other shapes, in this case a Source
  val sourceGraph = Source.fromGraph(GraphDSL.create(){ implicit builder =>
    import GraphDSL.Implicits._
    //Creating components
    val concat = builder.add(Concat[Int](2))
    //Tyding them together
    firstSource ~> concat
    secondSource ~> concat

    // Concat has an output port that we can use
    SourceShape(concat.out)
  })

  sourceGraph.to(Sink.foreach(println)).run()

  val sink1 = Sink.foreach[Int](x => println(s"Sink1:$x"))
  val sink2 = Sink.foreach[Int](x => println(s"Sink2:$x"))

  // In the same way, we can create a complex sink
  val sink = Sink.fromGraph(GraphDSL.create(){implicit builder =>
    import GraphDSL.Implicits._
    val broadcast = builder.add(Broadcast[Int](2))
    broadcast ~> sink1
    broadcast ~> sink2

    SinkShape(broadcast.in)
  })

  sourceGraph.to(sink).run()

  // Create a complex flow that's composed of two other flows, one that adds 2 to a number and another one that multiplies
  // by 10

  val flow1 = Flow[Int].map(x => x + 2)
  val flow2 = Flow[Int].map(x => x * 10)
  val flowGraph = Flow.fromGraph(GraphDSL.create(){implicit builder =>
    import GraphDSL.Implicits._
    val incrementerShape = builder.add(flow1)
    val multiplierShape = builder.add(flow2)
    //Only shapes has the ~> operator available, the builder can return a shape from any streams component sources, flows
    // or sinks
    incrementerShape ~> multiplierShape

    // The flow shape needs an input port and an output port, which are taking from the components
    FlowShape(incrementerShape.in, multiplierShape.out)
  })
  firstSource.via(flowGraph).to(Sink.foreach(println)).run()

  // Exercise: create a flow from a sink and a source? Something like [->□ □->]
  def fromSinkAndSource[A,B](sink: Sink[A, _], source:Source[B, _]):Flow[A,B,_] = Flow.fromGraph(
    GraphDSL.create(){implicit builder =>
      import GraphDSL.Implicits._
      val  sourceShape = builder.add(source)
      val  sinkShape = builder.add(sink)
      // Since the two components are not connected, we don't have to do anything on step 3
      FlowShape(sinkShape.in, sourceShape.out)
    })

  // The previous component is also available in the akka streams core:
  Flow.fromSinkAndSource(sink, sourceGraph)
  // The danger is that if the elements going into the sink for example are finished then the source has
  // no way of stopping the stream because the components are not connected. To solve this problem, there is another
  // method in the akka dsl:
  Flow.fromSinkAndSourceCoupled(sink, sourceGraph)

  
  system.terminate()
}
