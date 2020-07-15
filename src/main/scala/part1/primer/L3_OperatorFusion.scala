package part1.primer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

object L3_OperatorFusion extends App{
  implicit val system = ActorSystem("OperatorFusion")
  implicit val materializer = ActorMaterializer()

  val source = Source[Int](1 to 100)
  val simpleFlow = Flow[Int].map(x => x + 1)
  val simpleFlow2 = Flow[Int].map(x => x * 10)
  val simpleSink = Sink.foreach[Int](println)

  // If I try to connect all this components, the following will run in the same actor. This is called operator/component
  // fusion (default behaviour and improves throughput). A simple CPU core will be used for complete process of every
  // element.
  source.via(simpleFlow).via(simpleFlow2).to(simpleSink).run()

  // However it can be harmfull in complex graphs like in
  val complexFlow = Flow[Int].map(x =>{
    Thread.sleep(1000)
    x + 1
  })
  val complexFlow2= Flow[Int].map(x =>{
    Thread.sleep(1000)
    x * 10
  })
  //This can kill performance
  source.via(complexFlow).via(complexFlow2).to(simpleSink)
  // To avoid that, we can parallelize the operations to increase throughput, for that we use an async boundaries
  source.via(complexFlow).async // Runs in one actor (we are marking an async boundary here)
    .via(complexFlow2).async // runs in another actor (another async boundary)
    .to(simpleSink) // runs on yet another actor
    //.run()

  //Technically, an async boundary contains everything from the previous boundary (if any).
  // ORDERING GUARANTEES:
  // The elements are computed sequentially from the source to the sink, and then next element is processed (there is no
  // async boundaries so everything is processed in a single actor (DETERMINISTIC!)
  Source[Int](1 to 3)
    .map(x => {println(s"Element A is ${x}"); x})
    .map(x => {println(s"Element B is ${x}"); x})
    .map(x => {println(s"Element C is ${x}"); x})
    .runWith(Sink.ignore) // Doesn't produce results

  // With Async boundaries, it is not guarantee the order in which the print lines will be printed in the console, the
  // only guarantee is that for an element, A is before B and B before C
  system.terminate()
}
