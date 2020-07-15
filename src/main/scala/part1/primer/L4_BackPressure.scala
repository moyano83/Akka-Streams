package part1.primer

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

object L4_BackPressure extends App{
  implicit val system = ActorSystem("BackPressure")
  implicit val materializer = ActorMaterializer()

  //BackPressure is one of the fundamental feature of Reactive Streams. Key Concepts:
  // 1 - Elements flow as response to demand from consumers
  //    a - If the consumers are fast there is no problems
  //    b - If the consumers are slow there is a problem
  // In case of 1b, the consumer would send a signal to the consumers to slow down (message will pass backwards) so it has
  // time to process. This is known as BackPressure.

  val  fastSource = Source(1 to 1000)
  val slowSink = Sink.foreach[Int](x =>{
    Thread.sleep(500)
    println(s"Element is ${x}")
  })

  // Since this two compoents are run in different actors, we can use backpressure to slow down the emission of elements
  val simpleFlow = Flow[Int].map(x => {
    println(s"Element ${x}")
    x +1
  })
  // We can see that automatically the elements are processed in batches. What happens is that the flow has a buffer
  // (defaults to 16) that is filled, and once it is the flow send a backpressure signal to the source to slow down the
  // emission of new elements, until the slowSink processes them (this happens over and over again automatically)
  fastSource.async.via(simpleFlow).to(slowSink).run()

  // Batch processor can behave in multiple ways:
  // 1 - Try to slow down (if possible)
  // 2 - Buffer elements until it is more demand
  // 3 - Drop elements from the buffer if it overflows (only behaviour that us as programmers can affect)
  // 4 - Tear down or kill the whole stream (known as failure)

  // A demonstration of how to affect previous point 3, is shown below
  val bufferedFlow = simpleFlow.buffer(20, OverflowStrategy.dropTail)
  // multiple strategies can be picked on overflow strategy
  // 1 - dropHead: Drop the oldest element in the buffer to make space for the new one:
  //     a) messages 1 => 16 nobody is backpressured
  //     b) messages 17 => 26 Flow will buffer
  //     c) messages n-16 => n: Sink will process the buffer
  // 2 - dropTailL Drops the newest elements
  // 3 - drops the exact elements we added (keeps the buffer)
  // 4 - drop the entire buffer
  // 5 - emit backpressure signal
  // 6 - Fail
  fastSource.async.via(bufferedFlow).to(slowSink).run()

  // There is also a manual signal to apply backpressure, this is called throttling:
  import scala.concurrent.duration._
  //This sets the flow to emit 2 element at most every second
  fastSource.throttle(20, 5 seconds).runWith(Sink.foreach(println))


  system.terminate()
}
