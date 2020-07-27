package part3.techniques

import akka.actor.ActorSystem
import akka.stream.Supervision.{Resume, Stop}
import akka.stream.{ActorAttributes, ActorMaterializer}
import akka.stream.scaladsl.{RestartSource, Sink, Source}

import scala.concurrent.duration._
import scala.util.Random

object L4_FaultTolerance extends App {

  implicit val system = ActorSystem("FaultTolerance")
  implicit val materializer = ActorMaterializer()

  // Summary of the different strategies against failure:
  //1 - Logging
  val faultySource = Source(1 to 10).map(x => if (x == 6) throw new RuntimeException() else x)
  // This logs errors at debug level, but because the default log level for akka is INFO, we need to override the default
  // config on application.conf
  // Notice that when an element throws an exception it cancels the streams. To recover from such exception, see 2
  faultySource.log("Tracking element").to(Sink.ignore)
  //.run()

  //2 - Gracefully terminate stream: Use recover method which needs a PF from a throwable to a value
  faultySource.recover {
    case r: RuntimeException => Int.MinValue
  }.log("Tracking element with recover").to(Sink.ignore)
  //.run()

  // 3 - RecoverWith: Instead of replacing a failure with a value, it replaces the stream with another stream and in case
  // it fails, it retries it n times (passed as a parameter)
  faultySource.recoverWithRetries(3, {
    case _: RuntimeException => Source(9 to 99)
  }).log("Recovered with retries").to(Sink.ignore)
  //.run()

  // 2 and 3 is a powerful way to restore a stream in case it fails
  // 4 - Backoff supervision: To recap back off supervision on actors, when an actor failed its supervisor automatically
  // tried to restart it after an exponentially increasing delay. This Backoff supervisor also add some randomness so that
  // if failed actors were trying to access a resource at the same time not all of them should attempt at that exact moment.
  // In the same fashion, stream operators that access external resources (BBDD, HTTP connections...) we can use stream
  // backoff supervissor:

  // minBackoff:  is the initial delay after the first attempt is made
  // maxBackoff: maximum daily after which this restart source will be declared failed.
  // randomFactor: Prevents multiple components to start at the same time
  // generatorFunction: a function that would be called in every attempt and needs to return another Source
  // So the way that this works is that this function is called first, and when you plug this restart source to whatever
  // consumer it has if the value here fails for some reason it will be swapped.
  val restartSource = RestartSource.onFailuresWithBackoff(
    minBackoff = 3 second,
    maxBackoff = 30 second,
    randomFactor = 0.2)(() => {
    val randomNumber = new Random().nextInt(20)
    Source(1 to 10).map(x => if (x == randomNumber) throw new RuntimeException else x)
  })

  restartSource.log("Restart backoff").to(Sink.ignore)
    //.run()

  // 5 - Supervision strategy on Streams
  // Unlike actors, akka stream operators are not automatically subject to a supervision strategy, by default they just fail
  val numbers = Source(1 to 20).map(x => if (x == 13) throw new RuntimeException("Bad luck") else x).log("Supervision")
  // With attributes allows us to pass a range of configurations, like the supervision strategy
  val supervisedNumbers = numbers.withAttributes(ActorAttributes.supervisionStrategy{
    // The supervision strategy can either be Resume (skip the faulty element), Stop (stop the stream) or Restart (same
    // than resume, but it will also clear the internal state of the consumer)
    case _:RuntimeException => Resume
    case _ => Stop
  })
  supervisedNumbers.to(Sink.ignore)
    .run()
  Thread.sleep(2000)
  system.terminate()
}
