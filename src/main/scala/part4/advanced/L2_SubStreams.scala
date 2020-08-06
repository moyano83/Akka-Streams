package part4.advanced

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object L2_SubStreams extends App {
  implicit val system = ActorSystem("SubStreams")
  implicit val materializer = ActorMaterializer()

  // 1 - Grouping a stream by a function
  val wordSource = Source(List("I", "love", "Akka", "it", "is", "amazing"))
  // This will act like a source and we can attach a sink to it
  val streamWordGroup = wordSource.groupBy(30, word => if (word.isEmpty()) "\0" else word.toLowerCase().head)

  streamWordGroup.to(Sink.fold(0)((count, element) => {
    val newCount = count + 1
    println(s"I have received ${element} count is ${newCount}")
    newCount
  })).run()

  // Once you attach a consumer to a subflow, every single substream will have a different materialization for that component
  // (imagine a tree) which top node is the component that create the subflow

  // 2 - Merge Substreams back to the master component
  val textSource = Source(List(
    "I love akka streams",
    "Although they are very complicated",
    "learning akka streams at the moment",
    "Separate streams by length"
  ))

  val totalCharCountFuture = textSource
    .groupBy(2, x => x.length % 2)
    .map(x => x.length) //Imagine that here we do some expensive computation, and then we want to merge the streams back
    // This specifies the number of streams that can be merge at any one time, for unbounded substreams use the
    // mergeSubstream method instead
    .mergeSubstreamsWithParallelism(2)
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  totalCharCountFuture.onComplete({
    case Success(value) => println(s"Retrieved ${value}")
    case Failure(exception) => exception.printStackTrace()
  })

  // 3 - Splitting a stream into substreams when a condition is meet
  val text = "I love akka streams\n" +
    "Although they are very complicated\n" +
    "learning akka streams at the moment\n" +
    "Separate streams by length"

  // Imagine that you don't get to get the data into chunks but instead you have a huge data that you want to get into chunks
  val streamOfTextFuture = Source(text.toList) // Source of characters
    // When the incoming data character is equal to backslash, a new sub stream will be formed on the spot and all
    // further characters will be sent to that sub stream until the character incoming is equal to backslash again.
    .splitWhen(x => x == "\n")
    .filter(_ != "\n")
    .map(x => 1) //Expensive computation
    .mergeSubstreams
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()
  streamOfTextFuture.onComplete({
    case Success(value) => println(s"The stream has ${value} characters")
    case Failure(exception) => exception.printStackTrace()
  })

  // 4 - Flattening: There is also the equivalent to FlatMap in streams (each element spins up several streams that and them
  // we combine all of them). For that you need to choose if you want to concatenate the streams or merging them (elements of
  // all substreams are merged in no order)
  val simpleSource = Source(1 to 5)
  simpleSource.flatMapConcat(x => Source(List(x, 10 * x, 100 * x))).runWith(Sink.foreach(println))
  //The first parameter to flatMapMerge is the cap on the number of streams that can be merged at any time
  simpleSource.flatMapMerge(2, x => Source(List(x, 10 * x, 100 * x))).runWith(Sink.foreach(x => println(s"Merge: ${x}")))


  system.terminate()
}
