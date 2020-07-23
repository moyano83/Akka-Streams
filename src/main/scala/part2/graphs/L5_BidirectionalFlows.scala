package part2.graphs

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, BidiShape, ClosedShape}

object L5_BidirectionalFlows extends App {
  implicit val system = ActorSystem("BidirectionalFlows")
  implicit val materializer = ActorMaterializer()
  val encryptionNumber = 10
  // Ofter we need to attach two flows that goes in opposite directions, in akka streams this is called bidirectional flow
  // The main applications for this types of graphs are:
  // encryption/decryption
  // serializing/deserializing
  // encoding/decoding
  // i.e. Encryption/Decryption: This function translates each caracter of the text by the number amount
  def encrypt(number: Int)(text: String) = text.map(c => (c + number).toChar)

  def decrypt(number: Int)(text: String) = text.map(c => (c - number).toChar)

  println(encrypt(3)("Akka"))
  println(decrypt(3)(encrypt(3)("Akka")))

  // Usually the above functionality stays under the same component, in case of akka streams, we can do the following:
  val bidirectFlow = GraphDSL.create() { implicit builder =>
    val encryptionFlowShape = builder.add(Flow[String].map(s => s.map(c => (c + encryptionNumber).toChar)))
    val decryptionFlowShape = builder.add(Flow[String].map(s => s.map(c => (c - encryptionNumber).toChar)))
    // We need an special shape named BiDiShape which takes four arguments, the input and output of the first and second
    // flows
    //BidiShape(encryptionFlowShape.in, encryptionFlowShape.out, decryptionFlowShape.in, decryptionFlowShape.out)
    // Which can be symplified by
    BidiShape.fromFlows(encryptionFlowShape, decryptionFlowShape)
  }
  //The above can be plug into another components:
  val unencryptedStrings = List("Akka", "is", "Awesome", "try", "it")
  val unencryptedSource = Source(unencryptedStrings)
  val encryptedSource = Source(unencryptedStrings.map(encrypt(encryptionNumber)))
  val encryptedSink = Sink.foreach[String](x => println(s"encrypted sink: ${x}"))
  val uncryptedSink = Sink.foreach[String](x => println(s"unencrypted sink: ${x}"))

  val crypthoRunnableGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val unencryptedSourceShape = builder.add(unencryptedSource)
    val encryptedSourceShape = builder.add(encryptedSource)
    val bidiFlow = builder.add(bidirectFlow)
    val encryptedSinkShape = builder.add(encryptedSink)
    val unencryptedSinkShape = builder.add(uncryptedSink)

    unencryptedSourceShape ~> bidiFlow.in1; bidiFlow.out1 ~> encryptedSinkShape

    // Now, to test this we need to reverse the way the components are tied in, so the output of 1 is the input of 2. For
    // that we do the following (notice the <~ instead of the ~>)
    unencryptedSinkShape <~ bidiFlow.out2; bidiFlow.in2 <~ encryptedSourceShape
    ClosedShape
  })
  crypthoRunnableGraph.run()

  system.terminate()
}
