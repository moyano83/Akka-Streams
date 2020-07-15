package playground

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}

object Playground extends App{
  implicit val system = ActorSystem("Playground")
  implicit val materializer = ActorMaterializer()

  Source.single("Hello World").to(Sink.foreach(println)).run()
}