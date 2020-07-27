package part3.techniques

import java.util.Date

import scala.concurrent.duration._
import akka.pattern.ask
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

import scala.concurrent.Future

object L2_IntegratingWithExternalServices extends App {

  implicit val system = ActorSystem("IntegratingWithExternalServices")
  implicit val materializer = ActorMaterializer()
  implicit val customDispatcher = system.dispatchers.lookup("dedicated-dispatcher")

  def genericExternalService[A,B](element:A): Future[B] = ???

  // Design an akka Stream that integrates with a ficticious external API for a pager event (Whenever something breaks in
  // production an email or other type of notification is sent to the engineer on duty)
  case class PagerEvent(application:String, description:String, date: Date)

  val eventSource = Source(List(
    PagerEvent("AkkaInfra", "Infrastructure broke", new Date()),
    PagerEvent("FastDataPipeline", "Illegal element in the pipeline", new Date()),
    PagerEvent("AkkaInfra", "Service stop responding", new Date()),
    PagerEvent("SuperFrontEnd", "Button not working", new Date()),
    PagerEvent("AkkaInfra", "Infrastructure broke again", new Date())
  ))

  object PagerService{
    private val engineers = List("Daniel", "Jorge", "Matthew")
    // Let's assume the engineers here are on rotta, so each one of them gets a day
    private val emails =  Map[String, String](
      "Daniel" -> "daniel@test.com",
      "Jorge" -> "jorge@test.com",
      "Matthew" -> "matthew@test.com",
    )

    def processEvent(pager:PagerEvent):Future[String] = Future{
      // number of seconds in a day
      val engineerIndex = pager.date.toInstant.getEpochSecond / (24*3600) % engineers.length
      val engineer = engineers(engineerIndex.toInt)
      val engineerEmail = emails.get(engineer).getOrElse("")
      println(s"Sending engineer ${emails} a high priority notification ${pager}")
      Thread.sleep(1000)
      engineerEmail
    }
  }

  val infraEvents = eventSource.filter(pagerEvent => pagerEvent.application == "AkkaInfra")
  // The problem to retrieve the engineers emails associated to the events is that we cannot use map or use a regular flow
  // on pager events because our external service can only return a future so we'll use a different return type.
  // we use the mapAsync, which is different from a regular map in that the function that is used to process the elements
  // returns a future, so it is asynchronous. Parallelism determines how many futures can run at the same time, in case
  // any of this futures fails, the whole stream fails.
  //The map async guarantees the relative order of the elements, irrespective of the time it takes to complete each one of
  // them. If the important thing is to process as fast as we can but we don't care about the order, we can use
  // mapAsyncUnordered insted.
  val pageEngineerEmails = infraEvents.mapAsync(parallelism = 2)(event => PagerService.processEvent(event))

  val pageEngineerEmailsSink = Sink.foreach[String](email => println(s"Successfully sent notification to ${email}"))
  // Running futures in streams implies that we may end up running a lot of these futures so it's important that if you
  // run futures in a narco stream you should run them in their own execution context not on the actor systems dispatcher
  // because you may starve it for threats, that's why we have created our own dispatcher
  pageEngineerEmails.to(pageEngineerEmailsSink).run()


  // We can also replace the pager service by an actor, which is shown below:
  class PagerServiceActor extends Actor with ActorLogging {
    override def receive:Receive = {
      case pager:PagerEvent => sender() ! PagerService.processEvent(pager)
    }
  }
  // Then we can interact with this actor like this
  implicit val timeout = Timeout(2 seconds)
  val pagerActor = system.actorOf(Props[PagerServiceActor])
  val alternativeEngineerEmails = infraEvents.mapAsync(parallelism = 2)(event => (pagerActor ? event).mapTo[String])
  alternativeEngineerEmails.to(pageEngineerEmailsSink).run()

  // Important take, do not confuse mapAsync to async, async is the async boundary, which makes a component to run on a
  // separate actor

  Thread.sleep(6000)
  system.terminate()
}
