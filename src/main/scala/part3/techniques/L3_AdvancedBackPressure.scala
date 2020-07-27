package part3.techniques

import java.util.Date

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import scala.concurrent.duration._
object L3_AdvancedBackPressure extends App {

  implicit val system = ActorSystem("AdvancedBackPressure")
  implicit val materializer = ActorMaterializer()

  // Until now, the way we had to control how to handle back pressure was like this:
  val controllerFlow = Flow[Int].map(_ * 2).buffer(10, OverflowStrategy.dropHead)

  // Now an example of a more advanced backpressure control
  case class PagerEvent(description: String, nInstances:Int = 1, date: Date = new Date())

  case class Notification(email: String, pagerEvent: PagerEvent)

  val eventList = List(
    PagerEvent("Service discovery failed"),
    PagerEvent("Illegal elements in the data pipeline"),
    PagerEvent("Number of Http 500 spiked"),
    PagerEvent("Service stopped responding")
  )

  val eventSource = Source(eventList)
  val onCallEngineer = "jorge@test.com" // simulate fast service to fetch engineers

  // Imagine that we have a sink to send the notification
  def sendEmail(notification: Notification) =
    println(s"Sending an email to ${notification.email} with ${notification.pagerEvent}")

  val notificationSink = Flow[PagerEvent].map(e => Notification(onCallEngineer, e)).to(Sink.foreach[Notification](sendEmail))

  //eventSource.to(notificationSink).run()

  // Assume that the notification service is very slow, notification sink will try to slow down the emissor with
  // backpressure, but this might cause issues because the source might not be able to backpressure like for example in
  // timer sources.
  // A way to deal with this is instead of buffering the events on our flow, we can aggregate events into a single one
  // using the method conflate
  def sendEmailSlow(notification:Notification) = {
    Thread.sleep(1000)
    println(s"Sending a slow email to ${notification.email} with ${notification.pagerEvent}")
  }
  // We can create an aggregation float that will look something like this. `conflate` is a method which acts like fold,
  // that is it combines elements but it emits the result only when the downstream sends demand.
  def aggregateNotificationFlow = Flow[PagerEvent].conflate((ev1,ev2) => {
    val instances = ev1.nInstances + ev2.nInstances
    PagerEvent(s"You have ${instances} events that requires your attention", instances)
  })
  .map(resultingEvent => Notification(onCallEngineer, resultingEvent))

  // We put an async here because we want the flow and sink to run on separate actos to see the effects of the
  // backpressure (otherwise they'll run in a single thread).
  // Because conflate method never backed pressured, basically where decoupling the upstream rate from the downstream rate.
  eventSource.via(aggregateNotificationFlow).async.to(Sink.foreach[Notification](x => sendEmailSlow(x))).run()

  // For slow producers we can use the following technique (fast consumer slow producer) extrapolate/expand:
  val slowCounter = Source(Stream.from(1)).throttle(1,1 second)
  val hungrySink = Sink.foreach(println)

  // The idea is to insert a new element in between the above two, and you decide how to fill the gaps between each
  // emission of the Source. extrapolate needs a function that creates an iterator from each element.
  val extrapolator = Flow[Int].extrapolate(element=> Iterator.from(element))

  // There is another method called expand, which operates like an extrapolate, but instead of doing it like the
  // extrapolator that creates an iterator when there is demand, the expander creates this iterator all the time.
  val expander = Flow[Int].expand(element=> Iterator.continually(element))


  //slowCounter.via(extrapolator).to(hungrySink).run()
  slowCounter.via(expander).to(hungrySink).run()


  Thread.sleep(1000)
  system.terminate()

}
