package part4.advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.stage._
import akka.stream._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Random, Success}

object L4_CustomOperators extends App {
  implicit val system = ActorSystem("CustomOperators")
  implicit val materializer = ActorMaterializer()

  // Create a custom source that emits random numbers until cancelled
  //GraphStage takes a type parameter that is the shape of the component that I am going to define
  // The pattern here is the following:
  // 1 - Create a class that extends GraphStage with the appropriate Shape we know the component will take (Source)
  // 2 - Define the ports and the component specific members
  // 3 - Construct a new shape
  // 4 - Construct the logic (define mutable state, set the handlers for the ports, etc...)

  // Step 1
  class RandomNumberGenerator(max: Int) extends GraphStage[SourceShape[Int]] {

    val randomGenerator = new Random()
    // Step 2
    val outPort = Outlet[Int]("randomGenerator")

    // Step 3
    override def shape: SourceShape[Int] = SourceShape(outPort)

    // Step 4
    // Implementation of the class is in the following method from GraphStageLogic
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // We need to set handlers on our ports, this handlers are never called concurrently, so you can safely access
      // mutable state on this handler, but on the other hand, you should never expose mutable state outside this handlers
      // like in the future on complete, because you'll break actor encapsulation
      setHandler(outPort, new OutHandler {
        // This is the method that would be called (is a callback) when there is demand downstream
        // The push method pushes an element in the port passed
        override def onPull(): Unit = push(outPort, randomGenerator.nextInt(max))
      })
    }
  }

  // Now to create an actual source based on the definition above, we do the following:
  val randomGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100))

  // Once we plug the source to any other component and we start the graph, we are materializing the component, and the
  // method createLogic inside the class will be called then the object return will be constructed.
  //randomGeneratorSource.runWith(Sink.foreach(println))

  // Create a custom sink that prints elements in batches of a given size
  class Batcher(batchSize: Int) extends GraphStage[SinkShape[Int]] {
    val inport = Inlet[Int]("BatcherShape")

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      //Because we are going to test this component with the source defined before, we need one of the two to start
      // requesting or sending the signal so the other component starts processing. otherwise the two components would
      // remain idle waiting for down/upstream to request more elements. For that we override this method:
      override def preStart(): Unit = {
        pull(inport) // This signals demand for new elements, starting the stream
      }

      // We need to create an element to store the elements until they need to be flushed
      val queue = new mutable.Queue[Int]
      setHandler(inport, new InHandler {
        // Call back called when the method wants to send me an element
        override def onPush(): Unit = {
          val element = grab[Int](inport)
          queue.enqueue(element)
          if (queue.size >= batchSize) {
            println(s"new batch: ${queue.dequeueAll(_ => true).mkString("[", "'", "]")}")
          }
          pull(inport) // send demand upstream
        }

        // If the stream upstream terminates, we want to print the elements remaining in the queue, for that we override the
        // following method that will be called when this happens
        override def onUpstreamFinish(): Unit = {
          if (!queue.isEmpty) {
            println(s"new batch: ${queue.dequeueAll(_ => true).mkString("[", "'", "]")}")
            println("Stream Finished")
          }
        }
      })
    }

    override def shape: SinkShape[Int] = SinkShape[Int](inport)
  }

  //randomGeneratorSource.runWith(Sink.fromGraph(new Batcher(10)))

  /**
   * Exercise: Create a custom, it should be a filter flow with a custom filter function and one input an one output port
   *
   */
  class SimpleFilter[T](filterFunction: T => Boolean) extends GraphStage[FlowShape[T, T]] {
    val inPort = Inlet[T]("FlowInlet")
    val outPort = Outlet[T]("FlowOutlet")

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // The outport is very simple
      setHandler(outPort, new OutHandler {
        override def onPull(): Unit = pull(inPort)
      })
      // My filtering logic will be here
      setHandler(inPort, new InHandler {
        override def onPush(): Unit = {
          try {
            val element = grab[T](inPort)
            if (filterFunction(element)) push(outPort, element)
            else pull(inPort) // we need to ask for the next element if the predicate does not match
          } catch {
            // What happens if the predicate throws an exception? we need to fail the graph
            case ex: Exception => failStage(ex)
          }
        }
      })

    }

    override def shape: FlowShape[T, T] = FlowShape(inPort, outPort)
  }

  val myFilter = Flow.fromGraph(new SimpleFilter[Int](_ % 2 == 0))

  // BackPressure works out of the box
  //randomGeneratorSource.via(myFilter).runWith(Sink.fromGraph(new Batcher(10)))

  /**
   * Materialized values in graph stages: Create a flow that counts the number of elements that goes through it
   */
  // The difference between the GraphStage and the GraphStageWithMaterializedValue is that the later takes the shape of
  // the component (in this case a flow shape from T to T) and the materialized value, which in this case is a Future[Int]
  class CounterFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Int]] {
    val inPort = Inlet[T]("CounterIn")
    val outPort = Outlet[T]("CounterOut")

    override val shape = FlowShape(inPort, outPort)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Int]) = {
      // When the upstream or downstream finishes we need to complete a future with the value of the number of elements
      // that has gone through this Flow, for that we need a promise
      val promise = Promise[Int]

      val logic = new GraphStageLogic(shape) {
        var counter = 0
        setHandler(outPort, new OutHandler {
          override def onPull(): Unit = pull(inPort)

          // We need to complete the promise in this method which signals when the graph finishes
          override def onDownstreamFinish(): Unit = {
            promise.success(counter)
            super.onDownstreamFinish()
          }
        })

        setHandler(inPort, new InHandler {
          override def onPush(): Unit = {
            val element = grab(inPort)
            counter += 1
            push(outPort, element)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(counter)
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.success(counter)
            super.onUpstreamFailure(ex)
          }
        })
      }
      (logic, promise.future)
    }
  }

  val counter = Flow.fromGraph(new CounterFlow[Int])

  val countFuture = randomGeneratorSource
    .viaMat(counter)(Keep.right)
    .to(Sink.foreach(println))
    .run()

  countFuture.onComplete{
    case Success(value) => println(s"The number of elements is ${value}")
    case Failure(exception) => exception.printStackTrace()
  }

  Thread.sleep(5000)
  system.terminate()
}
