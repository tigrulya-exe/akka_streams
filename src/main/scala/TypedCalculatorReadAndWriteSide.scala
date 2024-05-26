import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.ClosedShape
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.GraphDSL.Implicits.{SourceShapeArrow, flow2flow, port2flow}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka_typed.CalculatorRepository.session.db
import akka_typed.CalculatorRepository.{Result, getLatestResult, updateResultAndOffset}
import akka_typed.TypedCalculatorWriteSide._
import slick.jdbc.{GetResult, PositionedResult}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}

object  akka_typed{

  trait CborSerialization

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide{
    sealed trait Command
    case class Add(amount: Int) extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int) extends Command

    sealed trait Event
    case class Added(id:Int, amount: Int) extends Event
    case class Multiplied(id:Int, amount: Int) extends Event
    case class Divided(id:Int, amount: Int) extends Event

    final case class State(value:Int) extends CborSerialization
    {
      def add(amount: Int): State = copy(value = value + amount)
      def multiply(amount: Int): State = copy(value = value * amount)
      def divide(amount: Int): State = copy(value = value / amount)
    }

    object State{
      val empty = State(0)
    }


    def handleCommand(
                       persistenceId: String,
                       state: State,
                       command: Command,
                       ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toInt, amount)
          Effect
            .persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }

  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]){
    implicit val materializer = system.classicSystem
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    var (offset, latestCalculatedResult) = {
      val result = getLatestResult
      (result.offset, result.state)
    }

    implicit val dbSession: SlickSession = CalculatorRepository.session
    system.whenTerminated.onComplete { _ => dbSession.close() }

    private val eventSource = {
      val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val startOffset: Long = if (offset == 1L) 1L else offset + 1L
      readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)
    }

    private val updatedStatesFlow = Flow.fromFunction[EventEnvelope, Result] { envelope =>
        println(s"Map envelope to domain entity: ${envelope.event}'")
        toUpdatedState(envelope.event, envelope.sequenceNr)
    }

    private val updateLocalStateSink = Sink.foreach[Result] { result =>
      println(s"Update local state from '$latestCalculatedResult' to '${result.state}'")
      latestCalculatedResult = result.state
    }

    private val updateDbSink = Slick.sink[Result] { result: Result =>
        println(s"Update db state to '${result.state}'")
        updateResultAndOffset(result)
    }

    val graph = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      val eventSourceNode = builder.add(eventSource)
      val updatedStatesFlowNode = builder.add(updatedStatesFlow)
      val updateLocalStateSinkNode = builder.add(updateLocalStateSink)
      val updateDbSinkNode = builder.add(updateDbSink)

      val eventBroadcast = builder.add(Broadcast[Result](2))

      eventSourceNode ~> updatedStatesFlowNode
      updatedStatesFlowNode ~> eventBroadcast

      eventBroadcast.out(0) ~> updateLocalStateSinkNode
      eventBroadcast.out(1) ~> updateDbSinkNode

      ClosedShape
    }

    private def toUpdatedState(event: Any, offset: Long): Result = {
      val newState = event match {
        case Added(_, amount) => latestCalculatedResult + amount
        case Multiplied(_, amount) => latestCalculatedResult * amount
        case Divided(_, amount) => latestCalculatedResult / amount
      }

      Result(newState, offset)
    }
  }

  object CalculatorRepository {
    implicit lazy val session: SlickSession = SlickSession.forConfig("slick-postgres")
    import session.profile.api._

    case class Result(state: Double, offset: Long)

    implicit object ResultDeserializer extends GetResult[Result] {
      def apply(rs: PositionedResult): Result = {
        Result(rs.nextDouble(), rs.nextLong())
      }
    }

    def getLatestResult(
        implicit executionContextExecutor: ExecutionContextExecutor): Result = {

      val query = sql"select calculated_value, write_side_offset from public.result where id = 1;"
        .as[Result]
        .headOption

      val resultFuture = db.run(query).map {
        case Some(value) => value
        case None => throw new IllegalStateException("Initial state value not found")
      }
      Await.result(resultFuture, 10.seconds)
    }

    def updateResultAndOffset(result: Result) = {
      sqlu"""
        UPDATE public.result
        SET calculated_value = ${result.state}, write_side_offset = ${result.offset}
        WHERE id = 1
        """
    }
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{
      ctx =>
        val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeActorRef ! Add(10)
        writeActorRef ! Multiply(2)
        writeActorRef ! Divide(5)

        Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    implicit  val system: ActorSystem[NotUsed] = ActorSystem(akka_typed(), "akka_typed")

    val readSide = TypedCalculatorReadSide(system)
    RunnableGraph.fromGraph(readSide.graph).run()
  }

}