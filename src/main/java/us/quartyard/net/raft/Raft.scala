package us.quartyard.net.raft

import java.io.{InputStream, OutputStream}
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import us.quartyard.net.{Serde, Transport, TransportScheme}

import scala.concurrent.{Future, TimeoutException}
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits._

class Raft[StateMachine <: Fsm] (val fsm: StateMachine, val transportScheme: TransportScheme, minTimeout: Int, maxTimeout: Int) {
  var shouldRun = true

  sealed trait Rpc
  implicit object wireFmt extends Serde[Rpc] {
    object rpcs {

      case class AppendEntriesRequest(cmds: Seq[fsm.Cmd]) extends Rpc

      case class AppendEndriesResponse() extends Rpc

      case class VoteRequest() extends Rpc

      case class VoteResponse() extends Rpc

    }

    override def readFrom(is: InputStream): Rpc = is.read() match {
      case 1 => ???
      case 2 => ???
      case 3 => ???
      case 4 => ???
    }
    override def writeTo(os: OutputStream, rpc: Rpc): Unit = rpc match {
      case rpcs.VoteRequest() => ???
      case rpcs.VoteResponse() => ???
      case rpcs.AppendEntriesRequest(cmds) => ???
      case rpcs.AppendEndriesResponse() => ???
    }
  }

  sealed class Event
  case class Tick(timestamp: Long) extends Event
  case class ReceivedRpc(rpc: Rpc, addr: transportScheme.Address) extends Event

  class Protocol(transport: Transport[Rpc]) {
    import wireFmt.rpcs._

    sealed trait RaftState {
      val term: Long
      def respondTo: PartialFunction[Event, RaftState]
    }

    def initialState: RaftState = new following(0)


    class following(val term: Long) extends RaftState {

      override def respondTo: PartialFunction[Event, RaftState] = {
        case Tick(ts) => ???
        case ReceivedRpc(rpc, sender) => rpc match {
          case VoteRequest() => ???
        }
      }

    }

    class leading(val term: Long) extends RaftState {

      override def respondTo: PartialFunction[Event, RaftState] = {
        case Tick(ts) => ???
        case ReceivedRpc(rpc, sender) => rpc match {
          case VoteRequest() => ???
        }
      }

    }

    class candidacy(val term: Long) extends RaftState {

      override def respondTo: PartialFunction[Event, RaftState] = {
        case Tick(ts) => ???
        case ReceivedRpc(rpc, sender) => rpc match {
          case VoteRequest() => ???
        }
      }

    }

  }

  private def now = System.currentTimeMillis

  private val incoming = new ArrayBlockingQueue[Event](64)

  private val transport = transportScheme[Rpc] { (addr: transportScheme.Address, rpc: Rpc) =>
    incoming.put(ReceivedRpc(rpc, addr))
  }

  private val protocol = new Protocol(transport)
  Future {
    var raftState = protocol.initialState
    while (shouldRun) try {
      val event = incoming.poll(maxTimeout, TimeUnit.MILLISECONDS)
      raftState = raftState.respondTo(event)
    } catch {
      case timeout: TimeoutException => raftState.respondTo(Tick(now))
    }
  }

  Future {
    val random = new Random
    while(shouldRun) {
      Thread.sleep(random.nextInt(maxTimeout-minTimeout)+minTimeout)
      incoming.offer(Tick(now))
    }
  }

  def stop: Unit = {
    shouldRun = false
  }


}
