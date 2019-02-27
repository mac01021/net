package us.quartyard.net.raft

import us.quartyard.net.Serde

/**
  * Created by mac01021 on 2/25/19.
  */
trait Fsm {
  type State
  type Cmd

  val cmdSerde: Serde[Cmd]
  val stateSerde: Serde[State]

  def apply(state: State, cmd: Cmd): State
}
