package us.quartyard.net

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import scala.concurrent.ExecutionContext.Implicits._

import scala.concurrent.Future

trait Serde[V] {
  def readFrom(is: InputStream): V
  def writeTo(os: OutputStream, v: V): Unit

  def deserialize(bytes: Array[Byte]): V = readFrom(new ByteArrayInputStream(bytes))
  def serialize(v: V): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    writeTo(baos, v)
    baos.toByteArray
  }
}


trait Transport[Msg] {
  val scheme: TransportScheme
  def sendTo(address: scheme.Address, msg: Msg)
}

trait TransportScheme {
  type Address
  def apply[Msg](recv: (Address, Msg) => Unit)(implicit serde: Serde[Msg]): Transport[Msg]
}


private class UDP(port: Int) extends TransportScheme {

  case class Address(host: String, port: Int)

  val outgoing = new DatagramSocket

  def apply[Msg](recv: (Address, Msg) => Unit)(implicit serde: Serde[Msg]) = new Transport[Msg] {

    val scheme = UDP.this

    Future {
      val incoming = new DatagramSocket(port)
      var bytes = new Array[Byte](65535)
      while (true) {
        val packet = new DatagramPacket(bytes, bytes.length)
        incoming.receive(packet)
        val sender = Address(packet.getAddress.getHostAddress, packet.getPort)
        recv(sender, serde.deserialize(packet.getData))
      }
    }

    override def sendTo(address: scheme.Address, msg: Msg): Unit = {
      val bytes = serde.serialize(msg)

      val packet = new DatagramPacket(bytes, bytes.length, InetAddress.getByName(address.host), address.port)
      outgoing.send(packet)
    }
  }
}

object UDP {
  def apply(port: Int): TransportScheme = new UDP(port)
}


