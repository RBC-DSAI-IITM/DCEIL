package in.ac.iitm.rbcdsai.dceil

import java.net.InetAddress
import java.nio.ByteBuffer

/** IpAddress provides helper methods for IP address representation.*/
object IpAddress {

  /** toString converts Long IP address to String.*/  
  def toString(address: Long): String = {
    val byteBuffer = ByteBuffer.allocate(8)
    val addressBytes = byteBuffer.putLong(address)

    // The below is needed because we don't have an unsigned Long, and passing
    // a byte array with more than 4 bytes causes InetAddress to interpret it
    // as a (bad) IPv6 address.
    val tmp = new Array[Byte](4)
    Array.copy(addressBytes.array, 4, tmp, 0, 4)
    InetAddress.getByAddress(tmp).getHostAddress()
  }

  /** toLong converts String IP address to Long.*/
  def toLong(_address: String): Long = {
    val address = try {
      InetAddress.getByName(_address)
    } catch {
      case e: Throwable =>
        throw new IllegalArgumentException("Could not parse address: " +
          e.getMessage)
    }
    val addressBytes = address.getAddress
    val byteBuffer = ByteBuffer.allocate(8)
    addressBytes.length match {
      case 4 =>
        byteBuffer.put(Array[Byte](0,0,0,0)) // Needs a filler
        byteBuffer.put(addressBytes)
      case n =>
        throw new IndexOutOfBoundsException("Expected 4 byte address, got "
          + n)
    }
    byteBuffer.getLong(0)
  }
}
