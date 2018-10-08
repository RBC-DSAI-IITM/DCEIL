package in.ac.iitm.rbcdsai.dceil

import org.scalatest.FunSuite

/** IpAddressTests tests methods of IpAddress.*/
class IpAddressTests extends FunSuite {

  // test toString()
  test("toString should return String") {
    val ipAddressString = IpAddress.toString(10101010)
    
    assert(ipAddressString == "0.154.33.18")
  }

  // test toLong()
  test("toLong should return Long") {
    val ipAddressLong = IpAddress.toLong("10.10.10.10")

    assert(ipAddressLong == 168430090)
  }
}
