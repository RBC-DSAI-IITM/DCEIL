package in.ac.iitm.rbcdsai.dceil

import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.log4j.{Level,Logger}
import org.scalatest.FunSuite

/** CeilSpec tests methods of CeilCore and CeilHarness.*/
class CeilSpec extends FunSuite {

  // initialize SparkContext
  val sc = new SparkContext("local[*]", "DCEIL tests")
  // suppress logging
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  // read file
  val testDirectory = new File(".").getCanonicalPath
  var edgeRDD = sc.textFile(testDirectory + "/src/test/scala/in/ac/iitm/rbcdsai/dceil/email-Eu-core.txt").map(row => {
    val tokens = row.split(" ").map(_.trim())
    new Edge(tokens(0).toLong, tokens(1).toLong, 1L)
  })
  // generate graph
  val graph = Graph.fromEdges(edgeRDD, None)
  // generate CEIL graph
  val ceilGraph = CeilCore.createCeilGraph(graph)
 
  // test CeilCore.createCeilGraph()
  test("createCeilGraph should return the graph specific for CEIL") {
    // test CEIL graph properties
    assert(ceilGraph.numEdges == 25571)
    assert(ceilGraph.numVertices == 1005)
  }

  // test CeilCore.ceil()
  test("ceil should return the modularity, graph and number of passes") {
    // test CEIL operation
    val totalVertices = ceilGraph.numVertices
    val (modularity, _, passes) = CeilCore.ceil(sc, ceilGraph, 2000, 1, totalVertices)
    assert(modularity == 0.04527570967805397)
    assert(passes == 2)
  }
 
  // test CeilCore.compressGraph()
  test("compressGraph should return the compressed graph") {
    // test CEIL graph compression
    val compressedCeilGraph = CeilCore.compressGraph(ceilGraph)
    assert(compressedCeilGraph.numVertices == 1005)
    assert(compressedCeilGraph.numEdges == 16064)
  }

  // test CeilHarness.run()
  test("run should execute the program") {
    // test proper execution of the program
    val runner = new CeilHarness(2000, 1)
    runner.run(sc, graph)
  }

}
