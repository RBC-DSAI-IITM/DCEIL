package in.ac.iitm.rbcdsai.dceil

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.log4j.{Level,Logger}
import org.scalatest.FunSuite

/** CeilCoreTests tests methods of CeilCore.*/
class CeilCoreTests extends FunSuite {

  val sc = new SparkContext("local[*]", "DCEIL tests")
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  var edgeRDD = sc.textFile("file:/Users/Synchon/Downloads/email-Eu-core.txt").map(row => {
    val tokens = row.split(" ").map(_.trim())
    new Edge(tokens(0).toLong, tokens(1).toLong, 1L)
  })
  val graph = Graph.fromEdges(edgeRDD, None)
  val ceilGraph = CeilCore.createCeilGraph(graph)
  
  // test createCeilGraph
  test("createCeilGraph should return the graph specific for CEIL") {
    // test CEIL graph properties
    assert(ceilGraph.numEdges == 25571)
    assert(ceilGraph.numVertices == 1005)
  }

  // test ceil
  test("ceil should return the modularity, graph and number of passes") {
    val totalVertices = ceilGraph.numVertices
    val (modularity, _, passes) = CeilCore.ceil(sc, ceilGraph, 2000, 1, totalVertices)
    assert(modularity == 0.04527570967805397)
    assert(passes == 2)
  }
 
  // test compressGraph
  test("compressGraph should return the compressed graph") {
    val compressedCeilGraph = CeilCore.compressGraph(ceilGraph)
    assert(compressedCeilGraph.numVertices == 1005)
    assert(compressedCeilGraph.numEdges == 16064)
  }
}
