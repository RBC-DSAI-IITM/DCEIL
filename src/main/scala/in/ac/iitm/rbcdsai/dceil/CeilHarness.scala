package in.ac.iitm.rbcdsai.dceil

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.Logging

/** A coordinator for execution of DCEIL.
  *
  * The input Graph must have an edge type of Long.
  *
  * For low-level algorithm see CeilCore.
  * This class coordinates calls into CeilCore and checks for convergence criteria
  *
  * Two hooks are provided to allow custom behavior:
  *    -saveLevel  override to save the graph (vertices/edges) after each phase of the process
  *    -finalSave  override to specify a final action/save when the algorithm has completed. (not necessary if saving at each level)
  *
  * High Level algorithm description.
  *
  *  Set up - Each vertex in the graph is assigned its own community.
  *  1.  Each vertex attempts to increase graph modularity by changing to a neighboring community, or remaining in its current community.
  *  2.  Repeat step 1 until progress is no longer made
  *         - progress is measured by looking at the decrease in the number of vertices that change their community on each pass.
  *           If the change in progress is < minProgress more than progressCounter times we exit this level.
  *  3. -saveLevel, each vertex is now labeled with a community.
  *  4. Compress the graph representing each community as a single node.
  *  5. repeat steps 1-4 on the compressed graph.
  *  6. repeat until modularity is no longer improved
  *
  *  For details see:  Fast unfolding of communities in large networks, Blondel 2008
  */
class CeilHarness(minProgress: Int, progressCounter: Int) {

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]) = {

    val startTimeGraphCreation: Double = System.currentTimeMillis.toDouble / 1000L
    var ceilGraph = CeilCore.createCeilGraph(graph)
    val n = ceilGraph.vertices.count() // n is total Graph Vertices
    // println(" Total Vertices in graph : " + n)
    val stopTimeGraphCreation: Double = System.currentTimeMillis.toDouble / 1000L
    val runTimeGraphCreation = stopTimeGraphCreation - startTimeGraphCreation
    // println("Create Graph Running time is : " + runn)

    val startTimeGraphOperation: Double = System.currentTimeMillis.toDouble / 1000L
    var level = -1 // number of times the graph has been compressed
    var q = 0.0 // current modularity value
    var halt = false
    do {
      level += 1
      // println(s"\nStarting Ceil level $level")

      // Labels each vertex with its best community choice at this level of compression
      val (currentQ, currentGraph, passes) = CeilCore.ceil(sc, ceilGraph, minProgress, progressCounter, n)
      ceilGraph.unpersistVertices(blocking = false)
      ceilGraph = currentGraph

      saveLevel(sc, level, currentQ, ceilGraph)

      // If modularity increases by at least 0.001, compresses the graph
      // and repeats halt immediately if the community labeling takes less
      // than 3 passes.

      // println(s"if ($passes > 2 && $currentQ > $q + 0000001 )")
      //if (currentQ > q + 0.0000001) {
      if (passes > 2 && currentQ > q + 0.0000001) {
        q = currentQ
        val startTimeGraphCompression: Double = System.currentTimeMillis.toDouble / 1000L
        ceilGraph = CeilCore.compressGraph(ceilGraph)
        val stopTimeGraphCompression: Double = System.currentTimeMillis.toDouble / 1000L
        val runningTimeGraphCompression = stopTimeGraphCompression - startTimeGraphCompression
        // println("Compress Graph Running time is : " + runningTime)
      } else {
        halt = true
      }

    } while (!halt)

    val stopTimeGraphOperation: Double = System.currentTimeMillis.toDouble / 1000L
    val runningTimeGraphOperation = stopTimeGraphOperation - startTimeGraphOperation
    // println("Total algo running time is : " + runnt)
    
    finalSave(sc, level, q, ceilGraph)
  }

  /** Saves the graph at the given level of compression with community labels.
    * 
    * level 0 = no compression
    * override to specify behavior
    */
  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {}

  /** Completes any final save actions required
    *
    * override to specify save behavior
    */
  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {}

}
