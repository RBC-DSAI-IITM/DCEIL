package in.ac.iitm.rbcdsai.dceil

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.Logging

/** CeilHarness is a coordinator for execution of DCEIL.
  * 
  * It coordinates calls into CeilCore and checks for convergence criteria
  * The input Graph must have an edge type of Long.
  *
  * High Level algorithm description:
  *
  *  Setup - Each vertex in the graph is assigned its own community.
  *  1. Each vertex attempts to increase graph modularity by changing to a neighboring community or remaining in its current community.
  *  2. Step 1 is repeated until progress is no longer made. The progress is measured by looking at the decrease in the number of
  *     vertices that change their community on each pass. If the change in progress is < minProgress more than progressCounter times
  *     the level is exited.
  *  3. The level is saved and each vertex is now labeled with a community.
  *  4. The graph is compressed, representing each community as a single node.
  *  5. Steps 1-4 are repeated on the compressed graph.
  *  6. Repeated until modularity is no longer improved
  * 
  * @constructor create a new harness with minProgress and progressCounter
  * @param minProgress the minimum progress
  * @param progressCounter the progress counter
  * 
  * @see CeilCore for low-level algorithm
  * @see HDFSCeilRunner for wrapper of CeilHarness
  *
  *  For details see:
  *  [[https://arxiv.org/abs/0803.0476 Fast unfolding of communities in large networks, 2008]]
  */
class CeilHarness(minProgress: Int, progressCounter: Int) {

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]) = {

    // Create graph
    val startTimeGraphCreation: Double = System.currentTimeMillis.toDouble / 1000L
    var ceilGraph = CeilCore.createCeilGraph(graph)
    val totalVertices = ceilGraph.numVertices

    val stopTimeGraphCreation: Double = System.currentTimeMillis.toDouble / 1000L
    val runTimeGraphCreation = stopTimeGraphCreation - startTimeGraphCreation
    // println("Create Graph Running time is : " + runn)

    // Operate on graph
    val startTimeGraphOperation: Double = System.currentTimeMillis.toDouble / 1000L
    var level = -1 // number of times the graph has been compressed
    var q = 0.0 // current modularity value
    
    var halt = false
    do {
      level += 1
      // println(s"\nStarting Ceil level $level")

      // Labels each vertex with its best community choice at this level of compression
      val (currentQ, currentGraph, passes) = CeilCore.ceil(sc, ceilGraph, minProgress, progressCounter, totalVertices)
      ceilGraph.unpersistVertices(blocking = false)
      ceilGraph = currentGraph

      saveLevel(sc, level, currentQ, ceilGraph)

      // If modularity increases by at least 0.001, compress the graph
      // and repeats halt immediately if the community labeling takes less
      // than 3 passes.

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
    * Override to specify save behavior. Not needed if saved at each level.
    */
  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {}

}
