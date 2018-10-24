package in.ac.iitm.rbcdsai.dceil

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag

/** CeilCore provides low-level methods for CEIL computation.

  * Generally used by CeilHarness to coordinate the correct execution of the algorithm
  * through its several stages.
  * 
  * For the sequential algorithm see:
  * [[https://www.ijcai.org/Proceedings/15/Papers/297.pdf CEIL: A Scalable,
  * Resolution Limit Free Approach for Detecting Communities in
  * Large Networks, 2015]]
  */
object CeilCore {
  /** Creates a CEIL graph.
    * 
    * @param graph the initial graph
    * @return a graph which can be used for CEIL computation.
    */
  def createCeilGraph[VD: ClassTag](graph: Graph[VD, Long]): Graph[VertexState, Long] = {

    val outgoingEdgesMapFunc = (e: EdgeTriplet[VD, Long]) =>
    Iterator((e.srcId, e.attr), (e.dstId, e.attr))

    val outgoingEdgesReduceFunc = (e1: Long, e2: Long) => e1 + e2
    val outgoingEdges = graph.mapReduceTriplets(outgoingEdgesMapFunc,
      outgoingEdgesReduceFunc)

    val ceilGraph = graph.outerJoinVertices(outgoingEdges)((id, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      val state = new VertexState()
      state.community = id
      state.changed = false
      state.communityTotalEdges = weight
      state.internalEdges = 0L
      state.outgoingEdges = weight
      state.internalVertices = 1L
      state.communityInternalEdges = 0L
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    return ceilGraph
  }

  /** Labels each vertex with a community to maximize global modularity but
    * does not compress the graph.
    * 
    * @param sc the SparkContext
    * @param graph the initial graph
    * @param minProgress the minimum progress
    * @param progressCounter the progress counter
    * @totalGraphVertices the number of vertices
    * @return 
   */
  def ceil(
    sc: SparkContext,
    graph: Graph[VertexState, Long],
    minProgress: Int = 1,
    progressCounter: Int = 1,
    totalGraphVertices: Long): (Double, Graph[VertexState, Long], Int) = {

    var ceilGraph = graph.cache()
    //ceilGraph.vertices.foreach(println)

    // Gathers community information from local neighborhood of each vertex.
    var msgRDD = ceilGraph.mapReduceTriplets(sendMsg, mergeMsg).cache()
    //msgRDD.foreach(println)
    //msgRDD.saveAsTextFile("/home/akash/Documents/Spark_Program/01_test/Output_MsgRDD")
    msgRDD.count() //materializes the msgRDD and caches it in memory

    var updated = 0L - minProgress // I don't get this concept, you can simply initialize from zero, as if you see flow of code, inside do {even = True and if(even) updated =0.
    var even = false
    var count = 0
    val maxIter = 100000
    var stop = 0
    var updatedLastPhase = 0L
    var CEILScore = 0.0
    var prevCEILScore = 0.0
    var diffInCEILScores = 0.0

    do {
      count += 1
      even = !even
      // Labels each vertex with its best community based on neighboring
      // community information.
      val startTime1: Double = System.currentTimeMillis.toDouble / 1000
      val labeledVertices = ceilVertJoin(ceilGraph, msgRDD, totalGraphVertices, even).cache()
      labeledVertices.count()
      val stopTime1: Double = System.currentTimeMillis.toDouble / 1000
      val T1 = stopTime1 - startTime1
      // println("Running time is 0 : " + rT1)

      val startTime2: Double = System.currentTimeMillis.toDouble / 1000
      val updatedVertex = labeledVertices.map({ case (id, data) =>
        (id, data) }).cache()
      updatedVertex.count()

      var prevGraph = ceilGraph
      ceilGraph = ceilGraph.outerJoinVertices(updatedVertex)((id, old, newOpt) => newOpt.getOrElse(old))
      ceilGraph.triplets.count()
      //ceilGraph.triplets.foreach(println)      
      //ceilGraph.vertices.foreach(println)
      updatedVertex.unpersist(blocking = false)
      labeledVertices.unpersist(blocking = false)
      prevGraph.unpersistVertices(blocking = false)

      val stopTime2: Double = System.currentTimeMillis.toDouble / 1000
      val T2 = stopTime2 - startTime2
      // println("Running time is 1 : " + rT_a)

      val startTime3: Double = System.currentTimeMillis.toDouble / 1000
      // Updated_Vert is a RDD gives the output with each community and it's internal edges
      var vertCommunitySet = scala.collection.mutable.HashSet[Long]()
      val updatedVertInternalWt = ceilGraph.triplets.flatMap(et => {
        if (et.srcAttr.community == et.dstAttr.community) {
          var commIntWt = et.attr
          var commIntVert = 0L
          if (!vertCommunitySet.contains(et.srcId)) {
            vertCommunitySet.add(et.srcId)
            commIntWt += et.srcAttr.internalEdges
            commIntVert += et.srcAttr.internalVertices
          }
          if (!vertCommunitySet.contains(et.dstId)) {
            vertCommunitySet.add(et.dstId)
            commIntWt += et.dstAttr.internalEdges
            commIntVert += et.dstAttr.internalVertices
          }
          Iterator((et.srcAttr.community, (commIntVert, commIntWt, commIntWt)))

        } else {
          var commVertSrc = 0L
          var commVertDst = 0L
          var commIntWtSrc = et.attr
          var commIntWtDst = et.attr
          if (!(vertCommunitySet.contains(et.srcId))) {
            commVertSrc = et.srcAttr.internalVertices
            commIntWtSrc += et.srcAttr.internalEdges
            vertCommunitySet.add(et.srcId)
          }
          if (!(vertCommunitySet.contains(et.dstId))) {
            commVertDst = et.dstAttr.internalVertices
            commIntWtDst += et.dstAttr.internalEdges
            vertCommunitySet.add(et.dstId)
          }

          Iterator((et.srcAttr.community, (commVertSrc, commIntWtSrc, 0L)), (et.dstAttr.community, (commVertDst, commIntWtDst, 0L)))
        }
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)).cache()
      updatedVertInternalWt.count()
      //updatedVertInternalWt.foreach(println) //(cId, (comIntVert+commNodesVert, commIntWt+commExtWt+commVertIntWt, ComInternalWt+nodeInternalWt)) // (cId, (comIntVert, commTotalEdges/sigTot, commInternalEdges))

      val stopTime3: Double = System.currentTimeMillis.toDouble / 1000
      val T3 = stopTime3 - startTime3
      // println("Running time is 2 : " + runningTime)

      val startTime4: Double = System.currentTimeMillis.toDouble / 1000

      // map each vertex ID to its updated community information
      //(cId, (comIntVert, sigmaTot, ComInternalWt+nodeInternalWt))
      val updatedVerts = ceilGraph.vertices.
        map({ case (id, data) => (data.community, (id, data)) }).
        join(updatedVertInternalWt).
        map({
          case (community, ((id, data), newCommUpdateTuple)) =>
            if (newCommUpdateTuple._3 != 0L)
              data.communityInternalEdges = newCommUpdateTuple._3
            else
              data.communityInternalEdges = data.internalEdges
            data.communityVertices = newCommUpdateTuple._1
            data.communityTotalEdges = newCommUpdateTuple._2
            (id, data)
        }).cache()
      updatedVerts.count()
      //updatedVerts.foreach(println)
      //( cId, (vId,vdata), (comIntVert, sigmaTot, ComInternalWt+nodeInternalWt)) // 2nd map o/p :(VertexId, VertexState)
      updatedVertInternalWt.unpersist(blocking = false)

      val stopTime4: Double = System.currentTimeMillis.toDouble / 1000
      val T4 = stopTime4 - startTime4
      // println("Running time is 3 : " + rT3)

      val startTime5: Double = System.currentTimeMillis.toDouble / 1000
      prevGraph = ceilGraph
      ceilGraph = ceilGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
      ceilGraph.cache()
      //.partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _).cache()
      //ceilGraph.triplets.count()
      //ceilGraph.triplets.foreach(println)
      //ceilGraph.vertices.foreach(println)
      val oldMsgs = msgRDD
      msgRDD = ceilGraph.mapReduceTriplets(sendMsg, mergeMsg).cache()
      //msgRDD.foreach(println) // Messages form with respect to new Graph.
      msgRDD.count() // materializes the graph by forcing computation
      oldMsgs.unpersist(blocking = false)
      prevGraph.unpersistVertices(blocking = false)
      updatedVerts.unpersist(blocking = false)
      // half of the communites can swtich on even cycles  and the other half on odd cycles (to prevent deadlocks). So we only want to look for progess on odd cycles (after all vertices have had a chance to move)
      if (even) updated = 0
      updated = updated + ceilGraph.vertices.filter(_._2.changed).count()
      if (!even) {
        println("  # vertices moved: " + java.text.NumberFormat.getInstance().format(updated))
        if (updated >= updatedLastPhase - minProgress) stop += 1
        //if (updated < minProgress) stop += 1
        updatedLastPhase = updated
      }

      val stopTime5: Double = System.currentTimeMillis.toDouble / 1000
      val T5 = stopTime5 - startTime5
      // println("Running time is 4 : " + runningTime4)
    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))
    // println("\nCompleted in " + count + " cycles") //above condition is condition to terminate phase -1

    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val allCommunities = ceilGraph.vertices.map({ case (id, data) => (data.community, data) }).reduceByKey((a, b) => a)
    val communitiesScores = allCommunities.map({
      case (id, data) =>
        var communityScore = 0.0
        val ns = data.communityVertices //vdata.communityInternalEdges as both are same when vertex is community itself.
        if (ns <= 1) communityScore = 0.0
        else {
          val bs = data.communityTotalEdges
          val as = data.communityInternalEdges
          communityScore = (2 * as * as).toDouble / (((ns - 1) * (as + bs)) * totalGraphVertices)
        }
        CEILScore += communityScore
        CEILScore
    })

    val last = communitiesScores.count().toInt
    CEILScore = communitiesScores.take(last).last
    diffInCEILScores = CEILScore - prevCEILScore

    // In CEIL, this is the CEIL score, sum of all the community scores.
    var actualQ = 0.0
    if (diffInCEILScores < 0.0) {
      actualQ = 0.0
    } else {
      actualQ = diffInCEILScores
    }

    prevCEILScore = CEILScore
    // return the modularity value of the graph along with the graph. vertices are labeled with their community
    return (actualQ, ceilGraph, count / 2)

  }

  /** Creates the messages passed between vertices to convey neighborhood community data.*/
  private def sendMsg(et: EdgeTriplet[VertexState, Long]) = {

    val m1 = (et.dstId, Map((et.srcAttr.community,
      et.srcAttr.communityTotalEdges,
      et.srcAttr.communityInternalEdges,
      et.srcAttr.communityVertices) -> et.attr))

    val m2 = (et.srcId, Map((et.dstAttr.community,
      et.dstAttr.communityTotalEdges,
      et.dstAttr.communityInternalEdges,
      et.dstAttr.communityVertices) -> et.attr))

    Iterator(m1, m2)
  }

  /** Merges neighborhood community data into a single message for each vertex.*/
  private def mergeMsg(
    m1: Map[(Long, Long, Long, Long), Long],
    m2: Map[(Long, Long, Long, Long), Long]) = {

    val newMap = scala.collection.mutable.HashMap[(Long, Long, Long, Long), Long]()
    m1.foreach({
      case (k, v) =>
        if (newMap.contains(k)) newMap(k) = newMap(k) + v
        else newMap(k) = v
    })
    m2.foreach({
      case (k, v) =>
        if (newMap.contains(k)) newMap(k) = newMap(k) + v
        else newMap(k) = v
    })

    newMap.toMap
  }

  /** Calculates delta in community score on the addition of vertex to
    * neighboring community and removal from its own community.
    * Wherever the change is maximum, moves vertex to that community and
    * returns set of vertices with updated information of their community.
    */
  private def ceilVertJoin(
    ceilGraph: Graph[VertexState, Long],
    msgRDD: VertexRDD[Map[(Long, Long, Long, Long), Long]],
    totalGraphVertices: Long,
    even: Boolean) = {

    ceilGraph.vertices.innerJoin(msgRDD)((id, data, msgs) => {

      var bestCommunity = data.community
      var maxDeltaQ: Double = 0.0
      maxDeltaQ = removal(data, msgs, totalGraphVertices)

      msgs.foreach({
        case ((communityId, testCommunityTotalEdges, testCommunityInternalEdges, testCommunityVertices), incidentEdges) =>
          var deltaQ = 0.0
          if (data.community == communityId)
            deltaQ = 0.0
          else {
            val commScores = communityScores(testCommunityTotalEdges, incidentEdges, testCommunityInternalEdges, testCommunityVertices, data.outgoingEdges, data.internalEdges, data.internalVertices, totalGraphVertices)
            deltaQ = commScores._2 - commScores._1
          }

            
          if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))) {
            maxDeltaQ = deltaQ
            bestCommunity = communityId
          }
      })
      // only allow changes from low to high communties on even cyces and high to low on odd cycles
      //println("For Community : " + vdata.community + ", Best Community is -----------$$$-----> " + bestCommunity)
      if (data.community != bestCommunity && ((even && data.community > bestCommunity) || (!even && data.community < bestCommunity))) {
        //println("Community : " + vdata.community + ", is going to community-----------$$$-----> " + bestCommunity)
        data.community = bestCommunity
        data.changed = true
      } else {
        data.changed = false
      }
      data
    })
  }

  /** Calculates delta Q for two communities.*/
  private def removal(
    data: VertexState,
    msgs: Map[(Long, Long, Long, Long), Long],
    totalGraphVertices: Long) = {

    var incidentEd = 0L
    msgs.foreach({
      case ((communityId, testCommunityTotalEdges, testCommInternalEdges, testCommunityVertices), incidentEdges) =>
        if (data.community == communityId)
          incidentEd += incidentEdges
    })
    val afrRmTotalEdges = data.communityTotalEdges - data.outgoingEdges - data.internalEdges + incidentEd
    val afrRmTestCommInternalEdges = data.communityInternalEdges - data.internalEdges - incidentEd
    val afrRmTestCommunityVertices = data.communityVertices - data.internalVertices

    val commScores = communityScores(afrRmTotalEdges, incidentEd, afrRmTestCommInternalEdges, afrRmTestCommunityVertices, data.outgoingEdges, data.internalEdges, data.internalVertices, totalGraphVertices)
    val deltaQ = commScores._2 - commScores._1
    deltaQ
  }

  /** Returns the community score of current and target community on the basis of CEIL Scoring function
    *
    * @param testCommunityTotalEdgeWt
    * @param incidentEdges
    * @param testCommunityInternalEdges
    * @param testCommunityVertices
    * @param outgoingEdgesCand
    * @param internalEdges
    * @param candidateInternalVertices
    * @param totalGraphVertices
    * @return oldCommunityScore and newCommunityScore
    */
  private def communityScores(
    testCommunityTotalEdgeWt: Long,
    incidentEdges: Long,
    testCommunityInternalEdges: Long,
    testCommunityVertices: Long,
    outgoingEdgesCand: Long,
    internalEdges: Long,
    candidateInternalVertices: Long,
    totalGraphVertices: Long): (Double, Double) = {

    // Community Score requires: n => number of vertices in community,
    //                           a => internal edges weight,
    //                           b => external edges weight,
    //                           N => total vertices in graph
    
    //println("CurrentCommID : " + currCommunityId + ", TestCommunityID : " + testCommunityId)
    var oldCommunityScore = 0.0

    if (testCommunityVertices <= 1) {
      oldCommunityScore = 0.0
    } else {
      //testCommTotatEdgeWt - outgoingEdgesDest
      val aOld = testCommunityInternalEdges

      //testCommTotatEdgeWt - testCommunityInternalEdge
      val bOld = testCommunityTotalEdgeWt - testCommunityInternalEdges

      oldCommunityScore = (2 * aOld * aOld).toDouble / ((((testCommunityVertices - 1) * (aOld + bOld)) * totalGraphVertices)).toDouble
    }

    var newCommunityScore = 0.0
    if ((testCommunityVertices + candidateInternalVertices) <= 1) {
      newCommunityScore = 0.0
    } else {
      //testCommTotatEdgeWt + incident_Edges + internalEdges - outgoingEdgesDest
      val aNew = testCommunityInternalEdges + incidentEdges + internalEdges
      //testCommTotatEdgeWt - testCommunityInternalEdge + outgoingEdgesCand - 2 * incident_Edges
      val bNew = testCommunityTotalEdgeWt - testCommunityInternalEdges + outgoingEdgesCand - 2 * incidentEdges
      val nNew = testCommunityVertices + candidateInternalVertices
      newCommunityScore = (2 * aNew * aNew).toDouble / (((nNew - 1) * (aNew + bNew)) * totalGraphVertices).toDouble
    }

    return (oldCommunityScore, newCommunityScore)
  }

  /** Compresses a graph by its communities and aggregates both internal node weights
    * and edge weights within communities.
    * 
    * @param graph the uncompressed graph
    * @param debug the flag to debug
    * @return the compressed graph
    */
  def compressGraph(graph: Graph[VertexState, Long], debug: Boolean = true): Graph[VertexState, Long] = {

    // Makes each community a vertex and renames them as  newVerts.
    // Updates vertex information from community information.
    val newVerts = graph.vertices.
      map({ case (id, data) => (data.community, data) }).
      reduceByKey((a, b) => a).
      map({ case (id, data) =>
        val state = new VertexState()
        state.community = data.community
        state.changed = false
        state.communityTotalEdges = data.communityTotalEdges
        state.internalEdges = data.communityInternalEdges
        state.outgoingEdges = data.communityTotalEdges - data.communityInternalEdges
        state.internalVertices = data.communityVertices
        state.communityVertices = data.communityVertices
        state.communityInternalEdges = data.communityInternalEdges
        (data.community, state)
      }).cache()
    newVerts.count()

    // Translates each vertex edge to a community edge.
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()

    // Generates a new graph where each community of the previous graph
    // now represents a single vertex.
    val ceilGraph = Graph(newVerts, edges).
      partitionBy(PartitionStrategy.EdgePartition2D).
      groupEdges(_ + _).cache()

    ceilGraph.triplets.count()

    newVerts.unpersist(blocking = false)
    edges.unpersist(blocking = false)

    return ceilGraph
  }
}
