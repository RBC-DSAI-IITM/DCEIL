package in.ac.iitm.rbcdsai.dceil

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.Map

/** HDFSCeilRunner is a wrapper for CEIL algorithm executor.
  * 
  * At each level, it saves the edges and vertices in HDFS. Also possible to
  * save locally if in local mode.
  *
  * @constructor create a new runner with minProgress, progressCounter
  * and outputdir
  * @param minProgress the minimum progress
  * @param progressCounter the progress counter
  * @param outputDir the output directory
  * 
  * @see CeilHarness for high-level interface
  */
class HDFSCeilRunner(
    minProgress: Int,
    progressCounter: Int,
    outputDir: String)
  extends CeilHarness(minProgress: Int, progressCounter: Int) {

  var qValues = Array[(Int, Double)]()
  var prevLevelCommunityVert = Map[Long, List[Long]]()

  /** saveLevel overrides the CeilHarness.saveLevel method to provide functionality
    * to save the output of communities in the form of
    * `community: list of vertices in the community`.
    */
  override def saveLevel(
    sc: SparkContext,
    level: Int,
    q: Double,
    graph: Graph[VertexState, Long]) = {
 
    graph.vertices.saveAsTextFile(outputDir + "/level_" + level + "_vertices")
    //graph.edges.saveAsTextFile(outputdir + "/level_" + level + "_edges")
    //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices_Mapped")
    //graph.edges.mapValues({ case e => "" + e.srcId + "," + e.dstId + "," + e.attr }).saveAsTextFile(outputdir+"/level_"+level+"_edges")  
    qValues = qValues :+ ((level, q))
    // println(s"qValue: $q")

    // Overwrites the q values at each level
    sc.parallelize(qValues, 1) //.saveAsTextFile(outputdir+"/qvalues")
    
    val communityVertMap = scala.collection.mutable.Map[Long, List[Long]]()
    val prevCommunityVert = prevLevelCommunityVert      // previous level's community:vertices map
    
    val comm: RDD[scala.collection.mutable.Map[Long,List[Long]]] = graph.vertices.map({
      case (id, data) =>
        val vertexId: Long = id
        val vertexComm = data.community
        //println("Community is : "+vertex_Comm +", Vertex of this community is : "+vid)
        if (!communityVertMap.contains(vertexComm)) {
          val newListOfVert = List(vertexId)
          //println(newList_Of_Vert)
          if (!prevCommunityVert.isEmpty) {
            var updatedList: List[Long] = List()
            newListOfVert.foreach { id =>
              if (prevCommunityVert.contains(id)) {
                val newList = prevCommunityVert.getOrElse(id, List(id))
                newList.foreach { v =>
                  if (!updatedList.contains(v)) {
                    updatedList :::= List(v)
                    //println("New Updated list : "+updatedList)
                  } 
                }
              }
            }
            //println("New Updated list of Vertices is : "+updatedList)
//            if (!updatedList.contains(vertex_Comm)) {
//              updatedList :::= List(vertex_Comm)
//            }
            communityVertMap.put(vertexComm, updatedList)
          } else {
            communityVertMap.put(vertexComm, newListOfVert)
          }
          
        } else {
          val existingListOfVert = communityVertMap.getOrElse(vertexComm, List[Long]())
          val newListOfVert = existingListOfVert ::: List(vertexId)
          //println(newList_Of_Vert)
          if (!prevCommunityVert.isEmpty) {
            var updatedList: List[Long] = List()
            newListOfVert.foreach { id =>
              if (prevCommunityVert.contains(id)) {
                val newList = prevCommunityVert.getOrElse(id, List(id))
                newList.foreach { v =>
                  if (!updatedList.contains(v)) {
                    updatedList :::= List(v)
                    //println("New Updated list : "+updatedList)
                  } 
                }
              }
            }
//            println("New Updated list of Vertices is : "+updatedList)
//            if (!updatedList.contains(vertex_Comm)) {
//              updatedList :::= List(vertex_Comm)
//            }
            communityVertMap.put(vertexComm, updatedList)
          } else {
            communityVertMap.put(vertexComm, newListOfVert)
          }
        }
        communityVertMap
    })
    comm.count()
    prevLevelCommunityVert = comm.collect().last
    //println(prevLevelCommunityVert)
    sc.parallelize(prevLevelCommunityVert.toSeq).
      saveAsTextFile(outputDir + "/level_" + level + "_vertices_Mapped1")
    comm.unpersist(blocking = false)
  }
}
