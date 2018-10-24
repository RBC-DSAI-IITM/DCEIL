package in.ac.iitm.rbcdsai.dceil

/** VertexState contains information needed for CEIL community detection.*/
class VertexState extends Serializable {

  var community = -1L
  var outgoingEdges = 0L
  var changed = false

  // Total vertices within a vertex (in hypernode form);
  // at zeroth level the vertex is included as well.
  var internalVertices = 1L
  var internalEdges = 0L
  var communityVertices = 1L
  var communityTotalEdges = 0L
  var communityInternalEdges = 0L

  /** toString overrides the default toString to provide specific details.*/
  override def toString(): String = {

    "{ community: " + community +
    ", communitySigmaTotal: " + communityTotalEdges +
    ", internalWeight: " + internalEdges +
    ", nodeWeight: " + outgoingEdges +
    ", internalVertices: " + internalVertices +
    ", communityVertices: " + communityVertices +
    ", communityInternalWeight: " + communityInternalEdges + " }"
  }
}
