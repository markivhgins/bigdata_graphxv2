package com.socialanalyzer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import java.io.{File, PrintWriter}
import scala.util.Try

/** Runs both required algorithms with Apache Spark GraphX (not a Python graph library). */
object SocialNetworkAnalyzer {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: SocialNetworkAnalyzer <input-edge-list> <output-directory>")
      System.exit(2)
    }
    val inputPath = new File(args(0)).getCanonicalPath
    val outputDir = new File(args(1)).getCanonicalFile
    require(new File(inputPath).isFile, s"Input file does not exist: $inputPath")
    if (!outputDir.exists() && !outputDir.mkdirs()) throw new IllegalStateException(s"Cannot create output directory: $outputDir")

    val conf = new SparkConf().setAppName("Social Network Analyzer - GraphX")
    // spark-submit supplies the local master from the launcher; default only helps direct IDE runs.
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    try {
      val parsed: RDD[(String, String)] = sc.textFile(inputPath).flatMap { raw =>
        val line = raw.trim
        if (line.isEmpty || line.startsWith("#") || line.toLowerCase.startsWith("source")) None
        else {
          val parts = line.split("[,\\s]+", -1).map(_.trim).filter(_.nonEmpty)
          if (parts.length < 2) None else Some((parts(0), parts(1)))
        }
      }.filter { case (a, b) => a != b }
        .distinct()
      val edgePairs = parsed.map { case (a, b) => if (a <= b) (a, b) else (b, a) }.distinct().cache()
      val edgeCount = edgePairs.count()
      require(edgeCount > 0, "No valid relationships found. Provide at least one edge with two user IDs.")

      // GraphX VertexId is Long. Stable IDs are assigned from sorted distinct external user IDs.
      val userIds = edgePairs.flatMap { case (a, b) => Iterator(a, b) }.distinct().sortBy(identity).zipWithIndex()
        .map { case (externalId, index) => (externalId, index + 1L) }.cache()
      val externalByVertex = userIds.map { case (external, internal) => (internal, external) }.collect().toMap
      val vertexCount = userIds.count()
      val vertexMap = userIds.map { case (external, internal) => (internal: VertexId, external) }
      val internalEdges = edgePairs.map { case (a, b) => (a, b) }.join(userIds).map { case (a, (b, srcId)) => (b, srcId) }
        .join(userIds).map { case (_, (srcId, dstId)) => Edge(srcId: VertexId, dstId: VertexId, 1) }
      val vertices = vertexMap.map { case (id, _) => (id, 1) }
      val graph: Graph[Int, Int] = Graph(vertices, internalEdges, 1).cache()

      // Actual GraphX PageRank implementation. Iterative fixed iteration count for reproducibility.
      val pageRankGraph = graph.pageRank(0.0001).cache()
      val degreeById = graph.degrees.collect().toMap
      val rankRows = pageRankGraph.vertices.map { case (id, rank) => (id, rank) }.collect()
        .sortBy { case (_, rank) => -rank }
      val ccRows = graph.connectedComponents().vertices.collect().toMap

      val rankFile = new PrintWriter(new File(outputDir, "pagerank_results.csv"), "UTF-8")
      try {
        rankFile.println("user_id,pagerank,degree")
        rankRows.foreach { case (id, rank) =>
          val external = csv(externalByVertex(id))
          rankFile.println(s"$external,$rank,${degreeById.getOrElse(id, 0)}")
        }
      } finally rankFile.close()

      val communityFile = new PrintWriter(new File(outputDir, "communities_results.csv"), "UTF-8")
      try {
        communityFile.println("user_id,component_id")
        ccRows.toSeq.sortBy(_._1).foreach { case (id, component) =>
          communityFile.println(s"${csv(externalByVertex(id))},${csv(externalByVertex(component))}")
        }
      } finally communityFile.close()

      val componentSizes = ccRows.toSeq.groupBy(_._2).map { case (component, members) =>
        (externalByVertex(component), members.size)
      }.toSeq.sortBy { case (_, size) => -size }
      val stats = new PrintWriter(new File(outputDir, "graph_stats.json"), "UTF-8")
      try {
        val sizesJson = componentSizes.map { case (id, size) => s"""{"component_id":${json(id)},"size":$size}""" }.mkString(",")
        stats.println(s"""{"vertices":$vertexCount,"edges":$edgeCount,"components":${componentSizes.size},"engine":"Apache Spark GraphX 3.5.3","component_sizes":[$sizesJson]}""")
      } finally stats.close()
      println(s"GRAPHX_SUCCESS vertices=$vertexCount edges=$edgeCount components=${componentSizes.size}")
      println(s"Results written to ${outputDir.getAbsolutePath}")
      pageRankGraph.unpersist(blocking = false); graph.unpersist(blocking = false); edgePairs.unpersist(blocking = false)
    } finally sc.stop()
  }

  private def csv(value: String): String = "\"" + value.replace("\"", "\"\"") + "\""
  private def json(value: String): String = "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
