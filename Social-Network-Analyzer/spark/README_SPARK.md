# Spark GraphX backend

This is the authoritative analysis engine. `SocialNetworkAnalyzer.scala` constructs `org.apache.spark.graphx.Graph` and invokes `graph.pageRank(0.0001)` and `graph.connectedComponents()`. There is no NetworkX/Python substitute.

## Build
From the `spark` folder, with JDK 17 and sbt 1.10.7 installed:

```powershell
sbt clean package
```

The jar is created at `spark/target/scala-2.12/social-network-graphx_2.12-1.0.0.jar`. Spark's own runtime supplies the `provided` Spark libraries.

## Run
```powershell
spark-submit --class com.socialanalyzer.SocialNetworkAnalyzer --master local[*] --conf spark.driver.memory=2g target/scala-2.12/social-network-graphx_2.12-1.0.0.jar ..\data\sample_edges.txt ..\output
```

Input is a whitespace-, comma-, or tab-delimited edge list. Blank lines, comments beginning with `#`, a `source` header, and self-links are ignored. Relationships are treated as undirected and duplicate pairs are removed. External user IDs are mapped to GraphX Long vertex IDs and mapped back on export.

Outputs: `pagerank_results.csv`, `communities_results.csv`, and `graph_stats.json`. A successful run prints `GRAPHX_SUCCESS`.

## Algorithm details
- PageRank: Spark GraphX iterative PageRank with tolerance `0.0001` (GraphX default reset probability).
- Connected Components: Spark GraphX `connectedComponents()`. Component ID is the minimum internal vertex ID in the connected component, mapped back to its external user ID.
- The graph is local (`local[*]`) for a single Windows laptop; this is still Spark GraphX execution, not a distributed cluster.
