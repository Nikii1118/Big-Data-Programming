import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

object GraphTask {
  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    val df = spark.read.option("header", "true").csv("/home/strwbrygapple/Desktop/lab2/Input/wordgame_20170721.csv").limit(1000)
    df.show()

    val vertices= df.select("word1").toDF("id").distinct()
    val moreVerticies = df.select("word2").toDF("id").distinct()
    val allVerticies = vertices.union(moreVerticies).dropDuplicates()

    val edges= df.select("word1","word2").toDF("src","dst").distinct()
    val moreEdges = df.select("word2", "word1").toDF("src", "dst").distinct()
    val allEdges = edges.union(moreEdges).dropDuplicates()

    val g=GraphFrame(allVerticies,allEdges)
    g.vertices.show()
    g.edges.show()

    // Run PageRank
    val results3 = g.pageRank.resetProbability(0.15).maxIter(2).run()
    results3.vertices.select("id", "pagerank").sort(desc("pagerank")).show
  }
}