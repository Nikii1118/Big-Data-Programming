import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object twitterWordCount {
  val outputDir = "/home/strwbrygapple/Desktop/lab2/output"
  val oauthConfigPath = "/home/strwbrygapple/Desktop/lab2/src/main/oauth.txt"

  def main(args: Array[String]): Unit = {
    // Set up environment
    val sparkConf = new SparkConf().
      setAppName("bigDataSummer2019").
      setMaster("local[1]")
    val sparkCont = new SparkContext(sparkConf)
    val sc = new StreamingContext(sparkCont, Seconds(20))
    // Load oauth config file
    val oauthConfig = sparkCont.textFile(oauthConfigPath).flatMap(_.split(",")).collect()
    // Oauth with twitter
    val configurationBuilder = new ConfigurationBuilder
    configurationBuilder.setDebugEnabled(true).setOAuthConsumerKey(oauthConfig(0))
      .setOAuthConsumerSecret(oauthConfig(1))
      .setOAuthAccessToken(oauthConfig(2))
      .setOAuthAccessTokenSecret(oauthConfig(3))
    val auth = new OAuthAuthorization(configurationBuilder.build())
    // Create input stream
    val input = TwitterUtils.createStream(sc, Some(auth))
    // Extract text from all tweets in English
    val inputTexts = input.filter(_.getLang() == "en").map(x => x.getText)
    // Count words with MapReduce paradigm
    val wordsCounts = inputTexts.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    // Perform secondary sort on wordCounts
    val sortedCounts = wordsCounts.map(x => x.swap).transform(_.sortByKey(false)).map( x => x.swap)
    // Save output
    sortedCounts.saveAsTextFiles(outputDir)

    // Trigger job and track execution time
    val startTime = System.currentTimeMillis()
    sc.start()
    sc.awaitTermination()
    val endTime = System.currentTimeMillis()
    val runTime = (endTime - startTime) / 1000
    println("Approximate sample time: " + runTime + " seconds.")
  }
}
