import jdk.nashorn.internal.objects.annotations.Where
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.rdd.RDD
import org.apache.spark.sql



object rdd {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Lab2").setMaster("local[*]")
    // Create Spark Context.

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc) // Create SQL context
    //val sparkSession = new SparkContext()(sc)
    val spark = SparkSession
      .builder()
      .appName("Dataframes1")
      .config("spark.master", "local")
      .getOrCreate()


    //Question 2.3: Spark RDD vs Data Frame

    //5 queries in Spark RDD

    System.setProperty("hadoop.home.dir","C:\\winutils")

    //Changing Struct Type of rdd to dataframe

    val schemardd = StructType(
      List(
        StructField("Year", StringType, true),
        StructField("Country", StringType,true),
        StructField("Winner", StringType, true),
        StructField("Runners-Up", StringType,true),
        StructField("Third", StringType,true),
        StructField("Fourth", StringType,true),
        StructField("GoalsScored", StringType,true),
        StructField("QualifiedTeams", StringType,true),
        StructField("MatchesPlayed", StringType,true),
        StructField("Attendance", StringType,true)))

    //create an RDD. Here .rdd method is used to convert Dataset<Row> to RDD<Row>

    //val textRDD = spark.read.csv("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Source Code\\WorldCups.csv").rdd
    //println(textRDD.foreach(println))
    //get no of columns
    //val lines = textRDD.map(s => s.length)
    //val pairs2= textRDD.map(s => (s, 1))
    //println(pairs2)
    //val counts1 = pairs2.reduceByKey((a, b) => a + b)
    //println(lines)
    //val totalLength = lines.reduce((a, b) => a + b)


    //dataframe Query 1: Return countries with top 10 goals scored
    //val k=dfnew.select("Country","GoalsScored").orderBy(desc("GoalsScored")).show()
    //val pairs = pairs2.map(x => (x.split(" ")(1), x))

    //pairs.filter{case (key, value) => value.length < 20}

    //dataframe Query 2:  finding mean, max, min etc of the attendance over the entire matches
    //dfnew.describe("Attendance").show()

    //dataframe Query 3: Show distinct ordered records
    //val query=dfnew.select("*").distinct().sort(asc("Year")).show()

    //dataframe Query 4: Find statistics for the World Cup 2010
    //dfnew.select("*").where(col("Year").contains("2010")).show()

    //dataframe Query 5: Find the years details in which maximum no. of matches were played
    //dfnew.filter(col("MatchesPlayed")=== 64).show()

    //Perform any 5 queries in Spark RDD’s and Spark Data Frames.

    // To Solve this Prolblem we first create the rdd as we already have Dataframe wc_df created above code

    // RDD creation

    val csv = sc.textFile("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Source Code\\WorldCups.csv")

    val header = csv.first()

    val data = csv.filter(line => line != header)

    val rdd = data.map(line=>line.split(",")).collect()


    val dfnew = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Source Code\\WorldCups.csv")


    dfnew.createOrReplaceTempView("WorldCup")


    //Query 1: RDD Highest Numbers of goals

    val rddgoals = data.filter(line => line.split(",")(6) != "NULL").map(line => (line.split(",")(1),
      (line.split(",")(6)))).takeOrdered(10)
    rddgoals.foreach(println)

    // Dataframe

    dfnew.select("Country","GoalsScored").orderBy("GoalsScored").show(10)

    // Dataframe SQL

    val dfGoals = spark.sql("select Country,GoalsScored FROM WorldCup order by GoalsScored Desc Limit 10").show()

    // Query 2: Year, Venue country = winning country

    // Using RDD

    val rddvenue = data.filter(line => line.split(",")(1)==line.split(",")(2))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(2)))
      .collect()

    rddvenue.foreach(println)

    // Using Dataframe

    dfnew.select("Year","Country","Winner").filter("Country==Winner").show(10)

    // usig Spark SQL

    val venueDF = spark.sql("select Year,Country,Winner from WorldCup where Country = Winner order by Year").show()

    // Query 3: Details of years ending in ZERO

    // RDD
    var years = Array("1930","1950","1970","1990","2010")


    val rddwinY = data.filter(line => (line.split(",")(0) == "1930" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddwinY.foreach(println)

    //DataFrame
    dfnew.select("Year","Winner","Runners-Up").filter("Year='1930' or Year='1950' or " +
      "Year='1970' or Year='1990' or Year='2010'").show(10)

    //DF - SQL

    val winYDF = spark.sql("SELECT * FROM WorldCup WHERE " +
      " Year IN ('1930','1950','1970','1990','2010') ").show()

    //Query 4: 2014 world cup stats
    //Rdd

    val rddStat = data.filter(line=>line.split(",")(0)=="2014")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddStat.foreach(println)

    //using Dataframe
    dfnew.filter("Year=2014").show()

    //using DF - Sql
    spark.sql(" Select * from WorldCup where Year == 2014 ").show()


    //Query 5: Max matches played

    //RDD

    val rddMax = data.filter(line=>line.split(",")(8) == "64")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddMax.foreach(println)

    // DataFrame
    dfnew.filter("MatchesPlayed == 64").show()

    // Spark SQL

    spark.sql(" Select * from WorldCup where MatchesPlayed in " +
      "(Select Max(MatchesPlayed) from WorldCup )" ).show()




  }
}