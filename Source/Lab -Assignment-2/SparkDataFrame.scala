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


object DataFrames1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Lab2").setMaster("local[*]")
    // Create Spark Context.

    val sc = new SparkContext(conf)
    val sqlContext= new SQLContext(sc)   // Create SQL context
    //val sparkSession = new SparkContext()(sc)
    val spark = SparkSession
      .builder()
      .appName("Dataframes1")
      .config("spark.master", "local")
      .getOrCreate()

    //Defining structure type for every column in the data before importing the csv data
    val schema =
      StructType(
        List(
          StructField("Year", IntegerType,true),
          StructField("Datetime", StringType, true),
          StructField("Stage", StringType, true),
          StructField("Stadium", StringType, true),
          StructField("City", StringType, true),
          StructField("Home Team Name", StringType, true),
          StructField("Home Team Goals", StringType, true),
          StructField("Away Team Goals", StringType, true),
          StructField("Away Team Name", StringType, true),
          StructField("Win Conditions", StringType, true),
          StructField("Attendance", IntegerType, true),
          StructField("Half-time Home Goals", StringType, true),
          StructField("Half-time Away Goals", StringType, true),
          StructField("Referee", StringType, true),
          StructField("Assistant 1", StringType, true),
          StructField("Assistant 2", StringType, true),
          StructField("RoundID", StringType, true),
          StructField("MatchID", StringType, true),
          StructField("Home Team Initials", StringType, true),
          StructField("Away Team Initials", StringType, true)
        )

      )

    val df = sqlContext.read.option("header", "True").csv("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Source Code\\WorldCupMatches.csv")

    df.write.option("header", "True").csv("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Output")

    // 2 Changing the structType of columns from String to Double





    val df1=sqlContext.read.option("header", "True").csv("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Source Code\\WorldCupPlayers.csv")

    df1.write.option("header", "True").csv("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Output1")

    val df2= sqlContext.read.option("header", "True").csv("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Source Code\\WorldCups.csv")

    df2.write.option("header", "True").csv("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Output3")

    print("Printing schema of stored data")
    df.printSchema()
    df1.printSchema()
    df2.printSchema()


    //Correct Query 1: If the average half time goals of home team are greater than away team then return true,
    // In this scenario we are checking the performance of teams with half time parameter
    val n= df.agg(avg("Half-time Home Goals")>avg("Half-time Away Goals")).show()

    //Correct Query 2: show only those matches that won with a condition
    df.select("Win Conditions").distinct().show()

    //Creating a table to put all the three dataframes into it and printing the tables
    val testtable=df.select("*")

    val testtable1=df1.select("*")
    val testtable2=df2.select("*")


    //Correct Query 3: when home country won the matches show the goals scored
    val testTable2 = df2.select("Year","GoalsScored", "Country","Winner").where("Country==Winner")
    testTable2.show()

    //Correct query 4 : which countries have won how many times and sort their wins by desc
    val testTable3 = df2.select("Winner").groupBy("Winner").count().sort(desc("count"))
    testTable3.show()

    testtable.createOrReplaceTempView("testTable")
    testtable1.createOrReplaceTempView("testTable1")
    testtable2.createOrReplaceTempView("testTable2")


    //Correct Query 5: join operation showcasing joined schema on df and df2 on matchid,
    // joinWith creates a Dataset with two columns _1 and _2 that each contain records for which condition holds.
    val joined = testtable.joinWith(testtable1, testtable("MatchID") === testtable1("MatchID"))
    joined.printSchema()
    joined.show()


    //Correct query 6 : Display the Team Initials and Coach name for distinct Events
    spark.sql("Select `Team Initials`,`Coach Name`, Event From testTable1 where Event != 'NULL'").show()


    //Correct Query 7: Total Attendance of World Cups by Year
    val matches=df.dropDuplicates("MatchID")
    matches.col("Year").isNotNull
    matches.groupBy("Year").agg(sum("Attendance")).show()


    //Correct Query 8: Cities that hosted highest world cup matches
    matches.groupBy("City").count().sort(desc("count")).show()


    // Correct Query 9: Display all the finale matches
    val finale=df.select("*").filter(df("Stage") ==="Final")
    finale.show()


    //Correct Query 10: Average matches played by Germany
    val aggQuery1 = df2.select("*").filter(df2("Country")==="Germany").agg(avg("MatchesPlayed"))
    aggQuery1.show()




    //Question 2.3: Spark RDD vs Data Frame



    //5 queries in Spark RDD

    System.setProperty("hadoop.home.dir","C:\\winutils")

    //create an RDD. Here .rdd method is used to convert Dataset<Row> to RDD<Row>

    val textRDD = spark.read.csv("C:\\Users\\vasim\\Desktop\\Cloudera\\Documentation\\Lab 2\\Source Code\\WorldCups.csv").rdd
    println(textRDD.foreach(println))
    //get no of columns
    val lineLengths = textRDD.map(s => s.length)
    println(lineLengths)
    val totalLength = lineLengths.reduce((a, b) => a + b)



    //Changing Struct Type of rdd to dataframe

    val schemardd = StructType(
      List(
        StructField("Year", IntegerType, true),
    StructField("Country", StringType,true),
    StructField("Winner", StringType, true),
    StructField("Runners-Up", StringType,true),
    StructField("Third", StringType,true),
    StructField("Fourth", StringType,true),
    StructField("GoalsScored", IntegerType,true),
    StructField("QualifiedTeams", IntegerType,true),
    StructField("MatchesPlayed", IntegerType,true),
    StructField("Attendance", IntegerType,true)))

   val dfnew=spark.createDataFrame(textRDD,schemardd)
    dfnew.show()

    //dataframe Query 1: Return countries with top 10 goals scored
    val k=dfnew.select("Country","GoalsScored").orderBy(desc("GoalsScored")).take(10)
    println(k)

    //dataframe Query 2:  finding mean, max, min etc of the attendance over the entire matches
    dfnew.describe("Attendance").show()

    //dataframe Query 3:Find the world cup matches details for year-wise
    dfnew.select("Year","Winners","Runner-up").filter(col("Year")).show()

    //dataframe Query 4: Find statistics for year 2010
    dfnew.select("Year").filter(col("Year")==="2010").show()

    //dataframe Query 5: Find the years details in which maximum no. of matches were played
    dfnew.withColumn("MatchesPlayed", dfnew("MatchesPlayed")).asInstanceOf[IntegerType]
    dfnew.filter(col("MatchesPlayed")=== 64).show()







































  }
}