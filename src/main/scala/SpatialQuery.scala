package cse511

import org.apache.spark.sql.SparkSession
import scala.math.sqrt

object SpatialQuery extends App{
	def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
  val x = pointString.split(",")(0).toDouble
  val y = pointString.split(",")(1).toDouble
  val coords = queryRectangle.split(",").map(_.toDouble)
  
  // Normalize coordinates (xmin, ymin, xmax, ymax)
  val xmin = Math.min(coords(0), coords(2))
  val xmax = Math.max(coords(0), coords(2))
  val ymin = Math.min(coords(1), coords(3))
  val ymax = Math.max(coords(1), coords(3))

  if (xmin <= x && x <= xmax && ymin <= y && y <= ymax) {
    true
  } else {
    false
  }
}

  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val pt1 = pointString1.split(",").map(_.toDouble)
    val pt2 = pointString2.split(",").map(_.toDouble)

    val x1 = pt1(0)
    val y1 = pt1(1)
    val x2 = pt2(0)
    val y2 = pt2(1)

    val dist = Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2))

    if(dist <= distance) true else false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

	val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
	resultDf.show()

	return resultDf.count()
  }



  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
	rectangleDf.createOrReplaceTempView("rectangle")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

	val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
	resultDf.show()

	return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))
	val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
	resultDf.show()

	return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point1")

	val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
	pointDf2.createOrReplaceTempView("point2")

	// YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
	spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))
	val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
	resultDf.show()

	return resultDf.count()
  }
}
