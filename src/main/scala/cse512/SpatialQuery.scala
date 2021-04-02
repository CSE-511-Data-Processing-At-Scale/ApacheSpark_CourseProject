package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(queryRectangle:String, pointString:String): Boolean = {

    // Split the points string to get X-axis and Y-axis values.
	  var pointStringSplit = pointString.split(",")
    var queryRectangleSplit = queryRectangle.split(",")

    // Convert the string values to double for the point.
    var point_x = pointStringSplit(0).toDouble
    var point_y = pointStringSplit(1).toDouble

    // Convert the string values to double for the rectangle.
    var rect_x1 = queryRectangleSplit(0).toDouble
    var rect_y1 = queryRectangleSplit(1).toDouble
    var rect_x2 = queryRectangleSplit(2).toDouble
    var rect_y2 = queryRectangleSplit(3).toDouble

    // Assuming that the rectangle is parallel to X and Y axis.
    // The given points of the rectangle is assumed as the endpoints
    // of a diagonal. Therefore the points with the lowest value will
    // be the lowest point and the points with the highest value will
    // be the highest point.
    if(rect_x1 > rect_x2) {
      var temp = rect_x1
      rect_x1 = rect_x2
      rect_x2 = temp
    }

    if(rect_y1 > rect_y2) {
      var temp = rect_y1
      rect_y1 = rect_y2
      rect_y2 = temp
    }

    // Check if the point is inside the rectangle. It also includes points on
    // the boundary. If it is there, then return true.
    if(point_x >= rect_x1 && point_y >= rect_y1 && point_x <= rect_x2 && point_y <= rect_y2) {
      return true
    }

    // If the point is outside the boundary, then return false.
    return false
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
    
    // Split the points string to get X-axis and Y-axis values.
    var point1Split = pointString1.split(",")
    var point2Split = pointString2.split(",")

    // Convert the strings to double values
    var x1 = point1Split(0).toDouble
    var y1 = point1Split(1).toDouble
    var x2 = point2Split(0).toDouble
    var y2 = point2Split(1).toDouble

    // Calculate the distance between the two points using euclidean distance
    var calculated_distance = scala.math.sqrt(scala.math.pow((x2-x1),2)+scala.math.pow((y2-y1),2))

    // Compare the calculated distance with the given distance
    // if it is within or equal to that distance, return true
    if(calculated_distance <= distance)
      return true
    
    // else return false
    return false

  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle:String, pointString:String))))

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
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle:String, pointString:String))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1:String, pointString2:String, distance:Double))))

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
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1:String, pointString2:String, distance:Double))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
