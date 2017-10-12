package com.shan.spark

import java.util
import java.util.{Calendar, Date}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Shan {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("mapExample").master("local").getOrCreate()
    val data = spark.read.textFile("file:///home/jpa/Desktop/output1/WA_Fn-UseC_-HR-Employee-Attrition.csv").rdd

    /*    def onlyStrings(a: Any) = a match {
          case 1 => "Married"
          case 2 => "Divorced"
          case 3 => "unmarried"

          case _ => null
        }*/ val mapFile = data.map(line => (line, line.length)) // map transformation by length
    mapFile.foreach(println)
    //Action
    val mapf = data.map(x => x.split(" ").toString) //map Transformation by Split
    mapf.foreach(println)
    //Action
    var s = mapf.count()
    //Action
    var fmap = data.flatMap(y => y.split(",")) // FlatMap Transformation By Split
    // var fil=fmap.filter(z=>z.contains("Married"))
    fmap.foreach(println)
    var sh = fmap.filter(z => z.contains("Divorced"))
    println(s)
    var sp = sh.collect() //Action
    var sw = sh.count() // Action
    println(sw)
    val now = Calendar.getInstance().getTimeInMillis
    //println(sw.toString())
    sh.saveAsTextFile("file:///home/jpa/Desktop/hadoop/shan/" + now) // Action
    sh.saveAsObjectFile("file:///home/jpa/Desktop/hadoop/shan/" + now + "B")// Action
    // sh.saveAsSequenceFile("file:///home/jpa/Desktop/hadoop/shan/" + now)
    //sp.saveAsTextFile("file:///home/jpa/Desktop/hadoop/shan/" + now) // we cannot apply an action RDD to Another action RDD
  }
}
