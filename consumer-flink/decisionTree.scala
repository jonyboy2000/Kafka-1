package org.myorg.quickstart

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink
import org.apache.flink.streaming.util.serialization._

object StreamWC {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.addSource(new KafkaSource[String]("ovhbhshad3:2181", "vulab123", new SimpleStringSchema))
    val datasets: DataStream[String] = stream.flatMap { _.split("\n") } filter { _.nonEmpty }
    val nonEmptyDatasets: DataStream[Array[String]] = datasets.map { s => s.split("\t") }.filter { !_.contains("") }

    val inputProRe: DataStream[LabeledVector] = inputPro(nonEmptyDatasets)
    inputProRe.map { s => (s.position, s.label, s.feature) }.print

    // execute program
    env.execute()
  }

  // *************************************************************************
  //  UTIL Variable
  // *************************************************************************  
  private var inputPath: String = null
  private var outputPath: String = null
  private val numBins = 20 // B bins for Update procedure
  private val numSplit = 20 //By default it should be same as numBins
  private val numLevel = 1 // how many levels of tree

  case class LabeledVector(position: String, label: Double, feature: Array[Double])
  case class Histo(featureValue: Double, frequency: Double)
  case class Histogram(position: String, label: Double, featureIndex: Int, histo: Array[Histo])
  case class MergedHisto(position: String, featureIndex: Int, histo: Array[Histo])
  case class NumSample(position: String, number: Int)
  case class Uniform(position: String, featureIndex: Int, uniform: Array[Double])
  case class Sum(position: String, label: Double, featureIndex: Int, sum: Array[Double])
  case class Gain(position: String, featureIndex: Int, gain: Array[Double])
  case class Frequency(position: String, label: Double, frequency: Double)

  // *************************************************************************
  //  UTIL METHODS
  // *************************************************************************

  /*
   * input data process
   */
  def inputPro(nonEmptyDatasets: DataStream[Array[String]]): DataStream[LabeledVector] = {

    val nonEmptySample: DataStream[Array[Double]] = nonEmptyDatasets.map { s => s.take(14)
    }.map { s =>
      var re = new Array[Double](14)
      var i = 0

      for (aa <- s) {
        if (i <= 13)
          re(i) = aa.toDouble
        i += 1
      }
      re
    }

    val labledSample: DataStream[LabeledVector] = nonEmptySample.map { s =>
      new LabeledVector("", s(0), s.drop(1).take(13))
    }

    labledSample
  }

}
