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

    val histoSample: DataStream[Histogram] = inputProRe.flatMap { s =>
      (0 until s.feature.size) map {
        index => new Histogram(s.position, s.label, index, Array(Histo(s.feature(index), 1)))
      }
    }
    val updatedSample: DataStream[Histogram] = histoSample.groupBy("position", "label", "featureIndex") reduce {
      (h1, h2) => updatePro(h1, h2)
    }
    updatedSample.map { s => (s.position, s.label, s.featureIndex, s.histo.toList) }.print

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

  /*
   * update process
   */
  def updatePro(h1: Histogram, h2: Histogram): Histogram = {
    var re = new Histogram(h1.position, 0, 0, null)
    var h = (h1.histo ++ h2.histo).sortBy(_.featureValue) //accend

    if (h.size <= numBins) {
      re = new Histogram(h1.position, h1.label, h1.featureIndex, h)
    } else {
      while (h.size > numBins) {
        var minIndex = 0
        var minValue = Integer.MAX_VALUE.toDouble
        for (i <- 0 to h.size - 2) {
          if (h(i + 1).featureValue - h(i).featureValue < minValue) {
            minIndex = i
            minValue = h(i + 1).featureValue - h(i).featureValue
          }
        }
        val newfrequent = h(minIndex).frequency + h(minIndex + 1).frequency
        val newValue = (h(minIndex).featureValue * h(minIndex).frequency + h(minIndex + 1).featureValue * h(minIndex + 1).frequency) / newfrequent
        val newFea = h.take(minIndex) ++ Array(Histo(newValue, newfrequent)) ++ h.drop(minIndex + 2)
        h = newFea
      }
      re = new Histogram(h1.position, h1.label, h1.featureIndex, h)
    }
    re
  }

}
