package org.myorg.quickstart

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink
import org.apache.flink.streaming.util.serialization._
import math._
import java.util.concurrent.TimeUnit._
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.windowing.helper.Time

object StreamWC {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.addSource(new KafkaSource[String]("ovhbhshad3:2181", "vulab123", new SimpleStringSchema))
    val datasets: DataStream[String] = stream.flatMap { _.split("\n") } filter { _.nonEmpty }
    val nonEmptyDatasets: DataStream[Array[String]] = datasets.map { s => s.split("\t") }.filter { !_.contains("") }

    val labledSample: DataStream[LabeledVector] = inputPro(nonEmptyDatasets)

    val histoSample: DataStream[Histogram] = labledSample.flatMap { s =>
      (0 until s.feature.size) map {
        index => new Histogram(s.position, s.label, index, Array(Histo(s.feature(index), 1)))
      }
    }
    val updatedSample: DataStream[Histogram] = histoSample.groupBy("position", "label", "featureIndex") reduce {
      (h1, h2) => updatePro(h1, h2)
    }

    val mergedSample: DataStream[MergedHisto] = updatedSample.map { s => new MergedHisto(s.position, s.featureIndex, s.histo) }
      .groupBy("position", "featureIndex") reduce {
        (m1, m2) => mergePro(m1, m2)
      }

    val numSample: DataStream[NumSample] = labledSample.map { s => new NumSample(s.position, 1) }.groupBy(0)
      .reduce { (s1, s2) => new NumSample(s1.position, s1.number + s2.number) }

	// numSample update every 5 sec
    val res = numSample.join(mergedSample).onWindow(5, TimeUnit.SECONDS).where("position").equalTo("position")

    res.print

    // execute program
    env.execute()
  }

  // *************************************************************************
  //  UTIL Variable
  // *************************************************************************  
  private var inputPath: String = null
  private var outputPath: String = null
  private val numBins = 5 // B bins for Update procedure
  private val numSplit = 5 //By default it should be same as numBins
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

  /*
   * merge process
   */
  def mergePro(m1: MergedHisto, m2: MergedHisto): MergedHisto = {
    var re = new MergedHisto(m1.position, 0, null)
    var h = (m1.histo ++ m2.histo).sortBy(_.featureValue) //accend
    if (h.size <= numBins) {
      re = new MergedHisto(m1.position, m1.featureIndex, h)
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
      re = new MergedHisto(m1.position, m1.featureIndex, h)
    }
    re
  }

  /*
   * uniform process
   */
  def uniformPro(sample: (NumSample, MergedHisto)): Uniform = {
    val histo = sample._2.histo //ascend
    val len = histo.length
    var u = new Array[Double](numSplit - 1)

    for (j <- 1 to numSplit - 1) {
      var s = j * sample._1.number / numSplit.toDouble

      if (s <= histo(0).frequency) {
        u(j - 1) = histo(0).featureValue
      } else {
        val totalSum = (0 until len) map { index => histo(index).frequency } reduce { _ + _ }
        val extendSum = totalSum - histo(len - 1).frequency / 2
        if (s >= extendSum) {
          u(j - 1) = histo(len - 1).featureValue
        } else {
          var i = 0
          var sumP = 0.0
          //sumPro
          while (sumP < s) {
            if (i == 0)
              sumP += histo(i).frequency / 2
            else
              sumP += histo(i).frequency / 2 + histo(i - 1).frequency / 2
            i += 1
          }
          i -= 2

          var d = s - (sumP - histo(i + 1).frequency / 2 - histo(i).frequency / 2)
          var a = histo(i + 1).frequency - histo(i).frequency
          var b = 2 * histo(i).frequency
          var c = -2 * d
          var z = if (a == 0) -c / b else (-b + sqrt(pow(b, 2) - 4 * a * c)) / (2 * a)

          u(j - 1) = histo(i).featureValue + (histo(i + 1).featureValue - histo(i).featureValue) * z
        }
      }
    }
    new Uniform(sample._1.position, sample._2.featureIndex, u)
  }

}