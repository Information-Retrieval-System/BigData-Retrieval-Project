///**
// * Bespin: reference implementations of "big data" algorithms
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package ca.uwaterloo.cs451.a5
//
//import io.bespin.scala.util.Tokenizer
//import org.apache.hadoop.fs._
//import org.apache.log4j._
//import org.apache.spark.{SparkConf, SparkContext}
//import org.rogach.scallop._
//import org.apache.spark.Partitioner
//import scala.math._
//
//
//class Conf(args: Seq[String]) extends ScallopConf(args) {
//  mainOptions = Seq(input, model, shuffle, reducers)
//  val input = opt[String](descr = "input path", required = true)
//  val model = opt[String](descr = "model save path", required = true)
//  val shuffle = opt[String](descr = "model save path", required = false)
//  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
//  val num_executors = opt[Int](descr = "number of num-executors", required = false, default = Some(1))
//  val cores_executor = opt[Int](descr = "number of executor-cores", required = false, default = Some(1))
//  verify()
//}
//
//class SplitPartitioner(override val numPartitions: Int) extends Partitioner {
//  def getPartition(key: Any): Int = key match {
//    case (word1 : String, word2 : String) => { (word1.hashCode & 0x7FFFFFFF) % numPartitions }
//  }
//}
//
//
//object TrainSpamClassifier extends Tokenizer {
//  val log = Logger.getLogger(getClass().getName())
//
//
//
//  def main(argv: Array[String]) {
//    val args = new Conf(argv)
//
//    log.info("Input: " + args.input())
//    log.info("Model Path: " + args.model())
//    log.info("Shuffle On/Off: " + args.shuffle())
//    log.info("Number of reducers: " + args.reducers())
//
//    val conf = new SparkConf().setAppName("Spam classification Trainer: TrainSpamClassifier ")
//    val sc = new SparkContext(conf)
//
//    val outputDir = new Path(args.model())
//    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
//
//    val textFile = sc.textFile(args.input(), 1)
//    val shuffleBroadCast = sc.broadcast(args.shuffle())
//    printf("shuffle broadcast: " + shuffleBroadCast + "\n")
//
//    val trained = textFile
//      .map(line => {
//        val tokens = line.split(" ")//tokenize(line)
////        if (tokens.length > 1) {
////          tokens.sliding(2).map(p => List((p(0), "*"), (p(0), p(1))))
////        } else List()
//        val docid = tokens(0);
//        val isSpamStr = tokens(1);
//        var isSpam = 0
//        if(isSpamStr.equals("spam")) isSpam = 1
//
//        //printf("features in map....tokens....")
//       // printf(tokens(2).toString)
//        val features = tokens.takeRight(tokens.length-2).map(x => x.toInt).toArray;
//       // printf("features in map....")
//       // printf(features.length.toString)
//
//        val shuf = shuffleBroadCast.value.nonEmpty
//        printf("checking shuffle: " + shuf)
//        if(false) {
//          (0, (docid, isSpam, features))
//        }else{
//          val rand = new scala.util.Random
//
//          (0, (docid, isSpam, features, rand.nextInt()))
//        }
//      })
////      .map(bigram => (bigram, 1))
////      .reduceByKey(_ + _)
////      //.sortByKey(true)
////      .repartitionAndSortWithinPartitions(new SplitPartitioner(args.reducers()))
//      .groupByKey(1)
//        .map(x => {
//
//        //  print("shakti start....")
//          //printf(x.toString())
//
//          // w is the weight vector (make sure the variable is within scope)
//          val weight_vector = scala.collection.mutable.Map[Int, Double]()
//         // printf("weight_vector.toString()....checking")
//          //printf(weight_vector.toString())
//
//            x._2.foreach(y => {
//
//          // Scores a document based on its list of features.
//          def spamminess(features: Array[Int]): Double = {
//            var score = 0d
//            features.foreach(f => if (weight_vector.contains(f)) score += weight_vector(f))
//            score
//          }
//
//          // This is the main learner:
//          val delta = 0.002
//
//          // For each instance...
//          val isSpam = y._2 // label
//          val features = y._3 // feature vector of the training instance
//             // printf("features check.....")
//            //printf(features.length.toString)
//          // Update the weights as follows:
//          val score = spamminess(features)
//          val prob = 1.0 / (1 + exp(-score))
//          //val wei = (isSpam - prob) * delta
//          features.foreach(f => {
//            if (weight_vector.contains(f)) {
//              weight_vector(f) +=  (isSpam - prob) * delta
//              //printf("inside f condition...")
//             // printf(weight_vector(f).toString)
//            } else {
//              weight_vector(f) = (isSpam - prob) * delta
//              //printf("inside f condition else...")
//              //printf(weight_vector(f).toString)
//            }
//          })
//
//        })
//          //val shaq = v;
//          //val ret = weight_vector.toList
//         // print(weight_vector.keySet)
//          //println(ret(0))
////          printf("\n")
////          printf("weight_vector.toString()....checking end")
////          printf("\n")
////          printf(weight_vector.toString())
////          printf("\n")
//
//          val ret = weight_vector.toList
//////          printf("shakti end....")
//////          printf(ret.toString())
//          ret
//         // weight_vector
//        })
//      .flatMap(r => r)
////      .map(x => {
////
////       // printf("Shakti")
////        x
////      })
//    trained.saveAsTextFile(args.model())
//  }
//}
/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.math._


class ConfShuffle(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle, reducers)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model save path", required = true)
  val shuffle = opt[Boolean](descr = "Want Shuffle", required = false, default = Option(false))
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val num_executors = opt[Int](descr = "number of num-executors", required = false, default = Some(1))
  val cores_executor = opt[Int](descr = "number of executor-cores", required = false, default = Some(1))
  verify()
}

//class SplitPartitioner(override val numPartitions: Int) extends Partitioner {
//  def getPartition(key: Any): Int = key match {
//    case (word1 : String, word2 : String) => { (word1.hashCode & 0x7FFFFFFF) % numPartitions }
//  }
//}


object Index extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())



  def main(argv: Array[String]) {
    val args = new ConfShuffle(argv)

    log.info("Input: " + args.input())
    log.info("Model Path: " + args.model())
    log.info("Shuffle On/Off: " + args.shuffle())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Spam classification Trainer: TrainSpamClassifier ")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), 1)
    val shuffleBroadCast = sc.broadcast(args.shuffle())
    //printf("shuffle broadcast: " + shuffleBroadCast + "\n")

    val trained = textFile
      .map(line => {
        val tokens = line.split(" ")//tokenize(line)
        //        if (tokens.length > 1) {
        //          tokens.sliding(2).map(p => List((p(0), "*"), (p(0), p(1))))
        //        } else List()
        val docid = tokens(0);
        val isSpamStr = tokens(1);
        var isSpam = 0
        if(isSpamStr.equals("spam")) isSpam = 1

        //printf("features in map....tokens....")
        // printf(tokens(2).toString)
        val features = tokens.takeRight(tokens.length-2).map(x => x.toInt).toArray;
        // printf("features in map....")
        // printf(features.length.toString)

        val shuf = shuffleBroadCast.value
        // printf("checking shuffle: " + shuf)
        if (shuf) {
          val rand = new scala.util.Random
          (0, (docid, isSpam, features, rand.nextInt(25000)))
        } else {
          (0, (docid, isSpam, features, -1))
        }
        //((0,rand.nextInt()) , (docid, isSpam, features, rand.nextInt()))
        //((0, rand.nextInt()) , (docid, isSpam, features, rand.nextInt()))
        //(1, (isSpam, features))

      })
      //      .map(bigram => (bigram, 1))~cs451/public_html/spam/
      //      .reduceByKey(_ + _)
      //      //.sortByKey(true)
      //      .repartitionAndSortWithinPartitions(new SplitPartitioner(args.reducers()))
      .groupByKey(1)
      .map(x => {
        val w = x._2.toArray.sortBy(_._4)
        // printf("rand num: "  + w(0)._4 + "\n")
        //  print("shakti start....")
        //printf(x.toString())

        // w is the weight vector (make sure the variable is within scope)
        val weight_vector = scala.collection.mutable.Map[Int, Double]()
        // printf("weight_vector.toString()....checking")
        //printf(weight_vector.toString())

        w.foreach(y => {
          // printf("DocId: " + y._1 + "\n")

          // Scores a document based on its list of features.
          def spamminess(features: Array[Int]): Double = {
            var score = 0d
            features.foreach(f => if (weight_vector.contains(f)) score += weight_vector(f))
            score
          }

          // This is the main learner:
          val delta = 0.002

          // For each instance...
          val isSpam = y._2 // label
          val features = y._3 // feature vector of the training instance
          // printf("features check.....")
          //printf(features.length.toString)
          // Update the weights as follows:
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          //val wei = (isSpam - prob) * delta
          features.foreach(f => {
            if (weight_vector.contains(f)) {
              weight_vector(f) +=  (isSpam - prob) * delta
              //printf("inside f condition...")
              // printf(weight_vector(f).toString)
            } else {
              weight_vector(f) = (isSpam - prob) * delta
              //printf("inside f condition else...")
              //printf(weight_vector(f).toString)
            }
          })

        })
        //val shaq = v;
        //val ret = weight_vector.toList
        // print(weight_vector.keySet)
        //println(ret(0))
        //          printf("\n")
        //          printf("weight_vector.toString()....checking end")
        //          printf("\n")
        //          printf(weight_vector.toString())
        //          printf("\n")

        val ret = weight_vector.toArray
        ////          printf("shakti end....")
        ////          printf(ret.toString())
        ret
        // weight_vector
      })
      .flatMap(r => r)
    //      .map(x => {
    //
    //       // printf("Shakti")
    //        x
    //      })
    trained.saveAsTextFile(args.model())
  }
}