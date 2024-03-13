import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.Random

package FinalProject {


  object NewDriver {
    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      // Setup Spark session
      val conf = new SparkConf().setAppName("NameOfApp")
        .setMaster("local[4]")
      val sc = new SparkContext(conf)

      val text = sc.textFile("adult.csv") // If running on server, put Hadoop input file path
      val header = text.first()
      // adjust the numeric data in RDD to be 4 categories representing percentiles
      val rows = text.filter(line => line != header)
        .map(line => line.split(","))
      // Array of (featureName, featureIdx, (25th, 50th, 75th) percentiles)
      val numericalFeatures = Array(
        ("fnlwgt", 3, find_percentiles(rows.map(r => r(3).toInt))),
        ("hours-per-week", 13, find_percentiles(rows.map(r => r(13).toInt)))
      )
      val values = rows.map(r => {
        numericalFeatures.foreach(f => {
          r(f._2) = getKeyFromPercentile(r(f._2).toInt, f._3)
        })
        r
      })

      //Splitting Training and Testing (80/20)
      val trainTest = values.randomSplit(Array(0.8, 0.2), 1)
      val training = trainTest(0)
      val testing = trainTest(1)
      println("training length:" ,training.collect().length)
      println("testing length:", testing.collect().length)
      println("Training Positive Length", training.filter(x => x(x.length - 1) == ">50K").collect().length)

      //getting the answers
      val testAnswers = testing.map(row => (row(0), row(row.length - 1)))
      val trainAnswers = training.map(row => (row(0), row(row.length - 1)))

      // rowNumber,age,workclass 2,fnlwgt 3*,education 4,education-num 5*,marital-status 6,occupation 7,relationship 8,race 9,
      // sex 10,capital-gain 11*,capital-loss 12*,hours-per-week 13*,native-country 14,income 15
      // (featureName, featureIdx)
      val featureNames = Array(("workclass", 2), ("education", 4), ("marital-status", 6), ("race", 9), ("native-country", 14))
      val allFeatures = Array(("workclass", 2), ("fnlwft", 3), ("education", 4), ("marital-status", 6), ("occupation", 7),
        ("relationship", 8), ("race", 9), ("sex", 10), ("hours-per-week", 13), ("native-country", 14))

      //code to run a forest
//      val forest = randomForest(training, allFeatures)
//      val testOutput = testing.map(x => (x(0), evaluateRandomForest(forest, x)))
//      val trainOutput = training.map(x => (x(0), evaluateRandomForest(forest, x)))
//      //Build tree on train Data
      val decisionTree = NewDecisionTree(maxDepth = 7)
      val parentNode = decisionTree.create_tree(training, 0, allFeatures, null)


//      decisionTree.recursive_print(parentNode)
      //Feed Test Data into Tree and get Results
      val testOutput = testing.map(x => (x(0), decisionTree.evaluate(x, parentNode)))
      val trainOutput = training.map(x => (x(0), decisionTree.evaluate(x, parentNode)))


      //computing F-Score for testAnswers
      val joinedAnsOut = testAnswers.join(testOutput)
//      joinedAnsOut.collect().foreach(println)
      val tp = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual == predicted && actual == ">50K" }).collect().length
      val fp = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted == ">50K" }).collect().length
      val fn = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted == "<=50K" }).collect().length
      val tn = joinedAnsOut
        .filter({case (rownum, (actual, predicted)) => actual == predicted && actual == "<=50K"}).collect().length

      val precision = tp.toDouble / (tp + fp)
      val recall = tp.toDouble / (tp + fn)
      val accuracy = (tp + tn).toDouble / (tp + fp + fn + tn).toDouble

      val fscore = 2 * (precision * recall) / (precision + recall)
      println("Testing", "f: ", fscore, "accur: ", accuracy, "prec: ", precision, "rec: ", recall, tp, fp, fn, tn)


      //computing F-Score for train Answers
      val joinedTrainAnsOut = trainAnswers.join(trainOutput)
//      joinedTrainAnsOut.collect().foreach(println)
      val trtp = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual == predicted && actual.equals(">50K") }).collect().length
      val trfp = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted.equals(">50K") }).collect().length
      val trfn = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted.equals("<=50K") }).collect().length
      val trtn = joinedTrainAnsOut
        .filter({case (rownum, (actual, predicted)) => actual == predicted && actual.equals("<=50K")}).collect().length

      val trprecision = trtp.toDouble / (trtp + trfp)
      val trrecall = trtp.toDouble / (trtp + trfn)
      val traccuracy = (trtp + tn).toDouble / (trtp + trfp + trfn + trtn).toDouble

      val trfscore = 2 * (trprecision * trrecall) / (trprecision + trrecall)
      println("Training", "f: ", trfscore, "accur:", traccuracy,  "prec: ", trprecision, "rec: ", trrecall, trtp, trfp, trfn, trtn)
//      decisionTree.recursive_print(parentNode, 5)
    }

    def getKeyFromPercentile(value: Int, percentiles: (Int, Int, Int)): String = {
      if (value < percentiles._1) {
        "0"
      } else if (value < percentiles._2) {
        "1"
      } else if (value < percentiles._3) {
        "2"
      } else {
        "3"
      }
    }

    // Given feature of integers, finds 25th, 50th, and 75th percentiles
    def find_percentiles(data: RDD[Int]): (Int, Int, Int) = {
      val featureVals = data.collect().sorted(Ordering[Int])
      (featureVals((featureVals.length * 0.25).toInt), featureVals((featureVals.length * 0.5).toInt), featureVals((featureVals.length * 0.75).toInt))
    }

    //all usable features is a length of 10. so the square root is 3, so we just need 3 random features
    def randomForest(data: RDD[Array[String]], features: Array[(String, Int)]): Array[(Array[(String, Int)], NewTreeNode)] = {
      val iterations = Array.range(1, 10)
      val decisionTree = NewDecisionTree(maxDepth = 5)
      //      val featureCount = Math.sqrt(features.length * 1.0).toInt
      val featureSplits = iterations.map(x => getThreeRandomFeatures(features))
      featureSplits.foreach((x) => println(x.mkString(",")))
      featureSplits.map({ case subFeatures => (subFeatures, decisionTree.create_tree(data, 0, subFeatures, null)) })
    }

    def getThreeRandomFeatures(features: Array[(String, Int)]) : Array[(String, Int)] = {
      val shuffled = Random.shuffle(features.toSeq)
      Array(shuffled(0), shuffled(1), shuffled(3))
    }

    def evaluateRandomForest(forest: Array[(Array[(String, Int)], NewTreeNode)], oneRow: Array[String]): String = {
      val decisionTree = NewDecisionTree(maxDepth = 5)
      val decisions = forest.map({case (features, parentNode) => decisionTree.evaluate(oneRow, parentNode)})
      val classOne = decisions.filter(x => x== ">50K").length
      val classTwo = decisions.filter(x => x == "<=50K").length
      if (classOne > classTwo) {">50K"}
      else {"<=50K"}
    }
  }
}