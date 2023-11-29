// Necessary imports using Flink
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

// Defining document
case class Document(docid: String, content: String)

object StreamingIndex {
  var totalDocLength: Double = 0.0
  var count: Int = 0
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Obtain stream in input data
    val documentStream: DataStream[Document] = env.readTextFile("Shakespeare.txt")
      .map(new MapFunction[String, Document] {
        override def map(value: String): Document = {
          val tokens = value.split(" ")
          Document(tokens(0), value)
        }
      })

    // Indexing Stream
    val indexedStream: DataStream[String] = documentStream.map(new MapFunction[Document, String] {
      override def map(doc: Document): String = {
        val docLength = doc.content.split(" ").length // Doc length
        val index = createIndex(doc.content)    // Index
        val compressedIndex = compressIndex(index)    // Compressed index
        val gapEncodedLength = gapEncode(docLength)    // GapEncoded length
        s"${doc.docid},$index,$compressedIndex,$gapEncodedLength"
      }

      // Creating index by joining commas
      def createIndex(content: String): String = {
        content.split(" ").mkString(",")
      }

      // Compression by removing commas
      def compressIndex(index: String): String = {
        index.replaceAll(",", "")
      }

      // GAP encoding: when the first document, print as is. Else, find the gap between current and earlier doc lengths
      def gapEncode(length: Int): String = {
        val encodedLength = if (count == 0) {
          length.toString
        } else {
          val gap = length - totalDocLength.toInt
          gap.toString
        }
        totalDocLength = length.toDouble
        count += 1
        encodedLength
      }
    })

    // Calculate running average document length per stream of data
    val avgDocLengthStream: DataStream[Double] = documentStream
      .map(doc => doc.content.split(" ").length.toDouble)
      .timeWindowAll(Time.seconds(60)) // 1 min
      .reduce((a, b) => a + b)
      .map(avg => avg / 60.0)

    indexedStream.print("Indexed Stream")
    avgDocLengthStream.print("Running Average Document Length")
    env.execute("Stream Indexing")
  }
}

