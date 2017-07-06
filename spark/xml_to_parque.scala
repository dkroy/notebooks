import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark._
import scala.xml.XML

case class Document(lang: String, xml: String)

def extractDocument(body:String):Document = {
  val doc = XML.loadString(body)
  val docLang = (doc \\ "ArchiveDoc" \ "Article" \ "@lang").headOption.map(x => x.text).getOrElse("")
  Document(docLang, body)
}

sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<DistDoc>")
sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</DistDoc>")

val input = sc.newAPIHadoopFile("gs://input*", classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])

val docs = input.map(x => x._2.toString.filter(_ >= ' ')).map(x => x.replace("<DistDoc>", s"""<DistDoc xmlns="http://xml.dowjones.net/distdoc/v8" xmlns:pd="PrivateData" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">""")).map(extractDocument)
val df = docs.toDF()

df.write.parquet("gs://output/*”)

//now read then extract the xml and dump to output
val docsP = sqlContext.read.parquet("gs://output/*”)
val enDocs = docsP.filter($"lang"==="en").select("xml")
enDocs.saveAsTextFile("gs://output/type=xml/“)