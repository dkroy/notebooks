import pyspark
import pyspark.sql.SqlContext

from xml.sax import ContentHandler, parseString
​
class DistDocHandler(ContentHandler):
    def __init__(self):
        self.lang = "en"
    def startElement(self, tag, attributes):
        if tag == "Article":
            self.lang = attributes["lang"]
​
def extractDoc(distdoc):
    h = DistDocHandler()
    try:
        parseString(distdoc, handler)
    except:
        return "en"
    return handler.lang
​
sc = SparkContext()
sqlContext = SqlContent(sc)
​
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "<DistDoc")
rdd = sc.textFile("gs://input/*") \
         .map(lambda x: '<DistDoc' + x) \
         .map(lambda x: x.replace('\n', '').replace('\r', '')) \
         .map(lambda x: (extractDoc(x), x))

df = sqlContext.createDataFrame(rdd, ['lang', 'xml'])
df.write.parquet("gs://output/")
​
dfDoc = sqlContext.read.parquet("gs://output=parquet/*")
dfDoc.filter(dfDoc.lang == "en").select(dfDoc.xml) \
    .rdd \
    .saveAsTextFile("gs://output=text/")