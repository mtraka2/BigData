import sys
import os
import re


os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python2.7'
os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages com.databricks:spark-csv_2.10:1.5.0 pyspark-shell')
os.environ['JAVA_TOOL_OPTIONS'] = "-Dhttps.protocols=TLSv1.2"

sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.9-src.zip')



from pyspark import SparkContext
from pyspark import HiveContext


sc = SparkContext()
sqlContext = HiveContext(sc)


text_file = sc.textFile('file:///home/cloudera/Downloads/df_pos.csv')

count = text_file.flatMap(lambda line: line.split()) \
             .map(lambda line: line.lower())\
             .map(lambda line: re.sub("[^a-zA-Z]","", line)) \
             .filter(lambda line: len(line) >= 3) \
             .map(lambda char: (char,1))\
             .reduceByKey(lambda a, b : a + b)\
             .map(lambda (a,b): (b,a))\
             .sortByKey(ascending=False)





#for i in count.collect(): print(str(i[0])+"\t\t"+str(i[1]))
count.map(lambda x:'%s\t%s' %(x[1],x[0])).saveAsTextFile('file:///home/cloudera/Downloads/word_count_pos')


#sc.stop()


