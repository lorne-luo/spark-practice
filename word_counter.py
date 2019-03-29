import logging
# Creating Spark Configuration and Spark Context-
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Word Counter")
sc = SparkContext(conf=conf)

logger = logging.getLogger(__name__)

# Reading the file-
myTextFile = sc.textFile("/usr/local/Cellar/apache-spark/2.4.0/README.md")

# Removing the empty lines-
non_emptyLines = myTextFile.filter(lambda line: len(line) > 0)

# Return a new RDD "words" by first applying "split()" function to all elements of this RDD, and
# then flattening the results.
words = non_emptyLines.flatMap(lambda x: x.split(' '))

# Executing three different functions-
# a) .map() - it takes each line of the rdd "words" which is now a list of words, then creates a
# tuple like ('apple', 1) etc.
# b) .reduceByKey() - it merges the values for each key using an associative and commutative reduce
# function. e.g. ('apple', 5) etc.
# c) .map() - It just change the position on the tupple as (5, 'apple') and sorts the key descending

wordCount = words.map(lambda x: (x, 1)). \
    reduceByKey(lambda x, y: x + y). \
    map(lambda x: (x[1], x[0])).sortByKey(False)

logger.info('#' * 20)
logger.info(type(wordCount))

# Save this RDD as a text file, using string representations of elements.
# Note: It creates part-00000, part-00001 ... files which shows how the job has been performed
# across multiple partions (executor nodes)

wordCount.saveAsTextFile("/tmp/wordCountResult")

# To make this as a single file, you can just repartion it using coalesce().
# It returns a new RDD that is reduced into `numPartitions` partitions.

wordCount.coalesce(1).saveAsTextFile("/tmp/wordCountResult2")

# Both these functions creates a file showing "SUCCESS" if it is successfully written.
# These files are calles .CRC file which stands for "Cyclic Redundancy Check". It's an error
# detecting code which is used to detect accidental changes to raw data.


# spark-submit --master spark://butterfly070.local:7077  word_counter.py