from vaderSentiment.vaderSentiment import sentiment as vaderSentiment
import json

#get vaderSentiment scores for each review
def calculate_Sentiment(item):
    reviews = []
    values_list = item[1]

    for review in values_list:
        with open("/path/to/sentiment_results_file", "a") as f:
            sentiment_score = vaderSentiment(review[2])['compound']
            f.write("\n Product ID: " + str(item[0]) + "   Review: " + str(review[2]) + "   User Rating: " + str(review[1]) + "   Sentiment Score: " + str(sentiment_score))

path = "/path/to/review_file"
rawDataRDD = sc.textFile(path)

#load data into rdd
jsonDataRDD = rawDataRDD.map(lambda x: (json.loads(x).get("asin"), json.loads(x).get("overall"), json.loads(x).get("reviewText")))
keyedRDD = jsonDataRDD.keyBy(lambda x : x[0])
#group reviews by product id
groupByRDD = keyedRDD.groupByKey().mapValues(list)

groupByRDD.foreach(calculate_Sentiment)