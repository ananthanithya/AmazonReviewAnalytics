# Execute using:
# ./home/spark_path/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --master local[*] ~/project/directory/trends.py

import json
from pyspark import SparkContext

def writeToTSV(item):
    with open("/home/nithya/BDS_Project/" + item[0] + "_scores.tsv", "a") as f:
        f.write("\n" + str(item[1][2]) + "\t" + str(item[1][1]))

def process(products):
    
    sc = SparkContext("local", "trends")

    path = "/home/path/to/data"
    rawDataRDD = sc.textFile(path)
    formattedRDD = rawDataRDD.map(lambda x: (json.loads(x).get("asin"), json.loads(x).get("overall"), json.loads(x).get("reviewTime")))
    keyedRDD = formattedRDD.keyBy(lambda x : x[0])
    groupByRDD = keyedRDD.groupByKey().mapValues(list)

    for product_id in products:
        filteredRDD = keyedRDD.filter(lambda x: x[0] == product_id)
        with open("/project/folder/" + product_id + "_scores.tsv", "w") as f:
            f.write("Date\tScore")
        filteredRDD.foreach(writeToTSV)

# To format date:
# from datetime import datetime
# dateobj = datetime.strptime("01 1, 2014", "%m %d, %Y")

def main():
    products = []
    products.append("B004A3XQQI")
    products.append("B003FZA1O2")
    products.append("B001L9O73U")
    products.append("B008PY8EL0")

    process(products)

if __name__ == '__main__':
    main()
