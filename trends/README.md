To get the trend data:


In the file trends.py:
Replace the path to data with training data file path in this line 
line 15: path = "/home/path/to/data"

Replace the path with the project folder:
line 23: with open("/project/folder/" + product_id + "_scores.tsv", "w") as f:

To run the program:
./home/path/to/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --master local[*] ~/project_directory/trends.py
