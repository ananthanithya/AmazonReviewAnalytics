To get the VaderSentiment scores:

In the file VaderSentiment_analysis.py:
Replace the path to results file  with  the actual path in this line 
line 8:  with open("/path/to/sentiment_results_file", "a") as f:

Replace the path to  data with the actual path in this line:
line 14: path = "/home/path/to/review_file"

Execute using:
./home/path/tp/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --master local[*] ~/project_folder/VaderSentiment_analysis.py
