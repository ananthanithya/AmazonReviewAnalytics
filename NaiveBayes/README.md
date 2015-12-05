To run the Naive Bayes classifier for gathering sentiment scores:  
  
In the file review_analysis.py:  
Replace the path to data with training data file path in this line   
line 5: train_reviews_labels_rdd = formatData(sc, '/path/to/training_data_file')  
  
Replace the path to test data with the actual path in this line:  
line 22: test_reviews_labels_rdd = formatData(sc,'/path/to/test_data_file')  
  
To run the program:  
./home/path/to/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --master local[*] ~/project_directory/review_analysis.py  
  
The results are saved in a directory called nb_pred  