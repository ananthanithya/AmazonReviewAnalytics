from nlp_utils import *
from operator import itemgetter

#get labels and tokenized reviews
train_reviews_labels_rdd = formatData(sc, '/path/to/training_data_file')

#Get tf vectors for all reviews
train_features_labels_rdd = buildFeatures1(train_reviews_labels_rdd)

#Get list of training examples
train_features_labels = train_features_labels_rdd.collect()

#separate labels from tf vectors
train_features_labels_list =  zip(*train_features_labels)
train_features = train_features_labels_list[0]
train_labels = train_features_labels_list[1]

#train the classifier
clf = trainNaiveBayes(train_features, train_labels)

#get test data
test_reviews_labels_rdd = formatData(sc,'/path/to/test_data_file')

#group test data by product id
grouped_rdd = predict_NB(clf, test_reviews_labels_rdd)
grouped_rdd1 =  grouped_rdd.map(lambda x : (x[0], list(x[1])))
#sort each review by sentiment score for each product 
sorted_grouped_rdd = grouped_rdd1.map(lambda x: (x[0],sorted(x[1], key = itemgetter(2), reverse = True)))
sorted_grouped_data = sorted_grouped_rdd.collect()
#save to file
sc.parallelize(sorted_grouped_data).saveAstextFile('sorted_nb_pred_200')


