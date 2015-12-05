import ast
import nltk
import string
import os

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector, DenseVector
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.clustering import KMeansModel
from  pyspark.mllib.feature import HashingTF
from pyspark.mllib.tree import DecisionTree
import itertools
from sklearn.naive_bayes import MultinomialNB
from numpy import array
from pyspark.mllib.feature import IDF
from pyspark.mllib.linalg import Vector
from random import shuffle
import numpy as np
from sklearn.decomposition import PCA
from pyspark.mllib.feature import PCA as PCAmllib

# In[6]:

cachedStopWords = stopwords.words("english")
stemmer = PorterStemmer()

#stemmer
def stem_tokens(tokens, stemmer):
	stemmed = list()
	for item in tokens:
		if item not in cachedStopWords and item not in string.punctuation:
			stemmed.append(stemmer.stem(item))
	return stemmed

#tokenize sentences to get a list of tokens
def tokenize(text):
	tokens = nltk.word_tokenize(text)
	stems = stem_tokens(tokens, stemmer)
	return stems

#get tokens and label from review
def processReview(line):
    
	json = ast.literal_eval(line.strip())
	reversestring = ''
	if len(json['reviewText'] ) > 0:
		tokenizedwords=tokenize(json['reviewText'])

		reversestring=' '.join(tokenizedwords)
	return (reversestring,json['overall'],json['asin'])

#get label for review
def getLabel(line):
	json = ast.literal_eval(line.strip())
	return json['overall']

#get tokens and labels for each review
def formatData(sc, doc):       
	words = sc.textFile(doc)
	filtered_reviews_sc = words.flatMap(lambda line: line.split("\n")).map(lambda line: processReview(line))
	return filtered_reviews_sc

#get labels for all reviews
def formatLabels(sc, doc):
	words = sc.textFile(doc)
	labels = words.flatMap(lambda line: line.split("\n")).map(lambda line: getLabel(line))
	return labels


#get tf vector for a review
def getTFVector(review):
	htf = HashingTF(1000)
	doc = review.split()
	return htf.transform(doc).toArray()

#get tf vectors for all reviews
def buildFeatures1(review_label_rdd):	
	return review_label_rdd.map(lambda x: (getTFVector(x[0]), x[1]))
	

#train Naive Bayes model
def trainNaiveBayes(features, labels):
	clf = MultinomialNB()
	clf.fit(features, labels)
	return clf

#PCA to reduce to two dimensions
def reduceDimensions(features_rdd):
	model = PCAmllib(2).fit(features_rdd)
	transformed_rdd = model.transform(features_rdd)
	return transformed_rdd

#calculate sentiment score
def score(probabilities):
	
	probabilities = probabilities.tolist()[0]
	print probabilities
	idx = probabilities.index(max(probabilities))
	print idx, probabilities[idx]
	offset = probabilities[idx] * 20
	if idx == 0:
		return offset, idx + 1
	elif idx == 1:
		return 20 + offset, idx + 1
	elif idx == 2:
		return 40 + offset, idx + 1
	elif idx == 3:
		return 60 + offset, idx + 1
	else:
		return 80 + offset, idx + 1


#Predict sentiment score for test dataset
def predict_NB(clf, test_reviews_labels_rdd):	
	prediction_rdd = test_reviews_labels_rdd.map(lambda x: (x[2],x[0], score(clf.predict_proba(getTFVector(x[0]))),x[1]))
	grouped_pred_rdd = prediction_rdd.keyBy(lambda x: x[0]).groupByKey()
	return grouped_pred_rdd
	