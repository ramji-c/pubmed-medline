# author; Ramji Chandrasekaran
# date: 06-Feb-2017
# feature extraction from input data

from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.stem import SnowballStemmer
import configparser


class FeatureExtractor:

    """extract features from input data - typically by vectorizing the data using Tf-Idf or Count vectorizers
    tuning parameters for feature extraction can be specified in default.cfg file under 'feature-extraction' section"""

    def __init__(self):
        self.stemmer = SnowballStemmer('english')
        self.vectorizer = TfidfVectorizer(stop_words='english', analyzer='word')
        self.vector_features = []
        self.lda_model = None
        self.config_handler = configparser.ConfigParser()

    def vectorize_text(self, text):
        vectorized_text = self.vectorizer.fit_transform(text)
        self.vector_features = self.vectorizer.get_feature_names()
        return vectorized_text

    def get_features(self):
        return self.vector_features
