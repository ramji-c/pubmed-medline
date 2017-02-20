# author; Ramji Chandrasekaran
# date: 06-Feb-2017
# feature extraction from input data

from sklearn.feature_extraction.text import TfidfVectorizer, HashingVectorizer, TfidfTransformer
from sklearn.pipeline import make_pipeline
from nltk.stem import SnowballStemmer
import configparser


class FeatureExtractor:
    """extract features from input data - typically by vectorizing the data using Tf-Idf or Hashing vectorizers
    tuning parameters for feature extraction can be specified in default.cfg file under 'feature-extraction' section"""

    def __init__(self, vectorizer_type='tfidf'):
        self.stemmer = SnowballStemmer('english')
        self.vectorizer = self._get_vectorizer(vectorizer_type)
        self.vectorizer_type = vectorizer_type
        self.vector_features = []
        self.lda_model = None
        self.config_handler = configparser.ConfigParser()

    def vectorize_text(self, text):
        vectorized_text = self.vectorizer.fit_transform(text)
        if self.vectorizer_type == 'tfidf':
            self.vector_features = self.vectorizer.get_feature_names()
        return vectorized_text

    def get_features(self):
        return self.vector_features

    def _get_vectorizer(self, type):
        if type == 'tfidf':
            return TfidfVectorizer(stop_words='english', analyzer='word')
        elif type == 'hashing':
            return make_pipeline(HashingVectorizer(input='content', stop_words='english', analyzer='word'),
                                 TfidfTransformer())
        else:
            raise ValueError("unsupported vectorizer type. value must be one of tfidf, hashing")
