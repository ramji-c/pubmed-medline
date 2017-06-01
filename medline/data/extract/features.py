# author: Ramji Chandrasekaran
# date: 06-Feb-2017
# feature extraction from input data

from sklearn.feature_extraction.text import TfidfVectorizer, HashingVectorizer, TfidfTransformer
from sklearn.pipeline import make_pipeline
from nltk.stem import SnowballStemmer
import numpy


class FeatureExtractor:
    """extract features from input data - typically by vectorizing the data using Tf-Idf or Hashing vectorizers
    tuning parameters for feature extraction can be specified in default.cfg file under 'feature-extraction' section"""

    def __init__(self, config, vectorizer_type='tfidf'):
        self.stemmer = SnowballStemmer('english')
        self.config = config
        self._vectorizer = None
        self.vectorizer_type = vectorizer_type
        self.vector_features = []
        self.lda_model = None

    @property
    def vectorizer(self):
        return self._vectorizer

    @vectorizer.setter
    def vectorizer(self, vec_type):
        """instantiate a vectorizer: tf-idf or hashing

            for large input files, a Hashing vectorizer could be used if there are memory limitations
            input:
                :parameter vec_type: type of vectorizer to use - either tfidf or hashing
            output:
                :return None
                :raises ValueError"""

        if vec_type == 'tfidf':
            self._vectorizer = TfidfVectorizer(input=self.config.VECTORIZER_INPUT, stop_words='english',
                                               norm=self.config.NORM, analyzer='word', max_features=10000,
                                               min_df=self.config.MINDF, max_df=self.config.MAXDF)
        elif vec_type == 'hashing':
            self._vectorizer = make_pipeline(HashingVectorizer(input=self.config.VECTORIZER_INPUT, stop_words='english',
                                                               norm=self.config.NORM, analyzer='word'),
                                             TfidfTransformer(norm=self.config.NORM))
        else:
            raise ValueError("unsupported vectorizer type. value must be one of tfidf, hashing")

    def vectorize_text(self, text):
        """perform feature extraction by converting data to a term-document matrix

            input:
                :parameter text: raw input data - list of documents
            output:
                :return vectorized_text: term-document matrix
                :rtype numpy.NDarray"""

        vectorized_text = self.vectorizer.fit_transform(text)
        if self.vectorizer_type == 'tfidf':
            self.vector_features = self.vectorizer.get_feature_names()
        return vectorized_text

    def get_features(self):
        return self.vector_features
