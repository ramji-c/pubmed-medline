# author; Ramji Chandrasekaran
# date: 06-Feb-2017
# cluster input data

from sklearn.cluster import KMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer
from sklearn.decomposition import LatentDirichletAllocation
import configparser
import os


class Cluster:

    """cluster input data using K-means or Latent Dirichlet Allocation. Input to clustering algorithms must be either
    a Tf-Idf vector or a simple count vector. tuning parameters can be configured in default.cfg file."""

    def __init__(self):
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)

        # load config file
        self._load_config()

        self.NCLUSTERS = int(self.cfg_mgr.get('clustering', 'clusters.count'))
        self.NITER = int(self.cfg_mgr.get('clustering', 'iterations.count'))
        self.NTOPICS = 10

        self.model = None
        self.svd = None

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "..", "config", "default.cfg")))

    def do_kmeans(self, dataset):
        # normalization
        self.svd = TruncatedSVD(self.NCLUSTERS)
        normalizer = Normalizer(copy=False)
        lsa = make_pipeline(self.svd, normalizer)
        dataset = lsa.fit_transform(dataset)
        # finish normalization,start k-means
        self.model = KMeans(n_clusters=self.NCLUSTERS, n_init=self.NITER)
        self.model.fit_transform(dataset)
        return self.model.labels_

    def print_top_terms(self, features, model='kmeans'):
        if model == 'kmeans':
            for ind, term in enumerate(self.get_top_cluster_terms(features, model='kmeans')):
                print("Cluster #: {0}   Top terms: {1}".format(ind, term))
        elif model == 'lda':
            for ind, term in enumerate(self.get_top_cluster_terms(features, model='lda')):
                print("Topic #: {0}   Top terms: {1}".format(ind, term))

    def get_top_cluster_terms(self, features, model='kmeans', num_terms=15):
        top_terms = []
        if model == 'kmeans':
            original_space_centroids = self.svd.inverse_transform(self.model.cluster_centers_)
            order_centroids = original_space_centroids.argsort()[:, ::-1]
            for cluster_num in range(self.NCLUSTERS):
                top_terms.append(", ".join([features[i] for i in order_centroids[cluster_num, :num_terms]]))
        elif model == 'lda':
            for topic in self.model.components_:
                top_terms.append(", ".join([features[i] for i in topic.argsort()[:-num_terms - 1:-1]]))
        return top_terms

    def do_lda(self, dataset):
        self.model = LatentDirichletAllocation(n_topics=self.NTOPICS, max_iter=self.NITER)
        self.model.fit(dataset)
        return self.model.components_
