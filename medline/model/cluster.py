# author; Ramji Chandrasekaran
# date: 06-Feb-2017
# cluster input data

from sklearn.cluster import KMeans, MiniBatchKMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer
from sklearn.decomposition import LatentDirichletAllocation
import configparser
import os


class Cluster:

    """cluster input data using K-means, Minibatch-Kmeans or LDA. Input to clustering algorithms must be either
    a Tf-Idf vector or a hashing vector. tuning parameters can be configured in default.cfg file."""

    def __init__(self):
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)
        self._load_config()
        # cluster config parameters
        self.NCLUSTERS = int(self.cfg_mgr.get('clustering', 'clusters.count'))
        self.NINIT = int(self.cfg_mgr.get('clustering', 'init.count'))
        self.NITER = int(self.cfg_mgr.get('clustering', 'iterations.count'))
        self.BATCH_SIZE = int(self.cfg_mgr.get('clustering', 'kmeans.batch.size'))
        self.NTOPICS = 10

        self.model = None
        self.svd = None

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "..", "config", "default.cfg")))

    def do_kmeans(self, dataset):
        """vanilla k-means - Llyod's algorithm.
            input:
                dataset: input data in the form of a term document matrix
            output:
                labels_; a list of cluster identifiers - 1 per input document"""

        # normalization
        self.svd = TruncatedSVD(self.NCLUSTERS)
        normalizer = Normalizer(copy=False)
        lsa = make_pipeline(self.svd, normalizer)
        dataset = lsa.fit_transform(dataset)

        # finish normalization,start k-means
        self.model = KMeans(n_clusters=self.NCLUSTERS, n_init=self.NINIT)
        self.model.fit_transform(dataset)
        return self.model.labels_

    def do_minibatch_kmeans(self, dataset):
        """scalable version of k-means. used for large datasets. same input/output as k-means function"""

        self.model = MiniBatchKMeans(n_clusters=self.NCLUSTERS, n_init=self.NINIT, batch_size=self.BATCH_SIZE,
                                     max_iter=self.NITER, verbose=True)
        self.model.fit_transform(dataset)
        return self.model.labels_

    def print_top_terms(self, features, model='kmeans'):
        """print top 'n' features(cluster centers) of each cluster"""

        if model == 'kmeans':
            for ind, term in enumerate(self.get_top_cluster_terms(features, model='kmeans')):
                print("Cluster #: {0}   Top terms: {1}".format(ind, term))
        elif model == 'lda':
            for ind, term in enumerate(self.get_top_cluster_terms(features, model='lda')):
                print("Topic #: {0}   Top terms: {1}".format(ind, term))

    def get_top_cluster_terms(self, features, model='kmeans', num_terms=15):
        """get top 'n' cluster features"""

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
        """Latent Dirichlet Allocation
            input:
                dataset: input data in the form of a term-document matrix
            output:
                components_: list of topic labels for each topic"""

        self.model = LatentDirichletAllocation(n_topics=self.NTOPICS, max_iter=self.NITER)
        self.model.fit(dataset)
        return self.model.components_
