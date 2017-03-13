# author: Ramji Chandrasekaran
# date: 06-Feb-2017
# cluster input data

from sklearn.cluster import KMeans, MiniBatchKMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer
from sklearn.decomposition import LatentDirichletAllocation


class Cluster:

    """cluster input data using K-means, Minibatch-Kmeans or LDA. Input to clustering algorithms must be either
    a Tf-Idf vector or a hashing vector. tuning parameters can be configured in default.cfg file."""

    def __init__(self, config):
        self.config = config
        self.model = None
        self.svd = None

    def do_kmeans(self, dataset):
        """vanilla k-means - Llyod's algorithm.
            input:
                :parameter dataset: input data in the form of a term document matrix
            output:
                :returns labels_: a list of cluster identifiers - 1 per input document
                :rtype list"""

        # normalization
        self.svd = TruncatedSVD(self.config.NCLUSTERS)
        normalizer = Normalizer(copy=False)
        lsa = make_pipeline(self.svd, normalizer)
        dataset = lsa.fit_transform(dataset)

        # finish normalization,start k-means
        self.model = KMeans(n_clusters=self.config.NCLUSTERS, n_init=self.config.NINIT)
        self.model.fit_transform(dataset)
        return self.model.labels_

    def do_minibatch_kmeans(self, dataset):
        """scalable version of k-means. used for large datasets. same input/output as k-means function
            input:
                :parameter dataset: input data in the form of a term document matrix
            output:
                :returns labels_: a list of cluster identifiers - 1 per input document
                :rtype list"""

        self.model = MiniBatchKMeans(n_clusters=self.config.NCLUSTERS, n_init=self.config.NINIT,
                                     batch_size=self.config.BATCHSIZE, max_iter=self.config.NITER, verbose=self.config)
        self.model.fit_transform(dataset)
        return self.model.labels_

    def print_top_terms(self, features, model='kmeans'):
        """print top 'n' features(cluster centers) of each cluster
            Inputs:
                :parameter features: list of features returned by the vectorizer
                :parameter model: name of the model. default - kmeans"""

        if model == 'kmeans':
            for ind, term in enumerate(self.get_top_cluster_terms(features, model='kmeans')):
                print("Cluster #: {0}   Top terms: {1}".format(ind, term))
        elif model == 'lda':
            for ind, term in enumerate(self.get_top_cluster_terms(features, model='lda')):
                print("Topic #: {0}   Top terms: {1}".format(ind, term))

    def get_top_cluster_terms(self, features, model='kmeans', num_terms=15):
        """get top 'n' cluster features that constitute cluster centroids
            Inputs:
                :parameter features: list of features returned by the vectorizer
                :parameter model: name of the model. default - kmeans
                :parameter num_terms: # of terms to return. default - 15
            Output:
                :returns cluster centroids
                :rtype list"""

        top_terms = []
        if model == 'kmeans':
            original_space_centroids = self.svd.inverse_transform(self.model.cluster_centers_)
            order_centroids = original_space_centroids.argsort()[:, ::-1]
            for cluster_num in range(self.config.NCLUSTERS):
                top_terms.append(", ".join([features[i] for i in order_centroids[cluster_num, :num_terms]]))
        elif model == 'lda':
            for topic in self.model.components_:
                top_terms.append(", ".join([features[i] for i in topic.argsort()[:-num_terms - 1:-1]]))
        return top_terms

    def do_lda(self, dataset):
        """Latent Dirichlet Allocation
            input:
                :parameter dataset: input data in the form of a term-document matrix
            output:
                :return components_: list of topic labels for each topic
                :rtype list"""

        self.model = LatentDirichletAllocation(n_topics=self.config.NTOPICS, max_iter=self.config.NITER)
        self.model.fit(dataset)
        return self.model.components_
