# author: Ramji Chandrasekaran
# date: 06-Feb-2017
# cluster input data

from sklearn.cluster import KMeans, MiniBatchKMeans
from sklearn.decomposition import LatentDirichletAllocation
import h2o
from h2o.estimators import H2OKMeansEstimator
from h2o.exceptions import H2OConnectionError
import logging


class Cluster:

    """cluster input data using K-means, Minibatch-Kmeans or LDA. Input to clustering algorithms must be either
    a Tf-Idf vector or a hashing vector. tuning parameters can be configured in default.cfg file."""

    def __init__(self, config):
        self.config = config
        self.model = None
        self.svd = None

        # log_file = self.config.LOG_DIR + self.config.LOGFILE
        # logging.basicConfig(format='%(asctime)s::%(levelname)s::%(message)s', level=logging.INFO, filename=log_file)

    def do_kmeans(self, dataset):
        """vanilla k-means - Llyod's algorithm.
            Input:
                :parameter dataset: input data in the form of a term document matrix

            Output:
                :returns labels_: a list of cluster identifiers - 1 per input document
                :rtype list"""

        # # normalization
        # self.svd = TruncatedSVD(self.config.NCLUSTERS)
        # normalizer = Normalizer(copy=False)
        # lsa = make_pipeline(self.svd, normalizer)
        # dataset = lsa.fit_transform(dataset)

        # finish normalization,start k-means
        self.model = KMeans(n_clusters=self.config.NCLUSTERS, n_init=self.config.NINIT, n_jobs=self.config.INIT_PCNT)
        self.model.fit_transform(dataset)
        return self.model.labels_

    def do_minibatch_kmeans(self, dataset):
        """scalable version of k-means. used for large datasets. same input/output as k-means function
            Input:
                :parameter dataset: input data in the form of a term document matrix

            Output:
                :returns labels_: a list of cluster identifiers - 1 per input document
                :rtype list"""

        self.model = MiniBatchKMeans(n_clusters=self.config.NCLUSTERS, n_init=self.config.NINIT,
                                     batch_size=self.config.BATCHSIZE, max_iter=self.config.NITER, verbose=self.config)
        self.model.fit(dataset)
        return self.model.predict(dataset)

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
            Input:
                :parameter features: list of features returned by the vectorizer
                :parameter model: name of the model. default - kmeans
                :parameter num_terms: # of terms to return. default - 15

            Output:
                :returns cluster centroids
                :rtype list"""

        top_terms = []
        if model == 'kmeans':
            # original_space_centroids = self.svd.inverse_transform(self.model.cluster_centers_)
            # order_centroids = original_space_centroids.argsort()[:, ::-1]
            order_centroids = self.model.cluster_centers_.argsort()[:, ::-1]
            for cluster_num in range(self.config.NCLUSTERS):
                top_terms.append(", ".join([features[i] for i in order_centroids[cluster_num, :num_terms]]))
        elif model == 'lda':
            for topic in self.model.components_:
                top_terms.append(", ".join([features[i] for i in topic.argsort()[:-num_terms - 1:-1]]))
        return top_terms

    def do_lda(self, dataset):
        """Latent Dirichlet Allocation
            Input:
                :parameter dataset: input data in the form of a term-document matrix

            Output:
                :return components_: list of topic labels for each topic
                :rtype list"""

        self.model = LatentDirichletAllocation(n_topics=self.config.NTOPICS, max_iter=self.config.NITER)
        self.model.fit(dataset)
        return self.model.components_

    def do_h2o_kmeans(self, dataset, server_url):
        """use the h2o module to perform k-means clustering.
            This method delegates clustering to a H2O server instance(local or remote). A connection attempt will be
            made to the provided server_url before clustering is initiated.
            input:
                :param dataset: input data - term document matrix
                :param server_url: URL of the H2O server instance on which clustering would run
            output:
                labels_: a list of cluster identifiers - 1 per input document
            :raises ConnectionError"""

        # establish connection to H20 server
        try:
            h2o.connect(url=server_url, verbose=False)
            logging.info("connected to H2O server")
            h2o_dataframe = h2o.H2OFrame(python_obj=dataset)
            self.model = H2OKMeansEstimator(max_iterations=self.config.NITER, k=self.config.NCLUSTERS, init="PlusPlus",
                                            standardize=False)
            self.model.train(training_frame=h2o_dataframe)
            logging.info("modelling complete. predicting cluster membership")
            return self.model.predict(h2o_dataframe)["predict"].as_data_frame(use_pandas=False, header=False)
        except H2OConnectionError:
            logging.error("unable to connect to H2O server @ {0}".format(server_url))
            raise ConnectionError("unable to connect to H2O server. check if server is running at specified URL")