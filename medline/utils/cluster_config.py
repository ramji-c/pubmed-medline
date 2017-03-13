# author: Ramji Chandrasekaran
# date: 12-Mar-2017
# parse custom cluster config params

import configparser


class ClusterConfig:
    """parse custom config file and store params in instance variables"""

    def __init__(self, config_file):
        self.cfg_mgr = configparser.ConfigParser()
        self._load_config(config_file)

        self.NCLUSTERS = None
        self.NITER = None
        self.NTERMS = None
        self.BATCHSIZE = None
        self.MINDF = None
        self.MAXDF = None
        self.VECTORIZER = None
        self.VECTORIZER_INPUT = None
        self._load_params()

    def _load_config(self, config_file):
        self.cfg_mgr.read(config_file)

    def _load_params(self):
        """read config file and store params in variables"""

        self.NCLUSTERS = self.cfg_mgr.get('clustering', 'clusters.count')
        self.NITER = self.cfg_mgr.get('clustering', 'iterations.count')
        self.BATCHSIZE = self.cfg_mgr.get('clustering', 'kmeans.batch.size')
        self.NTERMS = self.cfg_mgr.get('clustering', 'cluster.terms.count')
        self.MINDF = self.cfg_mgr.get('feature-extraction', 'document.frequency.min')
        self.MAXDF = self.cfg_mgr.get('feature-extraction', 'document.frequency.max')
        self.VECTORIZER = self.cfg_mgr.get('feature-extraction', 'vectorizer')
        self.VECTORIZER_INPUT = self.cfg_mgr.get('feature-extraction', 'vectorizer.input.type')
