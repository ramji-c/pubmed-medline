# author: Ramji Chandrasekaran
# date: 12-Mar-2017
# parse custom cluster config params

import configparser
import os


class Config:
    """parse custom config file and store params in instance variables"""

    def __init__(self, config_file):
        self.cfg_mgr = configparser.ConfigParser()
        self.config_file = config_file
        self._load_config(self.config_file)

        # cluster config params
        self.NCLUSTERS = None
        self.NITER = None
        self.NINIT = None
        self.NTERMS = None
        self.BATCHSIZE = None
        self.MINDF = None
        self.MAXDF = None
        self.VERBOSITY = None
        self.INIT_PCNT = None

        # feature extraction config params
        self.VECTORIZER = None
        self.VECTORIZER_INPUT = None
        self.GEN_KW = None
        self.DIM = None
        self.NORM = None
        self.VECTORIZED_FILES_DIR = None

        # framework config params
        self.LOG_DIR = None
        self.LOGFILE = None
        self.PERMALINK_URL = None
        self.INFILE_TYPE = None
        self.RECORD_SEP = None
        self.TEMP_DIR = None

        # load all config params
        self._load_params()

    @property
    def config_file(self):
        return self._config_file

    @config_file.setter
    def config_file(self, filename):
        # load given config file if not None, otherwise load default.cfg
        if not filename:
            filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/", "default.cfg"))
        self._config_file = filename

    def _load_config(self, config_file):
        self.cfg_mgr.read(config_file)

    def _load_params(self):
        """read config file and store params in variables"""

        self.NCLUSTERS = int(self.cfg_mgr.get('clustering', 'clusters.count'))
        self.NITER = int(self.cfg_mgr.get('clustering', 'iterations.count'))
        self.BATCHSIZE = int(self.cfg_mgr.get('clustering', 'kmeans.batch.size'))
        self.NTERMS = int(self.cfg_mgr.get('clustering', 'cluster.terms.count'))
        self.MINDF = float(self.cfg_mgr.get('feature-extraction', 'document.frequency.min'))
        self.MAXDF = float(self.cfg_mgr.get('feature-extraction', 'document.frequency.max'))
        self.VECTORIZER = self.cfg_mgr.get('feature-extraction', 'vectorizer')
        self.VECTORIZER_INPUT = self.cfg_mgr.get('feature-extraction', 'vectorizer.input.type')
        self.LOG_DIR = self.cfg_mgr.get('logging', 'logging.directory')
        self.LOGFILE = self.cfg_mgr.get('logging', 'log.filename')
        self.PERMALINK_URL = self.cfg_mgr.get('output', 'permalink.base.search.url')
        self.INFILE_TYPE = self.cfg_mgr.get('input', 'input.file.type')
        self.RECORD_SEP = self.cfg_mgr.get('input', 'abstracts.record.separator')
        self.NINIT = int(self.cfg_mgr.get('clustering', 'init.count'))
        self.VERBOSITY = bool(int(self.cfg_mgr.get('clustering', 'verbosity')))
        self.TEMP_DIR = self.cfg_mgr.get('input', 'temp.data.directory')
        self.GEN_KW = bool(int(self.cfg_mgr.get('feature-extraction', 'vectorizer.features.avail')))
        self.DIM = int(self.cfg_mgr.get('feature-extraction', 'features.dimension'))
        self.NORM = self.cfg_mgr.get('feature-extraction', 'normalization')
        self.INIT_PCNT = int(self.cfg_mgr.get('clustering', 'init.process.count'))
        self.VECTORIZED_FILES_DIR = self.cfg_mgr.get('feature-extraction', 'features.pickled.files.directory')
