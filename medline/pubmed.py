# author: Ramji Chandrasekaran
# date: 06-Feb-2017
# top level script to initiate PubMed data processing

from medline.data.load import loader
from medline.data.extract import features
from medline.model import cluster
from medline.utils import input_parser, data_streamer
from medline.utils.export_results import export_dataframe
from medline.utils.collate_results import collate_
from medline.utils.configuration import Config

import logging
import pandas
import argparse
import numpy
import os
import pickle
from datetime import datetime


class PubMed:
    """cluster PubMed-Medline journals using k-means algorithm. Input data are PubMed abstracts in the form
    of text or XML files. Output will be a .xlsx or .csv file with each input abstract assigned a cluster id and top
    cluster keywords/terms extracted from each cluster"""

    def __init__(self, config_file):
        # load configuration file
        self.config = Config(config_file=config_file)

        log_file = self.config.LOG_DIR + self.config.LOGFILE
        # clear all config handlers - to workaround a mysterious bug
        logging.getLogger().handlers = []
        logging.basicConfig(format='%(asctime)s::%(levelname)s::%(message)s', level=logging.INFO, filename=log_file)

    def process(self, input_file, in_format, output_file, out_format, vectorized_file, num_docs,
                large_file, use_temp_files, collate, use_h2o, h2o_url):
        """resembles a data processing pipeline.
            ->load input file into a pandas data frame (for file size < 2 GB)
            ->transform data into Tf-Idf or Hashing vector
            ->cluster the transformed data
        Parameters:
            input_file: fully qualified filename containing PubMed journals
            in_format: format of input file
            output_file: fully qualified name of output file(.xlsx) to be generated
            out_format: format of output file
            large_file: flag to indicate if input file is larger than 2 GB
            use_temp_files: use temporary pre-processed files(if available) and skip loading input file
            num_docs: number of documents to be clustered
            collate: flag to indicate if output should be collated into 1 record per cluster
            use_h2o: flag to indicate if processing should be delegated to H2O server cluster
            h20_url: URL of H2O serve to connect to

        :rtype None"""

        logging.info("Processing begins..initializing appropriate loader class")
        # create appropriate loader object
        if in_format == "xml":
            if large_file:
                if use_temp_files and num_docs == 0:
                    raise ValueError("param num_docs must be a non-zero value when use_temp_files flag is set to True")
                data_loader = loader.AbstractsXmlSplitLoader(filename=input_file, config=self.config,
                                                             use_temp_files=use_temp_files, num_docs=num_docs)
            else:
                data_loader = loader.AbstractsXmlLoader(filename=input_file, config=self.config)
        else:
            custom_input_parser = input_parser.AbstractsParser()
            data_loader = loader.AbstractsTextLoader(input_file, config=self.config, parser=custom_input_parser)

        if large_file:
            self._process_large_file(data_loader, output_file, out_format, collate, vectorized_file, use_h2o, h2o_url)
        else:
            # smaller datasets can be processed using pandas data frame and any in-memory vectorizer
            self._process_normal_file(data_loader, output_file, out_format, collate)

    def _process_large_file(self, data_loader, output_file, out_format, collate, vectorized_file, use_h2o, h2o_url):
        """stream data from temporary files to a hashing vectorizer to reduce memory overload
            Input:
                :parameter data_loader: loader object
                :parameter output_file: fully qualified path of output file
                :parameter collate: flag to collate results
                :parameter vectorized_file: file containing features extracted from source data
                :parameter use_h2o: flag to indicate if processing should be delegated to H2O server cluster

            :rtype None"""

        cluster_kw = None
        vectorized_data_dict = {}
        # form fully qualified path of feature_file
        if vectorized_file:
            vectorized_file_fullname = self.config.VECTORIZED_FILES_DIR + vectorized_file
            if os.path.lexists(vectorized_file_fullname):
                # skip data loading and load pre-vectorized data and vectorizer
                logging.info("skipping data loading and vectorizing steps")
                with open(vectorized_file_fullname, 'rb') as filehandle:
                    vectorized_data_dict = pickle.load(filehandle)
                    vectorized_data = vectorized_data_dict['data']
                    pmid_list = vectorized_data_dict['labels']
                    feature_extractor = vectorized_data_dict['feature_extractor']
                logging.info("loaded vectorized data from {0}".format(vectorized_file_fullname))
        else:
            # load and stream input data
            logging.info("large file detected..streaming input data")
            total_docs, temp_data_files = data_loader.load_(as_="files")
            datastreamer_obj = data_streamer.DataStreamer(temp_data_files)
            pmid_list = datastreamer_obj.doc_id_list

            # use Hashing or tf-idf vectorizer to transform data
            logging.info("transforming text - with {0} vectorizer".format(self.config.VECTORIZER))
            feature_extractor = features.FeatureExtractor(vectorizer_type=self.config.VECTORIZER, config=self.config)
            feature_extractor.vectorizer = self.config.VECTORIZER
            vectorized_data = feature_extractor.vectorize_text([datastreamer_obj]*total_docs)

            # pickle the vectorized data and vectorizer to be re-used
            vectorized_file_fullname = self.config.VECTORIZED_FILES_DIR + \
                                       "vectorized_{0}_".format(self.config.VECTORIZER) + \
                                       str(datetime.now().time()).replace(":", ".")
            vectorized_data_dict['data'] = vectorized_data
            vectorized_data_dict['labels'] = pmid_list
            vectorized_data_dict['feature_extractor'] = feature_extractor
            with open(vectorized_file_fullname, 'wb') as filehandle:
                pickle.dump(vectorized_data_dict, filehandle)
            logging.info("saved vectorized data and vectorizer to {0}".format(vectorized_file_fullname))

            # cluster transformed data
        logging.info("clustering begins")
        cluster_mgr = cluster.Cluster(config=self.config)
        if use_h2o:
            logging.info("clustering using H2O server")
            # override H2O server URL in config
            if h2o_url:
                server_url = h2o_url
            else:
                server_url = self.config.H2O_SERVER_URL
            cluster_ids = cluster_mgr.do_h2o_kmeans(vectorized_data, server_url=server_url)
        else:
            logging.info("clustering using scikit-learn")
            cluster_ids = cluster_mgr.do_minibatch_kmeans(vectorized_data)
        logging.info("clustering complete..gathering output")

        # merge cluster id of each document with its permalink id
        out_list = list(zip(cluster_ids, [pmid for pmid in pmid_list]))
        output_df = pandas.DataFrame.from_records(out_list, index=numpy.arange(len(out_list)))
        output_df.columns = ['cluster_id', 'permalink']

        if self.config.GEN_KW:
            cluster_kw = cluster_mgr.get_top_cluster_terms(feature_extractor.get_features(),
                                                           num_terms=self.config.NTERMS)
        self._gen_output_file(output_file, output_df, out_format, keywords=cluster_kw, kw_df=self.config.GEN_KW,
                              collate=collate)

    def _process_normal_file(self, data_loader, output_file, out_format, collate):
        """load data into pandas dataframe and use in-memory tf-idf vectorizer to process data
            Input:
                :parameter data_loader: loader object
                :parameter output_file: fully qualified path of output file
                :parameter collate: flag to collate results

            :rtype None"""

        cluster_kw = None
        # load input file into a pandas data frame
        input_dataframe = data_loader.load_(as_="dataframe")

        # clean input_dataframe; drop empty rows
        input_dataframe.dropna(inplace=True)

        # use Tf-Idf vectorizer to transform data
        logging.info("transforming text - with {0} vectorizer".format(self.config.VECTORIZER))
        feature_extractor = features.FeatureExtractor(vectorizer_type=self.config.VECTORIZER, config=self.config)
        feature_extractor.vectorizer = self.config.VECTORIZER
        vectorized_data = feature_extractor.vectorize_text(input_dataframe['content'])

        # write vectorized text to file
        numpy.save(self.config.TEMP_DIR + "vectorized_text", vectorized_data.todense())

        # cluster transformed data
        logging.info("clustering begins")
        cluster_mgr = cluster.Cluster(config=self.config)
        cluster_ids = cluster_mgr.do_kmeans(vectorized_data)
        logging.info("clustering complete..gathering output")

        # extract clustering output
        output_df = pandas.DataFrame(cluster_ids, index=input_dataframe.index)
        output_df = output_df.join(input_dataframe['permalink'])
        output_df.columns = ['cluster_id', 'permalink']

        if self.config.GEN_KW:
            cluster_kw = cluster_mgr.get_top_cluster_terms(feature_extractor.get_features(),
                                                           num_terms=self.config.NTERMS)
        self._gen_output_file(output_file, output_df, out_format, keywords=cluster_kw, kw_df=self.config.GEN_KW,
                              collate=collate)

    def _gen_output_file(self, output_file, output_df, out_format, keywords=None, kw_df=False, collate=False):
        """generate output file by exporting dataframe(s)
            cluster membership dataframe is exported by default. optionally cluster keywords dataframe is also exported
            Input:
                :parameter output_file: fully qualified path of output file
                :parameter output_df: dataframe containing cluster membership
                :parameter out_format: format of output file - csv or xlsx
                :parameter keywords: list of cluster keywords(centroids)
                :parameter kw_df: flag to indicate if cluster keyword dataframe should be exported
                :parameter collate: flag to indicate if results should be collated

            :rtype None"""

        if collate:
            base_url = self.config.PERMALINK_URL
            num_clusters = self.config.NCLUSTERS
            output_df = collate_(output_df, base_url, num_clusters)
        if kw_df:
            if not keywords:
                raise ValueError("param keywords is None; required to generate top cluster keywords dataframe")
            cluster_kw_df = pandas.DataFrame(keywords, columns=['cluster keywords'])
            export_dataframe(output_file, output_df, cluster_kw_df, format=out_format,
                             sheet_names=['clusters', 'cluster keywords'], indices=[False, False])
        else:
            export_dataframe(output_file, output_df, format=out_format, sheet_names=['clusters'], indices=[False])
        logging.info("Processing complete. check output file for clustering results")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cluster PubMed articles - abstracts or summaries",
                                     usage="pubmed input_file output_file -i input_format -o output_format "
                                           "[--num-docs #] [--large-file] [--use-temp-files]")
    parser.add_argument('input_file', help="fully qualified name of file containing PubMed articles")
    parser.add_argument('output_file', help="fully qualified name of clustering output file(.xslx) to be generated")
    parser.add_argument('-i', required=True, help="file format - xml or txt", choices=['xml', 'txt'])
    parser.add_argument('-o', required=True, help="file format - xlsx or csv", choices=['csv', 'xlsx'])
    parser.add_argument('--num-docs', default=0, help="# of documents in input file. required if --use-temp-files flag "
                                                      "is set or clustering should be restricted to subset of input")
    parser.add_argument('--config-file', help="fully qualified path of config file")
    parser.add_argument('--vectorized-file', default=None, help="name of features file to be used as input to cluster")
    parser.add_argument('--large-file', action='store_true', default=False,
                        help="set this flag for files larger than 2 GB")
    parser.add_argument('--use-temp-files', action='store_true', default=False,
                        help='set this flag if processing should use pre-processed files stored in temporary directory')
    parser.add_argument("--collate", action='store_true', default=False,
                        help="set this flag if output file should contain collated cluster results")
    parser.add_argument("--use-h2o", action='store_true', default=False,
                        help="set this flag if processing should be done using H2O server cluster")
    parser.add_argument("--h2o-url", default=None, help="URL of the H2O server to connect")
    args = parser.parse_args()

    pm_handler = PubMed(config_file=args.config_file)
    pm_handler.process(input_file=args.input_file, in_format=args.i, output_file=args.output_file, out_format=args.o,
                       num_docs=int(args.num_docs), vectorized_file=args.vectorized_file,
                       large_file=args.large_file, use_temp_files=args.use_temp_files, collate=args.collate,
                       use_h2o=args.use_h2o, h2o_url=args.h2o_url)
