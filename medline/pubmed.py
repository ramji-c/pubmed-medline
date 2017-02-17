# author; Ramji Chandrasekaran
# date: 06-Feb-2017
# top level script to initiate PubMed data processing

from medline.data.load import loader
from medline.data.extract import features
from medline.model import cluster
from medline.utils import input_parser, data_streamer
from medline.utils.export_results import export_dataframe

import pandas
import argparse
import configparser
import os
import logging


class PubMed:

    """cluster PubMed-Medline journals using k-means algorithm. Input data can be PubMed abstracts in the form
    of text or XML file. Output will be a .xlsx or .csv file with each input abstract assigned a cluster id and top
    cluster keywords/terms for each cluster"""

    def __init__(self):
        logging.basicConfig(format='%(asctime)s::%(levelname)s::%(message)s', level=logging.INFO)
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)
        self._load_config()

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "config", "default.cfg")))

    def process(self, input_file, in_format, output_file, out_format, large_file):
        """resembles a data processing pipeline.
            ->load input file into a pandas dataframe(for file size < 2 GB)
            ->transform data into Tf-Idf or Hashing vector
            ->cluster the transformed data
        Parameters:
            input_file: fully qualified filename containing PubMed journals
            in_format: format of input file
            output_file: fully qualified name of output file(.xlsx) to be generated
            out_format: format of output file
            large_file: flag to indicate if input file is larger than 2 GB"""

        logging.info("Processing starts")
        # create appropriate loader object
        if in_format == "xml":
            if large_file:
                data_loader = loader.AbstractsXmlSplitLoader(input_file, threshold=100000)
            else:
                data_loader = loader.AbstractsXmlLoader(input_file)
        else:
            custom_input_parser = input_parser.AbstractsParser()
            data_loader = loader.AbstractsTextLoader(input_file, custom_input_parser)

        if large_file:
            self._process_large_file(data_loader, output_file, out_format)
        else:
            # smaller datasets can be processed using pandas dataframe and any in-memory vectorizer
            self._process_with_dataframe(data_loader, output_file, out_format)

    def _process_large_file(self, data_loader, output_file, out_format):
        """stream data from temporary files to a hashing vectorizer to reduce memory overload"""

        # load and stream input data
        logging.info("saving input data into temporary files")
        total_docs, temp_data_files = data_loader.load_(as_="files")
        print(temp_data_files)
        datastreamer_obj = data_streamer.DataStreamer(temp_data_files)

        # use Hashing vectorizer to transform data
        logging.info("transforming text - with Hashing vectorizer")
        vectorizer = features.FeatureExtractor(vectorizer_type='hashing')
        vectorized_data = vectorizer.vectorize_text([doc for doc in datastreamer_obj.read()])

        # cluster transformed data
        logging.info("clustering begins")
        cluster_mgr = cluster.Cluster()
        cluster_ids = cluster_mgr.do_kmeans(vectorized_data)

    def _process_with_dataframe(self, data_loader, output_file, out_format):
        """load data into pandas dataframe and use in-memory tf-idf vectorizer to process data"""

        # load input file into a pandas dataframe
        input_dataframe = data_loader.load_(as_="dataframe")

        # clean input_dataframe; drop empty rows
        input_dataframe.dropna(inplace=True)

        # use Tf-Idf vectorizer to transform data
        logging.info("transforming text - with Tf-Idf vectorizer")
        vectorizer = features.FeatureExtractor()
        vectorized_data = vectorizer.vectorize_text(input_dataframe['content'])

        # cluster transformed data
        logging.info("clustering begins")
        cluster_mgr = cluster.Cluster()
        cluster_ids = cluster_mgr.do_kmeans(vectorized_data)

        # extract clustering output
        output_df = pandas.DataFrame(cluster_ids, index=input_dataframe.index)
        output_df = output_df.join(input_dataframe['permalink'])
        output_df.columns = ['cluster_id', 'permalink']

        cluster_kw_df = pandas.DataFrame(cluster_mgr.get_top_cluster_terms(vectorizer.get_features(), num_terms=20),
                                         columns=['top cluster keywords'])
        export_dataframe(output_file, output_df, cluster_kw_df, format=out_format,
                         sheet_names=['clusters', 'cluster keywords'], indices=[False, False])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cluster PubMed articles - abstracts or summaries",
                                     usage="pubmed input_file output_file -i input_format -o output_format "
                                           "[--large-file]")
    parser.add_argument('input_file', help="fully qualified name of file containing PubMed articles")
    parser.add_argument('output_file', help="fully qualified name of clustering output file(.xslx) to be generated")
    parser.add_argument('-i', required=True, help="file format - xml or txt", choices=['xml', 'txt'])
    parser.add_argument('-o', required=True, help="file format - xlsx or csv", choices=['csv', 'xlsx'])
    parser.add_argument('--large-file', action='store_true', default=False,
                        help="set this flag for files larger than 2 GB")
    args = parser.parse_args()

    pm_handler = PubMed()
    pm_handler.process(args.input_file, args.i, args.output_file, args.o, args.large_file)
