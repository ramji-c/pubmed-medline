# author; Ramji Chandrasekaran
# date: 06-Feb-2017
# top level script to initiate PubMed data processing

from medline.data.load import loader
from medline.data.extract import features
from medline.model import cluster
from medline.utils import input_parser
import pandas
import argparse
import configparser
import os
import urllib
from medline.utils.export_results import export_dataframe


class PubMed:

    """cluster PubMed-Medline journals using various k-means algorithm. Input data can be PubMed abstracts in the form
    of text or XML file. Output will be a .xlsx file with each input abstract given a cluster id"""

    def __init__(self):
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)
        self._load_config()

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "config", "default.cfg")))

    def process(self, input_file, in_format, output_file, out_format):
        """resembles a data processing pipeline.
            ->load input file into a pandas dataframe
            ->transform data into Tf-Idf vector
            ->cluster the transformed data
        Parameters:
            input_file: fully qualified filename containing PubMed journals
            in_format: format of input file
            output_file: fully qualified name of output file(.xlsx) to be generated
            out_format: format of output file"""

        # create appropriate loader object
        if in_format == "xml":
            data_loader = loader.AbstractsXmlLoader(input_file)
        else:
            custom_input_parser = input_parser.AbstractsParser()
            data_loader = loader.AbstractsTextLoader(input_file, custom_input_parser)
        # load input file into a pandas dataframe
        input_dataframe = data_loader.load_(as_="dataframe")

        # clean input_dataframe; drop empty rows
        input_dataframe.dropna(inplace=True)

        # use Tf-Idf vectorizer to transform data
        vectorizer = features.FeatureExtractor()
        vectorized_data = vectorizer.vectorize_text(input_dataframe['content'])

        # cluster transformed data
        cluster_mgr = cluster.Cluster()
        cluster_ids = cluster_mgr.do_kmeans(vectorized_data)

        # extract clustering output
        output_df = pandas.DataFrame(cluster_ids, index=input_dataframe.index)
        # output_df = output_df.join(input_dataframe['title'])
        output_df = output_df.join(input_dataframe['permalink'])
        output_df.columns = ['cluster_id', 'permalink']

        cluster_kw_df = pandas.DataFrame(cluster_mgr.get_top_cluster_terms(vectorizer.get_features(), num_terms=20),
                                         columns=['top cluster keywords'])

        # base_url = self.cfg_mgr.get('output', 'permalink.base.search.url')
        # num_clusters = self.cfg_mgr.get('clustering', 'clusters.count')
        # cluster_urls = []
        # for cluster_id in range(int(num_clusters)):
        #     search_terms = pandas.Series(output_df[output_df['cluster_id'] == cluster_id]['permalink']).tolist()
        #     full_url = base_url + urllib.parse.quote(" ".join(search_terms))
        #     cluster_urls.append(full_url)
        #     full_url = ""
        # custom_output_df = pandas.DataFrame(cluster_urls, columns=['clickable content'])
        # export_dataframe(output_file, custom_output_df, format=out_format, sheet_names=['clusters'], indices=[False])
        export_dataframe(output_file, output_df, cluster_kw_df, format=out_format,
                         sheet_names=['clusters', 'cluster keywords'], indices=[False, False])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cluster PubMed articles - abstracts or summaries",
                                     usage="pubmed input_file output_file -i input_format -o output_format")
    parser.add_argument('input_file', help="fully qualified name of file containing PubMed articles")
    parser.add_argument('output_file', help="fully qualified name of clustering output file(.xslx) to be generated")
    parser.add_argument('-i', required=True, help="file format - xml or txt", choices=['xml', 'txt'])
    parser.add_argument('-o', required=True, help="file format - xlsx or csv", choices=['csv', 'xlsx'])
    args = parser.parse_args()

    pm_handler = PubMed()
    pm_handler.process(args.input_file, args.i, args.output_file, args.o)
