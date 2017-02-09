# author; Ramji Chandrasekaran
# date: 06-Feb-2017
# top level script to initiate PubMed data processing

from medline.data.load import loader
from medline.data.extract import features
from medline.model import cluster
from medline.utils import input_parser
import pandas
import argparse


class PubMed:

    """cluster PubMed Medline journals using various clustering algorithms"""

    def process(self, input_file, output_file):
        """resembles a data processing pipeline.
            ->load input file into a pandas dataframe
            ->transform data into Tf-Idf vector
            ->cluster the transformed data
        Parameters:
            input_file: fully qualified filename containing PubMed journals
            output_file: fully qualified name of output file(.xlsx) to be generated"""

        # load input file into a pandas dataframe
        custom_input_parser = input_parser.AbstractsParser()
        data_loader = loader.AbstractsXmlLoader(input_file)
        input_dataframe = data_loader.load_(as_="dataframe")
        # self._export_results('temp_out.xlsx', input_dataframe, format='xlsx', sheet_names=['clusters'], indices=[False])

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
        output_df = output_df.join(input_dataframe['title'])
        output_df = output_df.join(input_dataframe['permalink'])
        self._export_results(output_file, output_df, format='xlsx', sheet_names=['clusters'], indices=[False])

    def _export_results(self, filename, *dataframes, **fileparams):
        file_format = fileparams['format']
        if file_format == 'xlsx':
            writer = pandas.ExcelWriter(filename)
            for ind, dataframe in enumerate(dataframes):
                sheet_name = fileparams['sheet_names'][ind]
                keep_index = fileparams['indices'][ind]
                dataframe.to_excel(writer, sheet_name=sheet_name, engine='xlsxwriter', index=keep_index)
            writer.save()
            writer.close()
        elif file_format == 'csv':
            dataframes[0].to_csv(filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cluster PubMed articles - abstracts or summaries",
                                     usage="pubmed input_file output_file")
    parser.add_argument('input_file', help="fully qualified name of file containing PubMed articles")
    parser.add_argument('output_file', help="fully qualified name of clustering output file(.xslx) to be generated")
    args = parser.parse_args()

    pm_handler = PubMed()
    pm_handler.process(args.input_file, args.output_file)
