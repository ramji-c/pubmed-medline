# author: Ramji Chandrasekaran
# date: 15-02-2017
# load source data and save it in a pickled intermediate data structure

import pickle
from medline.data.load import loader
from medline.utils import input_parser
import argparse


class Serializer:
    """use this script to load large datasets into a python dict or other data structure, which is then pickled.
    run this script before repeated analysis of a large dataset, if long data loading times are to be avoided"""

    def __init__(self, input_file, in_format, output_file):

        if in_format == "xml":
            self.data_loader = loader.AbstractsXmlLoader(input_file)
        else:
            self.data_loader = loader.AbstractsTextLoader(input_file, input_parser.AbstractsParser())

        self.out_file = output_file

    def save_(self):
        loaded_data = self.data_loader.load_(as_="dict")
        with open(self.out_file, 'wb') as filehandle:
            pickle.dump(loaded_data, filehandle)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(usage="serialize.py input_file output_file -i input_format",
                                         description="load input dataset to python dict and pickle it")
    arg_parser.add_argument("input_file", help="fully qualified path of input file")
    arg_parser.add_argument("output_file", help="fully qualified path of output file")
    arg_parser.add_argument("-i", required=True, choices=["xml", "txt"], help="format of input file")

    args = arg_parser.parse_args()

    data_serializer = Serializer(args.input_file, args.i, args.output_file)
    data_serializer.save_()
