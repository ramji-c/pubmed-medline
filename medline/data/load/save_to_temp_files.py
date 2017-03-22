# author: Ramji Chandrasekaran
# date: 15-02-2017
# load source data and save it in a pickled intermediate data structure

import pickle
import argparse
import os
import configparser

from medline.data.load import loader
from medline.utils import input_parser
from medline.utils.configuration import Config


class Serializer:
    """used to load large data-sets into a python dict or other data structure, which is then pickled.
    run this script before repeated analyses of large data-sets, if long XML loading times are to be avoided"""

    def __init__(self, input_path, in_format):
        if not os.path.isdir(input_path):
            raise ValueError("invalid input_path. not a directory")

        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)
        self._load_config()
        self.input_path = input_path
        self.format = in_format
        self.output_path = self.cfg_mgr.get('input', 'temp.data.directory')
        self.filepart_index = 1

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "../..", "config",
                                                       "default.cfg")))

    def create_temp_files(self):
        """parse all input files and create temporary files

        :parameter
            input: None
            output: None
        temporary files are stored in temp directory specified in default.cfg file"""
        for input_file in os.listdir(self.input_path):
            full_filename = self.input_path + input_file
            print(full_filename)
            if self.format == "xml":
                data_loader = loader.AbstractsXmlLoader(full_filename, config=Config(None))
            else:
                data_loader = loader.AbstractsTextLoader(full_filename, input_parser.AbstractsParser())
            loaded_data = data_loader.load_(as_="dict")
            output_file = self.output_path + "pubmed_tempfile" + str(self.filepart_index)
            with open(output_file, 'wb') as filehandle:
                pickle.dump(loaded_data, filehandle)
            self.filepart_index += 1


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(usage="save_to_temp_files.py input_path -i input_format",
                                         description="create temporary files by loading input data-set to python dict "
                                                     "and pickle them")
    arg_parser.add_argument("input_path", help="fully qualified path of directory where input files are stored")
    arg_parser.add_argument("-i", required=True, choices=["xml", "txt"], help="format of input file")

    args = arg_parser.parse_args()

    data_serializer = Serializer(args.input_path, args.i)
    data_serializer.create_temp_files()
