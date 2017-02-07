# author; Ramji Chandrasekaran
# date: 05-Feb-2017

import os
import configparser
import pandas

from medline.utils import input_parser


class Loader:

    """abstract base class"""

    def _validate_file(self):
        raise NotImplementedError

    def load_(self, as_, limit):
        raise NotImplementedError

    def _read_file(self):
        raise NotImplementedError


class AbstractsLoader(Loader):

    """Loads PubMed data from input file.
    Accepted inputs formats: .txt"""

    def __init__(self, filename, parser=input_parser.DefaultParser()):
        self.filename = filename
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)
        self.data_parser = parser

        # load config file
        self._load_config()
        # validate input file
        self._validate_file()

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "..\..", "config", "default.cfg")))

    def _validate_file(self):
        supported_formats = tuple(self.cfg_mgr.get('input', 'input.file.type').split(","))
        if os.path.isdir(self.filename):
            raise ValueError("filename is a directory. expected: a file")
        else:
            if not os.path.basename(self.filename).lower().endswith(supported_formats):
                raise ValueError("invalid file extension. supported formats: {0}".format(supported_formats))

    def load_(self, as_="dataframe", limit=100):
        """load input data file into a format specified. supports pandas dataframe
        Parameters:
            as_: data structure to load data into. default = pandas dataframe
            limit: # of data items to be loaded. default = 100
        """
        data_dict = {}
        data_index = 0
        for data in self.__collate_data():
            data_dict[data_index] = self.data_parser.parse_(data)
            data_index += 1
            if data_index >= limit:
                break
        return pandas.DataFrame.from_dict(data_dict, orient='index')

    def __collate_data(self):
        """collates read data into logical segments separating one data item from another"""

        collated_data = ""
        last_line = ""
        record_sep = self.cfg_mgr.get('input', 'abstracts.record.separator')

        for data in self._read_file():
            if data == "\n" and last_line == "\n":
                yield collated_data
                collated_data = ""
            elif data == "\n":
                collated_data += "\n" + record_sep + "\n"
            else:
                collated_data += data
            last_line = data

    def _read_file(self):
        """read input file and return 1 line of data at a time"""

        with open(self.filename, 'r', encoding='utf-8') as file:
            for data in file:
                yield data
