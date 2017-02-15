# author; Ramji Chandrasekaran
# date: 05-Feb-2017

import os
import configparser
import pandas
from xml.sax.handler import ContentHandler
from xml.sax import parse
from medline.utils import input_parser


class Loader:

    """base class"""

    def __init__(self):
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "..\..", "config", "default.cfg")))

    def _validate_file(self, filename):
        """validates input file format and type
        Parameters:
            filename: fully qualified path of input file"""

        supported_formats = tuple(self.cfg_mgr.get('input', 'input.file.type').split(","))
        if os.path.isdir(filename):
            raise ValueError("filename is a directory. expected: a file")
        else:
            if not os.path.basename(filename).lower().endswith(supported_formats):
                raise ValueError("invalid file extension. supported formats: {0}".format(supported_formats))

    def load_(self, as_, limit):
        raise NotImplementedError

    def _read_file(self):
        raise NotImplementedError


class AbstractsTextLoader(Loader):

    """Loads PubMed data from input file.
    Accepted inputs formats: .txt"""

    def __init__(self, filename, parser=input_parser.DefaultParser()):
        super(AbstractsTextLoader).__init__()
        self.filename = filename
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)
        self.data_parser = parser

        # load config file
        self._load_config()
        # validate input file
        self._validate_file(self.filename)

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "..\..", "config", "default.cfg")))

    def load_(self, as_="dataframe", limit=100):
        """load input data file into a format specified. supports pandas dataframe
        Parameters:
            as_: data structure to load data into. default = dataframe
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
            if data.startswith("PMID"):
                yield collated_data
                collated_data = ""
            elif data == "\n" and last_line == "\n":
                pass
            elif data == "\n":
                collated_data += record_sep
            else:
                collated_data += data
            last_line = data

    def _read_file(self):
        """read input file and return 1 line of data at a time"""

        with open(self.filename, 'r', encoding='utf-8') as file:
            for data in file:
                yield data


class AbstractsXmlLoader(Loader, ContentHandler):

    """load PubMed abstracts from file
    Accepted inputs: .xml"""

    def __init__(self, filename, parser=input_parser.DefaultParser()):
        super(AbstractsXmlLoader).__init__()
        self.filename = filename
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)
        self.data_parser = parser
        self.data_dict = {}
        self.data_index = -1
        self.char_buffer = []

        # load config file
        self._load_config()
        # validate input file
        self._validate_file(self.filename)

        # extract config params
        self.pmid_base_url = self.cfg_mgr.get('output', 'permalink.base.url')

    # def _load_config(self):
    #     self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "..\..", "config", "default.cfg")))

    def _read_file(self):
        return self.filename

    def load_(self, as_, limit=None):
        """load input data file into a format specified. supports pandas dataframe
           Parameters:
                as_: data structure to load data into. default = dataframe
               limit: # of data items to be loaded. default = None
        """

        # parse the input xml file
        parse(self._read_file(), self)

        # parsing complete. return the collated data
        return pandas.DataFrame.from_dict(self.data_dict, orient='index')

    def _get_content(self):
        content = " ".join(self.char_buffer).strip()
        self.char_buffer = []
        return content

    def _flush_char_buffer(self):
        self.char_buffer = []

    def endDocument(self):
        print("XML file parsing complete")

    def startDocument(self):
        print("Begin XML file parsing")

    def endElement(self, name):
        try:
            if name == "PubmedArticle":
                pass
            elif name == "ArticleTitle":
                self.data_dict[self.data_index]['title'] = self._get_content()
            elif name == "Abstract":
                self.data_dict[self.data_index]['content'] = self._get_content()
            elif name == "PMID":
                self.data_dict[self.data_index]['permalink'] = self.pmid_base_url + self._get_content()
            elif name == "DateCreated":
                pass
            else:
                pass
        except KeyError:
            print("Invalid xml - start tag missing")

    def startElement(self, name, attrs):
        if name == "PubmedArticle":
            print("Reading Article #: {0}".format(self.data_index+1))
            self.data_index += 1
            self.data_dict[self.data_index] = {}
            self._flush_char_buffer()
        elif name == "ArticleTitle" or name == "Abstract" or name == "PMID" or name == "DateCreated":
            self._flush_char_buffer()
        else:
            pass

    def characters(self, content):
        self.char_buffer.append(content)
