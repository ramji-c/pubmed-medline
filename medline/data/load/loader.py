# author; Ramji Chandrasekaran
# date: 05-Feb-2017

import os
import configparser
import pandas
import pickle
import logging
from xml.sax.handler import ContentHandler
from xml.sax import parse

from medline.utils import input_parser


class Loader(object):

    """base class"""

    def __init__(self):
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "../..", "config",
                                                       "default.cfg")))

    def _validate_file(self, filename):
        """validates input file format and type. skips validation if filename is NA

        input:
            :parameter filename: fully qualified path of input file"""

        # skip validation; input file not used for processing
        if filename == "NA":
            return
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
    """Loads PubMed data from input .txt file."""

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
    """load PubMed abstracts from .xml file"""

    def __init__(self, filename, parser=input_parser.DefaultParser()):
        super(AbstractsXmlLoader, self).__init__()
        logging.basicConfig(format='%(asctime)s::%(levelname)s::%(message)s', level=logging.INFO)
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

    def _read_file(self):
        return self.filename

    def load_(self, as_, limit=None):
        """load input data file into a format specified. supports pandas dataframe
           Parameters:
                as_: data structure to load data into.
               limit: # of data items to be loaded. default = None
        """

        # parse the input xml file
        parse(self._read_file(), self)

        # parsing complete. return the collated data
        if as_ == "dataframe":
            return pandas.DataFrame.from_dict(self.data_dict, orient='index')
        else:
            return self.data_dict

    def _get_content(self):
        content = " ".join(self.char_buffer).strip()
        self.char_buffer = []
        return content

    def _flush_char_buffer(self):
        self.char_buffer = []

    # SAX parser callback functions section
    def endDocument(self):
        logging.info("XML file parsing complete. read {0} documents".format(self.data_index-1))

    def startDocument(self):
        logging.info("Begin XML file parsing")

    def endElement(self, name):
        try:
            if name == "PubmedArticle":
                # if the 'content' is missing, make 'title' of the document its 'content'. delete the document if both
                # title and content are missing
                if 'content' not in self.data_dict[self.data_index] or \
                                'permalink' not in self.data_dict[self.data_index]:
                    try:
                        self.data_dict[self.data_index]['content'] = self.data_dict[self.data_index]['title']
                    except KeyError:
                        del self.data_dict[self.data_index]
            elif name == "ArticleTitle":
                self.data_dict[self.data_index]['title'] = self._get_content()
            elif name == "Abstract":
                self.data_dict[self.data_index]['content'] = self._get_content()
            elif name == "PMID":
                self.data_dict[self.data_index]['permalink'] = self._get_content()
            elif name == "DateCreated":
                pass
            else:
                pass
        except KeyError:
            logging.warning("Invalid xml element - end tag missing")

    def startElement(self, name, attrs):
        if name == "PubmedArticle":
            self.data_index += 1
            self.data_dict[self.data_index] = {}
            self._flush_char_buffer()
        elif name == "ArticleTitle" or name == "Abstract" or name == "PMID" or name == "DateCreated":
            self._flush_char_buffer()
        else:
            pass

    def characters(self, content):
        self.char_buffer.append(content)


class AbstractsXmlSplitLoader(AbstractsXmlLoader):
    """parse PubMed input .xml file but pickle subsets of extracted data in a temporary folder.
       parsing of input file can be skipped if pre-processed temporary files are available.
       extends AbstractsXmlLoader"""

    def __init__(self, filename, threshold=100000, use_temp_files=False, num_docs=0):
        super(AbstractsXmlSplitLoader, self).__init__(filename)

        self.use_temp_files = use_temp_files
        self.threshold = threshold
        self.num_docs_read = 0
        self.temp_filenames = []
        self.temp_files_dir = self.cfg_mgr.get('input', 'temp.data.directory')
        self.temp_file_basename = "filepart."
        self.filepart = 1
        self.num_docs_processed = num_docs

    def endDocument(self):
        logging.info("XML file parsing complete")
        self._check_and_save_temporary_file(eof=True)
        logging.info("# invalid xml documents: {0}".format(self.num_docs_read - self.num_docs_processed))

    def endElement(self, name):
        super(AbstractsXmlSplitLoader, self).endElement(name)
        if name == "PubmedArticle":
            self._check_and_save_temporary_file()
            self.num_docs_processed += 1

    def startElement(self, name, attrs):
        super(AbstractsXmlSplitLoader, self).startElement(name, attrs)
        if name == "PubmedArticle":
            self.num_docs_read += 1

    def load_(self, as_, limit=None):
        """load input data file into a specified format. Skips loading input file if use_temp_files flag is set to True
           and pre-processed temporary files are available
              Parameters:
                   as_: data structure to load data into. supports only "files"
                   limit: # of data items to be loaded. default = None
        """

        # exit if as_ value is not 'files'
        if as_ != "files":
            raise ValueError("invalid value for param as_. only 'files' is supported. For other purposes use "
                             "AbstractXmlLoader class")
        else:
            # check if use_temp_files flag is set
            if self.use_temp_files:
                # check if non-empty temp directory exists
                if os.path.lexists(self.temp_files_dir) and len(os.listdir(self.temp_files_dir)) > 0:
                    logging.info("non-empty temp directory found. returning temp files for processing")
                    return self.num_docs_processed, [self.temp_files_dir + file for file in os.listdir(self.temp_files_dir)]
                else:
                    if self.filename == "NA":
                        logging.error("temp directory missing & input file is set to NA. processing aborted")
                    else:
                        logging.info("temp directory missing. ignoring use_temp_files flag and loading source file")

            # parse the input xml file
            parse(self._read_file(), self)
            logging.info("total docs processed: {0}".format(self.num_docs_processed))
            return self.num_docs_processed, self.temp_filenames

    def _check_and_save_temporary_file(self, eof=False):
        """ check if # documents read is a multiple of 'threshold'; if so, pickle the data read so far and flush holding
        data structures"""

        if eof or self.data_index >= (self.filepart * self.threshold):
            # threshold reached - pickle in-memory data and flush data structures
            logging.info("threshold reached. saving data to temporary file")
            full_filename = self.temp_files_dir + self.temp_file_basename + str(self.filepart)
            try:
                with open(full_filename, 'wb') as filehandle:
                    pickle.dump(self.data_dict, filehandle)
                    self.filepart += 1
                    self.temp_filenames.append(full_filename)
            except IOError:
                logging.error("unable to save temporary data file")

            # flush data structures
            self.data_dict.clear()
