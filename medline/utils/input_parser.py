# author; Ramji Chandrasekaran
# date: 05-Feb-2017
# input data parser

import configparser
import os
import re


class InputParser:
    """abstract base class for pubmed data parsers"""

    def parse_(self, data):
        raise NotImplementedError


class DefaultParser(InputParser):

    """default parser implementation"""

    def parse_(self, data):
        return data


class AbstractsParser(InputParser):
    """parse PubMed abstract text files. Abstracts are made up of several sections typically delimited by \n\n.
    With use of regualr expressions, each abstract is parsed and its contents stored section-wise in a python dict"""

    def __init__(self):
        self.cfg_mgr = configparser.ConfigParser()
        self.script_dir = os.path.dirname(__file__)

        # load config file
        self._load_config()

    def _load_config(self):
        self.cfg_mgr.read(os.path.abspath(os.path.join(self.script_dir, "..", "config", "default.cfg")))

    def _extract_pmid(self, data):
        pattern = re.compile("PMID\:\s+(\d{8})")
        try:
            pmid = re.search(pattern, data).groups()[0]
        except AttributeError:
            pmid = None
        return pmid

    def parse_(self, data):
        """parse collated input data and filter unwanted data"""

        parsed_text = {}
        content_ind = int(self.cfg_mgr.get('input', 'abstracts.parser.content.index'))
        permalink_ind = int(self.cfg_mgr.get('input', 'abstracts.parser.permalink.index'))
        title_ind = int(self.cfg_mgr.get('input', 'abstracts.parser.title.index'))
        record_sep = self.cfg_mgr.get('input', 'abstracts.record.separator')

        try:
            data_split = data.split(record_sep)
            parsed_text["content"] = data_split[content_ind]
            parsed_text["title"] = data_split[title_ind]
            parsed_text["permalink"] = self._extract_pmid(data_split[permalink_ind])
        except IndexError:
            print("Invalid record format")
        return parsed_text
