# author; Ramji Chandrasekaran
# date: 16-Feb-2017
# stream data from temporary files

import pickle
import queue


class DataStreamer:
    """stream data from pickled temporary files for use by Hashing vectorizer"""

    def __init__(self, files):
        DataStreamer.files = files

    @staticmethod
    def read():
        for file in DataStreamer.files:
            with open(file, 'rb') as filehandle:
                doc_dict = pickle.load(filehandle)
                for doc_id, doc in doc_dict.items():
                    yield doc['permalink'], doc['content']

