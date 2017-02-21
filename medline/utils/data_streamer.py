# author; Ramji Chandrasekaran
# date: 16-Feb-2017
# stream data from temporary files

import pickle
import queue
import logging


class DataStreamer:
    """stream data from pickled temporary files for use by Hashing vectorizer"""

    def __init__(self, files):
        DataStreamer.files = files
        DataStreamer.docs_queue = queue.deque([])
        DataStreamer.file_queue = queue.deque(files)
        logging.basicConfig(format='%(asctime)s::%(levelname)s::%(message)s', level=logging.INFO)
        DataStreamer.doc_id_list = []

    @staticmethod
    def read():
        if not DataStreamer.docs_queue:
            DataStreamer.docs_queue = queue.deque(list(DataStreamer._load_next_batch()))
        data = DataStreamer.docs_queue.popleft()
        DataStreamer.doc_id_list.append(data[0].split("\\")[-1])
        return data[1]

    @staticmethod
    def _load_next_batch():
        file = DataStreamer.file_queue.popleft()
        logging.info("loading temp file: {0}".format(file))
        with open(file, 'rb') as filehandle:
            doc_dict = pickle.load(filehandle)
            for doc_id, doc in doc_dict.items():
                yield doc['permalink'], doc['content']
