# author: Ramji Chandrasekaran
# date: 22-Mar-2017

import argparse
import pickle
import os


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(usage="doc_count.py temp_dir", description="count PubMed articles")
    arg_parser.add_argument("temp_dir", help="fully qualified path of temp files directory")
    args = arg_parser.parse_args()

    num_articles = 0
    pickled_dict = {}
    for filename in os.listdir(args.temp_dir):
        with open(args.temp_dir + filename, 'rb') as file:
            pickled_dict = pickle.load(file)
            num_articles += len(pickled_dict)
            print("# articles counted so far: {0}".format(num_articles))

    print("PubMed articles #: {0}".format(num_articles))
