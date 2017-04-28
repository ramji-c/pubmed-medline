# author: Ramji Chandrasekaran
# date: 22-Mar-2017

import argparse
import pickle
import numpy


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("input")
    arg_parser.add_argument("output")
    args = arg_parser.parse_args()

    vectorized_data = {}
    vectors = []
    with open(args.input, 'rb') as input_file:
        vectorized_data = pickle.load(input_file)
        vectors = vectorized_data['data']
    with open(args.output, 'a') as output_file:
        for vector in vectors:
            print(vector)
            output_file.write("\t".join(vector))
            output_file.write("\n")
