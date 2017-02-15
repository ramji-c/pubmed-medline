# author: Ramji Chandrasekaran
# date: 15-02-2017
# export processing results to file or DB. results cane be a pandas dataframe or any other supported datastructure

import pandas


def export_dataframe(filename, *dataframes, **fileparams):
    """helper function to export multiple pandas dataframes to a .xlsx or .csv file.
    Parameters:
        filename: name of the .xlsx or .csv file to be saved
        *dataframes: 1 or more pandas dataframe
        **fileparams: dictionary of params as follows
            format: output file format. default: xlsx
            sheetnames: list of 1 or more sheet name corresponding to each dataframe
            indices: list of 1 or more boolean values that indicate if index of corresponding dataframe should be
                     exported"""

    file_format = fileparams['format']

    if file_format == 'xlsx':
        writer = pandas.ExcelWriter(filename)
        for ind, dataframe in enumerate(dataframes):
            sheet_name = fileparams['sheet_names'][ind]
            keep_index = fileparams['indices'][ind]
            dataframe.to_excel(writer, sheet_name=sheet_name, engine='xlsxwriter', index=keep_index)
        writer.save()
        writer.close()

    elif file_format == 'csv':
        for ind, dataframe in enumerate(dataframes):
            keep_index = fileparams['indices'][ind]
            dataframe.to_csv(filename, index=keep_index)
