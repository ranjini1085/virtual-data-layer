#!/usr/bin/python
import boto3


def retrieve_s3_file(bucket, filename, folder=None):
    '''retrieves file from S3

    keyword-args:
        bucket = name of S3 bucket from which to download file
        filename = name of S3 file to be downloaded
        folder = name of folder within S3, if any (optional)

    returns:
        name of downloaded file on local filesystem
'''
#   Create an S3 client
    s3 = boto3.client('s3')

#   set local and S3 filenames
    if folder is not None:
        local_filename = folder+'_'+filename
        s3_filename = folder+'/'+filename
    else:
        local_filename = filename
        s3_filename = filename

#   download file
    with open(local_filename, "wb") as s3_file:
        s3.download_fileobj(bucket, s3_filename, s3_file)

#   return name of local file
    return local_filename


def file_to_data_structure(local_filename):
    '''converts a local file to a data structure

    keyword_args:
        local_filename: name of the local CSV that should be converted to a
                        data file_to_data_structure
            assumes that the first column of this file is a header
                        with column column_names
            assumes that this file is comma delimited

    returns:
        column positions: a dictionary with a mapping between column names
                        and column_positions
        file_data: a list of rows from the local file, with each row
                        represented as a list


'''
    file_data = []
    column_positions = {}

    with open(local_filename) as lfile:
        column_names = \
            lfile.readline().replace('"', '').strip('\n\r').split(',')
        for i, v in enumerate(column_names):
            column_positions[v] = i
        for line in lfile:
            file_data.append(line.strip().split(','))

    return column_positions, file_data


if __name__ == '__main__':

    local_filename = retrieve_s3_file("virtual-data-layer", "orders.csv",
                                      "tcph")

    table_data = file_to_data_structure(local_filename)
    print(table_data[0])
    print(table_data[1][0])
