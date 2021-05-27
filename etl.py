#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from cassandra.cluster import Cluster
import os
import glob
import csv
from cql_queries import *

"""
extract data from raw files & create data file (csv) that will be used to feed tables in Apache Cassandra
"""

def extract_records(filepath):
    """
    get all rows from each csv file (event_data dir)
    :param: filepath
    :return: full_data_rows_list
    """
    # list of paths to files in dir
    file_path_list = glob.glob(os.path.join(filepath, '*'))

    # build a list of records from all files/csvs
    full_data_rows_list = []
    for f in file_path_list:
        # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            # skip header
            next(csvreader)

            for line in csvreader:
                # print(line)
                full_data_rows_list.append(line)

    print('Total rows: ', len(full_data_rows_list))

    return full_data_rows_list


def insert_records(full_data_rows_list):
    """
    create & write csv file with valid song data (artist/nextsong related records)
    :param full_data_rows_list:
    :return:
    """
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'first_name', 'gender', 'item_in_session', 'last_name', 'length', 'level', 'location',
                         'session_id', 'song', 'user_id'])

        for row in full_data_rows_list:
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    # number of rows in csv
    with open('event_datafile_new.csv', 'r', encoding='utf8') as f:
        print('Total valid rows: ', sum(1 for line in f))


def load_data(session):
    """
    load data from csv into cassandra tables (using insert queries in cql_queries.py)
    :param session:
    :return:
    """
    csv_file = 'event_datafile_new.csv'
    try:
        df = pd.read_csv(csv_file, encoding='utf8')
    except Exception as e:
        print(e)

    try:
        insert_data(session, session_metadata_insert, df, ['session_id', 'item_in_session', 'artist', 'song', 'length'])
    except Exception as e:
        print(e)

    try:
        insert_data(session, user_metadata_insert, df, ['user_id', 'session_id', 'item_in_session', 'artist', 'song', 'first_name', 'last_name'])
    except Exception as e:
        print(e)

    try:
        insert_data(session, song_metadata_insert, df, ['song', 'user_id', 'first_name', 'last_name'])
    except Exception as e:
        print(e)


def insert_data(session, insert_query, df, df_cols):
    """
    load data from dataframe into cassandra tables (using insert queries in cql_queries.py)
    :param session:
    :param insert_query:
    :param df:
    :param df_cols:
    :return:
    """
    for i, row in df.iterrows():
        data = tuple([row[col] for col in df_cols])
        # print(data)
        session.execute(insert_query, data)


def main():

    # path to dir with raw data (csv files) given
    filepath = os.getcwd() + '/event_data'


    # set up cluster & session/connection to cassandra
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    session.set_keyspace('sparkify')


    # PROCESS DATA
    full_data_rows_list = extract_records(filepath)

    insert_records(full_data_rows_list)

    load_data(session)


    # close session/connection
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
