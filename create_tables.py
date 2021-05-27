#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cql_queries import create_table_queries, drop_table_queries


def create_keyspace():
    """
    set up cassandra: creates keyspace & set session
    :return:
    cluster
    session
    """
    # set up cluster & session/connection to cassandra
    try:
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect()
    except Exception as e:
        print(e)

    # create & set keyspace
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """)
    except Exception as e:
        print(e)

    session.set_keyspace('sparkify')

    return cluster, session


def drop_tables(session):
    """
    drop tables in cassandra
    :param: session
    :return:
    """
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

    print("\nTables dropped.")


def create_tables(session):
    """
    create tables in cassandra
    :param: session
    :return:
    """
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

    print("\nTables created.")


def main():

    cluster, session = create_keyspace()

    drop_tables(session)

    create_tables(session)

    session.shutdown()

    cluster.shutdown()


if __name__ == "__main__":
    main()
