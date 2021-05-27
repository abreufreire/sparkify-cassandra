#!/usr/bin/env python
# -*- coding: utf-8 -*-


# DROP TABLES

session_metadata_drop = "DROP TABLE IF EXISTS session_metadata"
user_metadata_drop = "DROP TABLE IF EXISTS user_metadata"
song_metadata_drop = "DROP TABLE IF EXISTS song_metadata"

# CREATE TABLES

session_metadata_create = """CREATE TABLE IF NOT EXISTS session_metadata (
    session_id INT,
    item_in_session INT,
    artist TEXT,
    song TEXT,
    length DECIMAL,
    PRIMARY KEY (session_id, item_in_session)
    )
"""

user_metadata_create = """CREATE TABLE IF NOT EXISTS user_metadata (
    user_id INT,
    session_id INT,
    item_in_session INT,
    artist TEXT,
    song TEXT,
    first_name TEXT,
    last_name TEXT,
    PRIMARY KEY (user_id, session_id, item_in_session)
    )
"""

song_metadata_create = """CREATE TABLE IF NOT EXISTS song_metadata (
    song TEXT,
    user_id INT,
    first_name TEXT,
    last_name TEXT,
    PRIMARY KEY (song, user_id)
    )
"""

# INSERT RECORDS

session_metadata_insert = """INSERT INTO session_metadata (session_id, item_in_session, artist, song, length) 
    VALUES (%s, %s, %s, %s, %s)
"""

user_metadata_insert = """INSERT INTO user_metadata (user_id, session_id, item_in_session, artist, song, first_name, last_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

song_metadata_insert = """INSERT INTO song_metadata (song, user_id, first_name, last_name)
    VALUES (%s, %s, %s, %s)
"""

# FIND RECORDS

query_select_1 = """ SELECT artist, song, length FROM session_metadata WHERE session_id = 338 AND item_in_session = 4 """

query_select_2 = """ SELECT artist, song, first_name, last_name, item_in_session FROM user_metadata WHERE user_id = 10 AND session_id = 182 """

query_select_3 = """ SELECT song, first_name, last_name FROM song_metadata WHERE song = 'All Hands Against His Own' """

# QUERY LISTS

create_table_queries = [session_metadata_create, user_metadata_create, song_metadata_create]
drop_table_queries = [session_metadata_drop, user_metadata_drop, song_metadata_drop]
