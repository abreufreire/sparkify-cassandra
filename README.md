# Data Modeling with Apache Cassandra
### Project overview
A fictional startup **Sparkify** in the music streaming industry looks to provide a great service to the community that 
enjoys its music streaming app. Sparkify holds a collection of event data (csv format) output from the activity of its users. 
As data engineers we have been asked to perform improvements in data querying in order to offer a more useful usage to 
the analytics staff.
For this job we have adopted Apache Cassandra as the NoSQL database management system, built a data model based on given questions to data, 
and built an ETL pipeline to extract-transform-load data from the local files (csv) to the NoSQL database.


![Logo](https://github.com/abreufreire/sparkify-cassandra/blob/master/images/logo.png)


### Questions to be addressed

#### <font color=green> 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4 </font>

#### <font color=blue> 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182 </font>

#### <font color=orange> 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own' </font>


### Database model
![Schema](https://github.com/abreufreire/sparkify-cassandra/blob/master/images/cassandra_tables.png)


### Project files
```
sparkify-postgres
|  .gitignore                   # Config file for Git
|  README.md                    # Repository description
|  sparkify_cassandra.ipynb     # Procedures (based on given queries) to process & verify data
|  cql_queries.py               # Queries to build Cassandra tables
|  create_tables.py             # Creates keyspace & tables (drops content if exists) in Cassandra
|  etl.py                       # Runs ETL pipeline to process data
|  requirements.txt             # Contains libraries needed to run scripts
|  event_datafile_new.csv       # Output from etl.py (compilation of data from original event data files)
|
└--images
  |  erd_sparkifydb.png         # Database model (Cassandra)
  |  logo.png                   # Sparkify logo
  |  cassandra_tables.png       # Screenshot of denormalized data (cols & rows) as a reference for the results
```


### How to run
Clone this project, and to up and running it **locally** you can use Docker - in its repository/hub there is an 
image with **Apache Cassandra**.

- Install Docker 
- Login Docker Hub
  ```
  $ docker login docker.io
  ```
- Pull image: cassandra
  ```
  $ docker pull cassandra
  ```
- Run container: cassandra-sparkify
  ```
  $ docker run --name cassandra-sparkify -p 127.0.0.1:9042:9042 -d cassandra
  ```
- On terminal run:
    ```
    
    $ python3 create_tables.py
    
    
    $ python3 etl.py
    
    ```
- Stop container (& Remove container):    
    ```    
    $ docker stop cassandra-sparkify
        
    $ docker rm cassandra-sparkify
    ```
