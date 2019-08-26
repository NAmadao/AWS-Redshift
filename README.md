# SparkifyDB

## Summary

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The song data resides in JSON logs on user activity, as well as JSON metadata on the songs in their app.

Sparkify needs a better data model to analyse their data. This can be achieved with the STAR schema approach.

The STAR schema approach consists of 1 or more fact tables referencing any number of dimension tables. In this case I modelled the data as follows:

- Fact table: songplays table
- Dimension tables: User table, Song, table, Artist table, Time table.

The raw data is available in JSON files in the s3 buckets. There are 2 types of files:

- Event files
- Song files

We also have 2 tables in redshift created to load the raw data from the json files.

The task was as follows:

- Create redshift cluster and appropriate tables following above data model.
- Load raw data from s3 buckets into appropriate tables
- Clean data and extract required info and load into Fact and dimension tables using ETL

## List of files

1. *utility_functions.py* - contains functions needed to create & delete IAM role and redshift clusters and other Miscellaneous tasks.
2. *create_cluster.py* - script used to create IAM role and the redshift cluster according to specifications.
3. *sql_queries.py* - contains all the sql queries to delete tables, create tables and insert data into tables.
4. *create_tables.py* - python script to create the tables in the redshift cluster.
5. *etl.py* - script to load raw data from s3 buckets into staging tables and then perform ETL on them to load data into appropriate fact and dimension tables.
6. *delete_cluster.py* - script to delete the redshift cluster and IAM role.
7. *dwh_creation.cfg* - configuration file which contains AWS credentials and redshift cluster specifications to create the cluster and IAM role.
8. *dwh.cfg* - configuration file which is generated by create_cluster.py and contains all the redshift db and IAM role details to allow connection to the cluster.

## ETL process

1. Extract data from songs_data, transform and load into songs and artists tables.
2. Extract data from log_data, transform and load into users and time tables.
3. Compile data from all the tables to create the songplays table.

## Libraries required

- time - inbuilt python library. Used to make pause before rechecking server status.
- json - inbuilt python library. Used to handle json file format.
- boto3 - AWS SDK for python.
- configparser - inbuilt python library. Used to handle configuration file(.cfg) parsing.

Only boto3 needs to be installed and ca be done using *pip install* as follows:

```bash
pip install boto3
```

## Order of execution

1. Create your AWS account and create an IAM role with *programmatic access* and *administrator priviledges*. Save the credentials.

2. Copy the *Access key ID* and paste it as KEY in the *dwh_creation.cfg* file.

3. Copy the *Secret Access key* and paste it as SECRET in the *dwh_creation.cfg* file. Change db parameters if required under [DWH]

4. Run *create_cluster.py* 
    ```python
    create_cluster.py
    ```
5. Verify that dwh.cfg file has been generated

6. Run *create_tables.py*
    ```python
    create_tables.py
    ```
7. Run *etl.py*
    ```python
    etl.py
    ```
8. Use query editor in AWS redshift to query data loaded.

## Deletion

When you want to delete the cluster you can do so through the AWS console or run *delete_cluster.py*
    ```python
    delete_cluster.py
    ```

## Note

You can also create the cluster and IAM role through the AWS console. In this case you can skip steps 2-5. Here you would have to manually create the dwh.cfg configuration file. Use the given dwh.cfg file as template.