Big Data Pipeline (Spark + MovieLens 32M)

The program downloads a dataset of movie reviews
URL: https://files.grouplens.org/datasets/movielens/ml-32m.zip
On subsequent runs, if the data files are present, it does not download them again.
Files are downloaded and later unpacked in /data/source

We Builds medallion layers with Spark:
    bronze - pretty much raw data, here we read the .csv files, 
    ensure proper types and add an ingestion timestamp ("ingested_at")

    silver - joins movies and ratings, ensures propper range in ratings (0.5 to 5)
    and converts unix time to date time. 

    gold - aggregates. Groups silver data by
        rating_date
        movie_id
        min_rating
        max_rating
    
The parquet outputs are saved to data/medallion

The program requires Java 21+ (set JAVA_HOME if not auto-detected)

Architecture diagram:
docs/pipeline_architecture.mmd
