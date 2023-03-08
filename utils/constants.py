# CONSTANTS TO PROCESS DATA
MIN_VOTES = 10
N_GET_VOTES = 10
PIVOT_MAX_VALUES = 10000000
N_PRINCIPAL_COMPONENTS = 10

# TRAIN RATIO
TRAIN_RATIO = 0.8

# FILE PATHS
MIN_VOTES_FILE_PATH = '/Users/bryamdavidvegamoreno/Documents/recommender-system-spark/dataset/min_votes.parquet'
N_VOTES_FILE_PATH = '/Users/bryamdavidvegamoreno/Documents/recommender-system-spark/dataset/n_votes.parquet'
PIVOT_USER_FILE_PATH = '/Users/bryamdavidvegamoreno/Documents/recommender-system-spark/dataset/pivot_user.parquet'
PIVOT_MOVIE_FILE_PATH = '/Users/bryamdavidvegamoreno/Documents/recommender-system-spark/dataset/pivot_movie.parquet'
SVD_USER_FILE_PATH = '/Users/bryamdavidvegamoreno/Documents/recommender-system-spark/dataset/svd_user.parquet'
SVD_MOVIE_FILE_PATH = '/Users/bryamdavidvegamoreno/Documents/recommender-system-spark/dataset/svd_movie.parquet'
RATING_WRANGLING_FILE_PATH = '/Users/bryamdavidvegamoreno/Documents/recommender-system-spark/dataset/rating_wrangling.parquet'
RATING_GATHERING_FULL_FILE_PATH = ['../dataset/rating_partition_full/repartition-00.parquet',
                                   '../dataset/rating_partition_full/repartition-01.parquet',
                                   '../dataset/rating_partition_full/repartition-02.parquet',
                                   '../dataset/rating_partition_full/repartition-03.parquet',
                                   '../dataset/rating_partition_full/repartition-04.parquet',
                                   '../dataset/rating_partition_full/repartition-05.parquet']
