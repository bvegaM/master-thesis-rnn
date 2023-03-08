from pyspark import SparkConf
from pyspark.sql import SparkSession

from gathering.read import read_file, read_file_distributed, save_file, save_file_partition
from wrangling.clean import delete_users_by_min_votes, get_users_with_n_votes, merge_dataset_with_components
from wrangling.svdProcess import pivot_table_by_attribute, svd_process

from utils.constants import PIVOT_MAX_VALUES, MIN_VOTES_FILE_PATH, N_VOTES_FILE_PATH, PIVOT_USER_FILE_PATH, \
    SVD_USER_FILE_PATH, PIVOT_MOVIE_FILE_PATH, SVD_MOVIE_FILE_PATH, RATING_WRANGLING_FILE_PATH, \
    RATING_GATHERING_FULL_FILE_PATH

from models.model import split_train_test


def data_gathering_phase(spark_session: SparkSession):
    return read_file('../dataset/ratings.parquet', spark_session)


def data_gathering_phase_distributed(spark_sesion: SparkSession, list_path):
    return read_file_distributed(list_path, spark_sesion)


def data_wrangling_phase_distributed(spark_session: SparkSession, df_ratings):
    save_file(MIN_VOTES_FILE_PATH, delete_users_by_min_votes(spark_session, df_ratings))

    df_ratings_min_votes = read_file(MIN_VOTES_FILE_PATH, spark_session)
    save_file(N_VOTES_FILE_PATH, get_users_with_n_votes(spark_session, df_ratings_min_votes))
    df_ratings_min_votes.unpersist()

    df_ratings_clean = read_file(N_VOTES_FILE_PATH, spark_session)
    '''
    save_file_partition(PIVOT_USER_FILE_PATH,
                        pivot_table_by_attribute('userId', 'movieId', 'rating', df_ratings_clean))
    df_pivot_user = read_file(PIVOT_USER_FILE_PATH, spark_session)
    save_file_partition(SVD_USER_FILE_PATH, svd_process(df_pivot_user, 'userId', 'uC'))
    df_pivot_user.unpersist()

    save_file_partition(PIVOT_MOVIE_FILE_PATH,
                        pivot_table_by_attribute('movieId', 'userId', 'rating', df_ratings_clean))
    df_pivot_movie = read_file(PIVOT_MOVIE_FILE_PATH, spark_session)
    save_file_partition(SVD_MOVIE_FILE_PATH, svd_process(df_pivot_movie, 'movieId', 'mC'))
    df_pivot_user.unpersist()

    df_svd_user = read_file(SVD_USER_FILE_PATH, spark_session)
    df_svd_movie = read_file(SVD_MOVIE_FILE_PATH, spark_session)
    save_file(RATING_WRANGLING_FILE_PATH,
              merge_dataset_with_components(df_svd_user, df_svd_movie, df_ratings_clean, spark_session))
    df_svd_movie.unpersist()
    df_svd_user.unpersist()
    df_ratings_clean.unpersist()
    '''
    return df_ratings_clean


def deep_learning_phase(data_wrangling):
    (train_df, test_df) = split_train_test(data_wrangling)

    print("Número de filas en el conjunto de entrenamiento: ", train_df.count())
    print("Número de filas en el conjunto de prueba: ", test_df.count())


if __name__ == '__main__':
    conf = SparkConf().setAppName("Recommender System with Spark")
    conf.set('spark.master', 'local[10]')
    conf.set('spark.driver.memory', '20g')
    conf.set("spark.executor.memory", "12g")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.hadoop.fs.s3a.block.size", "268435456")
    conf.set("spark.sql.pivotMaxValues", PIVOT_MAX_VALUES)

    builder = SparkSession.builder
    builder.config(conf=conf)
    spark = builder.getOrCreate()

    parquet_path = ['../dataset/rating_partition_full/repartition-00.parquet',
                    '../dataset/rating_partition_full/repartition-01.parquet',
                    '../dataset/rating_partition_full/repartition-02.parquet',
                    '../dataset/rating_partition_full/repartition-03.parquet',
                    '../dataset/rating_partition_full/repartition-04.parquet',
                    '../dataset/rating_partition_full/repartition-05.parquet']

    df_rating = data_gathering_phase_distributed(spark, RATING_GATHERING_FULL_FILE_PATH)
    df_rating_wrangling = data_wrangling_phase_distributed(spark, df_rating)
    df_rating_wrangling.show()
    deep_learning_phase(data_wrangling=df_rating_wrangling)
