from utils.constants import MIN_VOTES, N_GET_VOTES


def delete_index_column(spark, df_rating):
    df_rating.createOrReplaceTempView("ratings")
    df_ratings_clean = spark.sql("SELECT userId,movieId,rating,timestamp FROM ratings")
    return df_ratings_clean


def get_avg_by_user(spark, df_rating):
    pass


def get_avg_by_movie(spark, df_rating):
    pass


def get_stvd_by_user(spark, df_rating):
    pass


def get_stvd_by_movie(spark, df_rating):
    pass


def delete_users_by_min_votes(spark, df_rating):
    df_rating.createOrReplaceTempView("ratings")
    df_ratings_clean = spark.sql('''SELECT * FROM ratings WHERE userId in (SELECT userId FROM ratings GROUP BY userId 
    HAVING COUNT(userId) >= {p1})'''.format(p1=MIN_VOTES))
    return df_ratings_clean


def get_users_with_n_votes(spark, df_rating):
    df_rating.createOrReplaceTempView("ratings")
    df_ratings_clean = spark.sql('''SELECT userId, movieId, rating, timestamp as timeDate, 
                                            unix_timestamp(timestamp,'yyyy-MM-dd') timeSeconds
                                    FROM (
                                      SELECT userId, movieId, rating,timestamp,
                                        ROW_NUMBER() OVER (PARTITION BY userId ORDER BY RAND()) AS num
                                      FROM ratings
                                    ) t
                                    WHERE num <= {p1}
                                    ORDER BY userId,timestamp;
                                    '''.format(p1=N_GET_VOTES))
    return df_ratings_clean


def merge_dataset_with_components(df_user_svd, df_movie_svd, df_rating,spark):
    df_rating.createOrReplaceTempView("ratings")
    df_user_svd.createOrReplaceTempView("svd_users")
    df_movie_svd.createOrReplaceTempView("svd_movies")

    df_rating_wrangling = spark.sql('''
    SELECT u.*,m.*,rating,timeDate,timeSeconds
    FROM ratings r
    INNER JOIN svd_users u ON u.userId = r.userId
    INNER JOIN svd_movies m ON m.movieId = r.movieId
    ''')

    return df_rating_wrangling.drop('userId', 'movieId')
