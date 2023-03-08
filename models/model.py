from pyspark.sql.functions import year

from utils.constants import TRAIN_RATIO


def split_train_test(data_wrangling):

    data_with_year = data_wrangling.withColumn("year", year('timeDate'))

    # Agrupar por año y contar la cantidad de registros en cada grupo
    counts_df = data_with_year.groupBy("year").count()

    # Ordenar por la cantidad de registros en orden descendente
    sorted_counts_df = counts_df.orderBy("count", ascending=False)

    # Obtener el total de registros en el dataframe
    total_count = sorted_counts_df.agg({"count": "sum"}).collect()[0][0]

    # Calcular el 80% del total
    eighty_percent = total_count * TRAIN_RATIO
    print(total_count, eighty_percent)
    # Iterar por los años y sumar los recuentos de registros hasta llegar al 80%
    cumulative_count = 0
    selected_years = []
    for row in sorted_counts_df.collect():
        years = row["year"]
        count = row["count"]
        cumulative_count += count
        print(years, count)
        selected_years.append(years)
        if cumulative_count >= eighty_percent:
            break

    train_df = data_wrangling.filter(year('timeDate').isin(selected_years))
    test_df = data_wrangling.filter(~year('timeDate').isin(selected_years))

    return train_df, test_df


def build_ALS_MODEL():
    pass