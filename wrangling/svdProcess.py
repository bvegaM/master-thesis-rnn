from pyspark.ml.feature import PCA
from pyspark.sql.functions import first, col, udf
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import StructType, StructField, DoubleType


def pivot_table_by_attribute(row, column, value, df_rating):
    return df_rating.groupBy(row).pivot(column).agg(first(value)).fillna(0)


def svd_process(df_pivot, row_pivot, key):
    df = df_pivot \
        .select(row_pivot, *[col(c) for c in df_pivot.columns if c not in [row_pivot]]) \
        .rdd \
        .map(lambda row: (row[0], Vectors.dense(row[1:]))) \
        .toDF([row_pivot, "features"])
    df_pivot.unpersist()

    # Crear un objeto PCA con k=10 (los 10 componentes principales)
    pca = PCA(k=10, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(df)

    # Transformar el DataFrame original para obtener las 10 componentes principales
    transformed = model.transform(df).select(row_pivot, 'pcaFeatures')

    # Define a list of column names for the new DataFrame
    component_cols = ['{p1}{p2}'.format(p1=key, p2=i + 1) for i in range(10)]

    # Definir la función UDF para convertir el vector en una estructura de registro
    to_tuple = udf(lambda v: tuple(v.toArray().tolist()),
                   StructType([StructField("_{}".format(i + 1), DoubleType()) for i in range(10)]))

    # Transformar el DataFrame original para obtener las 10 componentes principales
    transformed = model.transform(df).select(row_pivot, to_tuple('pcaFeatures').alias('components'))

    # Use select to extract each component as a separate column
    component_df = transformed.select(row_pivot, *[col('components._{}'.format(i + 1)).alias(component_cols[i]) for i in
                                                   range(10)])

    return component_df
