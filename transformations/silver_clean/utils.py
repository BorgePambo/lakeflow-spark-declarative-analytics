from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, IntegerType
from pyspark.sql.window import Window


def clean_strings(df):

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):

            df = df.withColumn(
                field.name, F.trim(F.col(field.name))
            )

    return df


def cast_decimal(df, columns, precision=10, scale=2):

    for col_name in columns:

        df = df.withColumn(
            col_name,
            F.col(col_name).cast(DecimalType(precision, scale))
        )

    return df


def remove_duplicates(df, partition_cols, order_col):

    window_spec = Window.partitionBy(partition_cols)\
                        .orderBy(F.col(order_col).desc())

    return (
        df.withColumn("rn", F.row_number().over(window_spec))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )



def to_date(df, columns, format="yyyy-MM-dd"):

    for col_name in columns:

        df = df.withColumn(
            col_name,
            F.to_date(F.col(col_name), format)
        )

    return df
    


def cast_integer(df, columns):

    for col_name in columns:

        df = df.withColumn(
            col_name,
            F.col(col_name).cast(IntegerType())
        )

    return df



