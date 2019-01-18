#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType

LOG = logging.getLogger(__name__)


def price_range(brand):
    if brand in ['Samsung', 'Apple']:
        return 'High Price'
    elif brand == 'MI':
        return 'Mid Price'
    else:
        return 'Low Price'


def main():

    spark = SparkSession.builder.appName('Data Processing').getOrCreate()
    df = spark.read.csv('/tmp/sample_data.csv', inferSchema=True, header=True)
    brand_udf = udf(price_range, StringType())

    LOG.info("Columns: {0}".format(df.columns))
    LOG.info("Count: {0}".format(df.count()))

    # Check schema of dataframe.
    df.printSchema()

    # Read two columns using select method.
    df.select('age', 'mobile').show()

    # decribe for analyzing the dataframe.
    df.describe().show()

    # Add a new column.
    df.withColumn('age_after_10_yrs', (df['age'] + 10)).show(10, False)

    # Add a new column and change the datatype of column.
    df.withColumn('age_double', df['age'].cast(DoubleType())).show(10, False)

    # Filtering data
    df.filter(df['mobile'] == 'Vivo').select('age', 'ratings', 'mobile').show()

    # Filtering data
    df.filter(df['mobile'] == 'Vivo').filter(df['experience'] > 10).show()

    # Distinct values in Column
    df.select('mobile').distinct().show()

    # Count
    LOG.info("Distinct values count: {0}".format(
        df.select('mobile').distinct().count()))

    # Group by
    df.groupBy('mobile').count().show(10, False)

    # Order by
    df.groupBy('mobile').count().orderBy('count', ascending=False).show(10,
                                                                        False)

    # Mean, Sum and Min values
    df.groupBy('mobile').mean().show(10, False)
    df.groupBy('mobile').sum().show(10, False)
    df.groupBy('mobile').max().show(10, False)
    df.groupBy('mobile').min().show(10, False)

    # Aggregations
    df.groupBy('mobile').agg({'experience': 'sum'}).show(10, False)

    # UDF
    df.withColumn('price_range', brand_udf(df['mobile'])).show(10, False)

    # lambda udf
    age_udf = udf(lambda age: "young" if age <= 30 else "senior", StringType())
    df.withColumn("age_group", age_udf(df['age'])).show(10, False)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
