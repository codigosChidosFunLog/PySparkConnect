import requests

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    """
        Usage: ejerciciospark2
    """

    spark = SparkSession \
        .builder \
        .appName("PySparkEjemplo2") \
        .getOrCreate()

    def getDataFromApi():
        url = "http://144.202.34.148:3333/obtenerData"
        response = requests.get(url)
        return response

    data = getDataFromApi()
    json_rdd = spark.sparkContext.parallelize([data.text])
    df = spark.read.json(json_rdd)
    result = df.select("luz").agg(F.min(df["luz"]), F.max(df["luz"]), F.avg(df["luz"]))
    print(result.show(truncate=False))

    spark.stop()
