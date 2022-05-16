from typing import List, Tuple

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from main.utils.utils_demo import init_spark


def check_prime_num(number: int):
    if number > 1:
        for i in range(2, number):
            if (number % i) == 0:
                return False
        else:
            return True
    else:
        return print(input, "No es un numero primo")


def creating_data(data: list):
    store_schema = StructType([
        StructField('date', StringType(), True),
        StructField('time', StringType(), True),
        StructField('transaction', IntegerType(), True),
        StructField('item', StringType(), True)
    ])
    store_records_df = spark.createDataFrame(data, store_schema)

    return store_records_df.show()


if __name__ == '__main__':

    spark = init_spark("testing_environments")

    print("Loading first function:")
    number = 7
    print(number, "is a prime number?", check_prime_num(number))
    print("End of first function \n")

    print("Loading second function:")
    data = [('2020-02-20', '10:13:27', 2, 'Jeans'),
            ('2020-02-18', '11:45:43', 1, 'Sun_glasses')]
    print("Store clothes sold are:", creating_data(data))
    print("End of second function \n")

    spark.stop()
