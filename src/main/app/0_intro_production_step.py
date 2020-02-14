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


if __name__ == '__main__':

    spark = init_spark("testing_environments")

    print(7, "es numero primo?", check_prime_num(7))
    print("FIN PROCESO \n")

    spark.stop()
