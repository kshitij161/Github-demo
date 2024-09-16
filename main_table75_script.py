from pyspark.sql import SparkSession
from CommonUtils import commonutils
import os
from dotenv import load_dotenv
from pyspark.sql.functions import *

if __name__ == '__main__':
    load_dotenv()
    spark = SparkSession.builder.master('local[*]')\
        .config('spark.jar', "D:\ojdbc8.jar")\
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true") \
        .getOrCreate()
    # accessing the creadentials
    user_host = os.getenv('USERHOST')
    user_name = os.getenv('user_name')
    password = os.getenv('password')
    storage_account_name = os.getenv('ACCOUNT_NAME')
    access_key = os.getenv('ACCOUNT_KEY')
    cmnutils = commonutils()

    table75 = cmnutils.connect_to_oracle(jdbc_path="D:\ojdbc8.jar", userhost=user_host, username=user_name,
                                         password=password,table_name='table75').drop('SYMBOL')
    list_of_table_numbers =cmnutils.find_distinct_values(table75,'table_number')

    for i in list_of_table_numbers:
        table_number = f'75.#'+i
        table75_1= cmnutils.connect_to_oracle(jdbc_path="D:\ojdbc8.jar", userhost=user_host, username=user_name,
                                            password=password,table_name='table75' ).filter(col('TABLE_NUMBER') ==table_number)
        table75_1 = cmnutils.explode_column(table75_1, column_name='TERR_CODE')
        table75_1 = cmnutils.split_column_alpha_number(table75_1, "TERR_CODE")
        table75_1 = cmnutils.extract_terr_code(table75_1,'TABLE_NUMBER','TERR_CODE')
        table75_1 = cmnutils.set_amount(table75_1).withColumn('AMOUNT', split(col('AMOUNT'), ' '))
        table75_1 = cmnutils.seperate_array_min_max(dataframe=table75_1, column='AMOUNT')
        table75_1 = cmnutils.swap_column_names(table75_1,'DEDUCTIBLE','DED_AMOUNT').drop('DED_AMOUNT')
        table75_1.show()
        cmnutils.upload_to_azure(table75_1, storage_account_name, access_key, f'table75', f'table{table_number}.csv')





