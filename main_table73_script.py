from pyspark.sql import  SparkSession
from CommonUtils import commonutils
from dotenv import  load_dotenv
import  os
from pyspark.sql.functions import col, split

if __name__ == '__main__':

    load_dotenv()
    spark = SparkSession.builder.master('local[*]').config('spark.jar',"D:\ojdbc8.jar").getOrCreate()
    # accessing the creadentials
    user_host  = os.getenv('USERHOST')
    user_name = os.getenv('user_name')
    password  = os.getenv('password')
    storage_account_name = os.getenv('ACCOUNT_NAME')
    access_key = os.getenv('ACCOUNT_KEY')
    #
    cmnutils = commonutils()
    table73_1  = cmnutils.connect_to_oracle(jdbc_path="D:\ojdbc8.jar",userhost=user_host,username=user_name,password=password,query="select * from table73 where table_number = '73.#1'")\

    table73_1 =cmnutils.explode_column(table73_1, column_name='SYMBOL')
    table73_1 = cmnutils.split_column_alpha_number(table73_1,'SYMBOL')
    table73_1 = cmnutils.set_amount(table73_1).withColumn('AMOUNT',split(col('AMOUNT'),' '))
    table73_1 = cmnutils.seperate_array_min_max(table73_1,'AMOUNT')\
                        .drop("AMOUNT","SYMBOL")
    table73_1.show()

    table73_2 =  cmnutils.connect_to_oracle(jdbc_path="D:\ojdbc8.jar",userhost=user_host,username=user_name,password=password,query="select * from table73 where table_number = '73.#2'")\

    table73_2 = cmnutils.explode_column(table73_2, column_name='SYMBOL')
    table73_2 = cmnutils.split_column_alpha_number(table73_2,"SYMBOL")
    table73_2 = cmnutils.set_amount(table73_2)\
                        .withColumn('AMOUNT',split(col('AMOUNT'),' '))
    table73_2 = cmnutils.seperate_array_min_max(table73_2,'AMOUNT')\
                        .drop("AMOUNT","SYMBOL")
    table73_2.show(1000)

    table73_3 = cmnutils.connect_to_oracle(jdbc_path="D:\ojdbc8.jar", userhost=user_host, username=user_name,
                                                password=password,query="select * from table73 where table_number = '73.#3'") \

    table73_3 = cmnutils.explode_column(table73_3,column_name='SYMBOL')
    table73_3 = cmnutils.split_column_alpha_number(table73_3,"SYMBOL")
    table73_3 = cmnutils.set_amount(table73_3)
    table73_3 = table73_3.withColumn('AMOUNT',split(col('AMOUNT'),' '))
    table73_3 = cmnutils.seperate_array_min_max(dataframe=table73_3,column='AMOUNT')
    table73_3 = table73_3.drop("AMOUNT","SYMBOL")
    table73_3.limit(10)



    df  = cmnutils.union_dataframes([table73_1,table73_2,table73_3])


    cmnutils.upload_to_azure(df,storage_account_name,access_key,container_name="table73",file_name="table73.csv")














