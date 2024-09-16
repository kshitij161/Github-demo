import azure.core.exceptions
from pyspark.sql.functions import *
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO
from pyspark.sql import SparkSession
spark = SparkSession.builder \
            .appName("Oracle to Spark") \
            .config("spark.jars", fr"D:\ojdbc8.jar") \
            .config("spark.master", "local[*]") \
            .getOrCreate()
class commonutils:
    def drop_null_columns(self,col,df):
        """ A function to drop columns in a dataframe that contain only null values

        :parameter
        col(list): List of the columns to check for null values
        d(dataframe): The input Dataframe to check a drop null columns from

        :returns
        Dataframe: A Dataframe with columns containing only null values dropped."""
        total_record_count = df.count()
        for column in col:
            null_df =  df.filter(df[f'{column}'].isNull())
            null_col_count = null_df.count()
            if total_record_count == null_col_count:
                df = df.drop(f'{column}')
        return df

    def connect_to_oracle(self, jdbc_path, userhost, username, password, table_name):
        """ Connects to the Oracle Database reads data from specific table
         :parameter
             jdbc_path(str): The path to the JDBC driver.
             user_name(str): The host where the oracle data is located.
             username(str): the username for accessing the Oracle database.
             query(str): The SQL query to execute

         :returns
            Dataframe:A Dataframe containing the data read from oracle table
            """
        spark = SparkSession.builder \
            .appName("Oracle to Spark") \
            .config("spark.jars", fr"{jdbc_path}") \
            .config("spark.master", "local[*]") \
            .getOrCreate()
        jdbcHostname = str(userhost)
        jdbcPort = 1521
        jdbcServiceName = "xe"
        jdbcUrl = f"jdbc:oracle:thin:@{jdbcHostname}:{jdbcPort}/{jdbcServiceName}"
        jdbcUsername = f"{username} as sysdba"
        jdbcPassword = str(password)
        jdbcDriver = "oracle.jdbc.OracleDriver"

        # Read data from Oracle
        oracle_df = spark.read.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("dbtable", table_name) \
            .option("user", jdbcUsername) \
            .option("password", jdbcPassword) \
            .option("driver", jdbcDriver) \
            .load()
        return oracle_df

    def upload_to_azure(self, dataframe, storage_account_name, access_key, container_name, file_name):
        """uploads a Dataframe to Azure Blob Storage
        :parameter
            dataframe : The Dataframe to be uploaded.
            storage_account_name: The name of the Azure storage account.
            access_key: the access key of the azure blob storage.
            container_name: the name of the container azure blob storage.
            file_name: the name of the file to br created in the container
        :returns
            none"""
        oracle_df_pandas = pd.DataFrame(dataframe.collect())
        df = pd.DataFrame(oracle_df_pandas)
        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  # Move the cursor to the beginning of the buffer
        # Create the connection string
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={access_key};EndpointSuffix=core.windows.net"
        # Create a BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # Get a ContainerClient object for the container
        try:
            container_client = blob_service_client.get_container_client(container_name)
        except azure.core.exceptions.HttpResponseError:
            print("the"
                  ". continer name contains invalid characters please use smallcase letter and don't use special character")
        # Check if the container exists
        if not container_client.exists():
            # Create the container if it doesn't exist
            container_client.create_container()
            print(f"Container {container_name} created.")
        # Create a BlobClient object for the file
        blob_client = container_client.get_blob_client(file_name)
        # Upload the CSV to the blob
        blob_client.upload_blob(csv_buffer.read(), overwrite=True)
        print(
            f'DataFrame uploaded to blob {file_name} in container {container_name}.{storage_account_name}.blob.core.windows.net/{file_name}", mode="overwrite"'
        )
    def spark_to_oracle(self,dataframe,new_table_name,userhost,username,password):
        """insert the table into the Oracle database
        :parameter
            Dataframe(Dataframe): The Spark Dataframe to ber written to Oracle
            new_table_name (str): The name of the new table to be created in Oracle
            userhost(str): The host of the Oracle database
            username (str): the username for connecting to the oracle database
            password(str): The password for the connecting to the oracle database

        :returns
            none
        """

        jdbcHostname = str(userhost)
        jdbcPort = 1521
        jdbcServiceName = "xe"
        jdbcUrl = f"jdbc:oracle:thin:@{jdbcHostname}:{jdbcPort}/{jdbcServiceName}"
        jdbcUsername = f"{username} as sysdba"
        jdbcPassword = str(password)
        jdbcDriver = "oracle.jdbc.OracleDriver"
        dataframe.write.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("dbtable", new_table_name) \
            .option("user", jdbcUsername) \
            .option("password", jdbcPassword) \
            .option("driver", jdbcDriver) \
            .save()
        print('success')

    def union_dataframes(self, dataframe_list):
        """
              A function to union a list of DataFrames by name while allowing missing columns.
              :param dataframe_list: List of DataFrames to be unioned.
              :return: A single DataFrame resulting from the union operation.
              """
        result = dataframe_list[0]
        for df in dataframe_list[1:]:
            result = result.unionByName(df,allowMissingColumns=True)
        return result





    def explode_column(self,df, column_name):
        """
        Function to
















         explode a column in the DataFrame.
        Parameters:
        df (DataFrame): The input DataFrame.
        column_name (str): The name of the column to explode.
                       Returns:
                           DataFrame: The DataFrame with the specified column exploded.
                       """
        return df.withColumn(column_name, explode(split(df[column_name], "[, or and]+")).alias(column_name))


    def split_column_alpha_number(self,dataframe,column):
        """
        Splits a column in a DataFrame into two new columns, one containing only numeric characters and the other containing only alphabetic characters.

        Parameters:
            dataframe (DataFrame): The input DataFrame.
            column (str): The name of the column to split.

        Returns:
            DataFrame: The DataFrame with two new columns: 'symbol_in_number' and 'symbol_in_alphabet'.
        """
        number_pattern = r"[^0-9]"
        alphabet_pattern = r"[^A-Za-z]"
        df = dataframe.withColumn('symbol_in_number',regexp_replace(col(column),pattern=number_pattern,replacement=''))\
                .withColumn('symbol_in_alphabet',regexp_replace(col(column),pattern=alphabet_pattern,replacement='')).drop(column)

        return df

    def set_amount(self, dataframe):
        """
               Sets the 'AMOUNT' column in the given dataframe by replacing specific strings with corresponding values.
               Args:
                   dataframe (pyspark.sql.DataFrame): The dataframe to modify.
               Returns:
                   pyspark.sql.DataFrame: The modified dataframe with the 'AMOUNT' column updated.
               """

        dataframe = dataframe.withColumn('AMOUNT', regexp_replace(col('AMOUNT'), '(?i)More than', '999999999999')) \
            .withColumn('AMOUNT', regexp_replace(col('AMOUNT'), '(?i)or Less', '0')) \
            .withColumn('AMOUNT', regexp_replace(col('AMOUNT'), '- ', '')) \
            .withColumn('AMOUNT', regexp_replace(col('AMOUNT'), ',', replacement=''))\
            .withColumn('AMOUNT', regexp_replace(col('AMOUNT'), '[$]', ''))
        return dataframe




    def seperate_array_min_max(self,dataframe,column):
        """
               Separates the maximum and minimum values from an array column in a DataFrame.
               Args:
                   dataframe (pyspark.sql.DataFrame): The input DataFrame.
                   column (str): The name of the column containing the array.
               Returns:
                   pyspark.sql.DataFrame: The DataFrame with two additional columns: 'max_amount' and 'min_amount'.
               """
        dataframe = dataframe.withColumn('max_amount',array_max(col(column)))\
            .withColumn('min_amount',array_min(col(column)))\
            .drop(column)

        return dataframe



    def extract_terr_code(self,dataframe,column_name,new_column_name):
        """
        Extracts the TERR code from a specified column in a dataframe and creates a new column with the extracted TERR code.

        Parameters:
            dataframe (pyspark.sql.DataFrame): The input dataframe.
            column_name (str): The name of the column from which to extract the TERR code.
            new_column_name (str): The name of the new column to be created with the extracted TERR code.

        Returns:
            pyspark.sql.DataFrame: The dataframe with the new column containing the extracted TERR code.
        """

        return dataframe.withColumn(new_column_name, regexp_replace(column_name, '.*#|[^0-9]$',''))


    def swap_column_names(self,df, col1, col2):
        """
        Swaps the names of two columns in a DataFrame.

        Parameters:
            df (pyspark.sql.DataFrame): The DataFrame containing the columns to be swapped.
            col1 (str): The name of the first column to be swapped.
            col2 (str): The name of the second column to be swapped.

        Returns:
            pyspark.sql.DataFrame: The DataFrame with the columns swapped.
        """
        df = df.withColumnRenamed(col1, "temp")
        df = df.withColumnRenamed(col2, col1)
        df=df.withColumnRenamed("temp", col2)

        return df


    def find_distinct_values(self,df,column_name):
        df = df.select(column_name)
        df = df.withColumn(column_name, regexp_replace(column_name, '.*#|[^0-9]$',''))

        list_of_values  = df.select(column_name).distinct().rdd.map(lambda x: x[0]).collect()
        return list_of_values




