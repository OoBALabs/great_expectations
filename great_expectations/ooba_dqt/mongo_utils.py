from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

class MongoUtils:

    def __init__(self):
        pass

    
    def get_spark_session(self):        
        spark = SparkSession\
            .builder\
            .getOrCreate()
        return spark


    def flatten_structs(self, nested_df):
        stack = [((), nested_df)]
        columns = []
        while len(stack) > 0:
            parents, df = stack.pop()
            array_cols = [
                c[0]
                for c in df.dtypes
                if c[1][:5] == "array"
            ]
            flat_cols = [
                sf.col(".".join(parents + (c[0],))).alias("__".join(parents + (c[0],)))
                for c in df.dtypes
                if c[1][:6] != "struct"
            ]
            nested_cols = [
                c[0]
                for c in df.dtypes
                if c[1][:6] == "struct"
            ]
            columns.extend(flat_cols)
            for nested_col in nested_cols:
                projected_df = df.select(nested_col + ".*")
                stack.append((parents + (nested_col,), projected_df))
        return nested_df.select(columns)


    def flatten_nested_spark_df(self, df):
        array_cols = [
                c[0]
                for c in df.dtypes
                if c[1][:5] == "array"
            ]
        while len(array_cols) > 0:
            for array_col in array_cols:
                cols_to_select = [x for x in df.columns if x != array_col ]
                df = df.withColumn(array_col, sf.explode(sf.col(array_col)))
            df = self.flatten_structs(df)
            array_cols = [
                c[0]
                for c in df.dtypes
                if c[1][:5] == "array"
            ]
        return df
    

if __name__ == "__main__":
    mongo_utils = MongoUtils()
    spark = mongo_utils.get_spark_session()
    sample_json = """
        {
            "name": "user",
            "Age": {
                "$numberInt": "29"
            },
            "Gender": "M",
            "Account": [
                {
                    "account_no": "1A2B3C4D5E",
                    "phone_no": [
                        {
                            "type": "mobile",
                            "value": [8740640610,123]
                        },
                        {
                            "type": "fax",
                            "value": [8740640610,678]
                        }
                    ]
                },
                {
                    "account_no": "1A2B3C4D5F",
                    "phone_no": [
                        {
                            "type": "fax",
                            "value": 5740640610
                        },
                        {
                            "type": "mobile",
                            "value": 6740640610
                        }
                    ]
                }
            ],
            "Phone": {
                "type": "mobile",
                "value": 99999999
            }
        }
    """
    sc = spark.sparkContext
    df_spark = spark.read.json(sc.parallelize([sample_json]))
    df_spark.show()
    flat_df = mongo_utils.flatten_nested_spark_df(df_spark)
    flat_df.show()