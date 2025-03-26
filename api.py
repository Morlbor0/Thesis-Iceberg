from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from pyspark.sql import SparkSession
import traceback
import pandas as pd
import os
import json

# Initialize Flask App
app = Flask(__name__)
api = Api(app)

# Connect to Spark inside Docker
spark = SparkSession.builder \
    .appName("IcebergFlaskAPI") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "spark-warehouse/iceberg") \
    .getOrCreate()

class IcebergTable(Resource):

    # GET method to read an Iceberg table
    def get(self, table_name):
        try:
            print(f"Attempting to read Iceberg table: iceberg.employee_db.{table_name}")
            df = spark.read.table(f"iceberg.employee_db.{table_name}")
            data = df.toPandas().to_dict(orient="records")
            return jsonify(data)

        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
        
    # THIS WOULD ALWAYS WORK, EVEN AFTER SCHEMA CHANGES, SINCE TABLE NAME CAN'T BE CHANGED AND WE ARE SELECTING THE WHOLE TABLE

# Accessing the history of the Iceberg table (should work even after tables are changed)
class IcebergTableHistory(Resource):
    def get(self, table_name):
        try:
            df = spark.sql(f"SELECT * FROM iceberg.employee_db.{table_name}.history")
            data = df.toPandas().to_dict(orient="records")
            return jsonify(data)
        
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
    
    # THIS WORKS EVEN AFTER SCHEMA CHANGES, SINCE WE ARE SELECTING THE HISTORY THROUGH THE TABLE NAME, WHICH IS IMMUTABLE

class NewColumn(Resource):
    def post(self, table_name, column_name, column_type):
        try:
            spark.sql(f"ALTER TABLE iceberg.employee_db.{table_name} ADD COLUMN {column_name} {column_type}")
            print(f"Column '{column_name}' added to Iceberg table: {table_name}")
            return {"message": "Success"}, 200
        
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
    
    # THIS WILL ADD A NEW COLUMN TO THE TABLE, AND IT WILL ALWAYS WORK

class IcebergTableAge(Resource):
    def get(self, table_name):
        try:
            df = spark.read.table(f"iceberg.employee_db.{table_name}").select("age")
            data = df.toPandas().to_dict(orient="records")
            return jsonify(data)
        
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
        
    # THIS WAS A TEST FOR SPECIFIC COLUMNS, AND AS EXPECTED ONLY WORKS IF THERE IS A COLUMN CALLED 'AGE'

class DeleteColumn(Resource):    
    def delete(self, table_name, column_name):
        try:
            spark.sql(f"ALTER TABLE iceberg.employee_db.{table_name} DROP COLUMN {column_name}")
            print(f"Column '{column_name}' dropped from Iceberg table: {table_name}")
            return {"message": "Success"}, 200
        
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500

    # THIS WILL DELETE A COLUMN FROM THE TABLE, AND IT WILL ALWAYS WORK AS LONG AS THE COLUMN NAME IS CORRECT

class GetFromDate(Resource):
    def get(self, table_name, date):
        try:
            date = pd.to_datetime(date).strftime("%Y-%m-%d")
            df = spark.sql(f"SELECT * FROM iceberg.employee_db.{table_name} WHERE added_at >= '{date}'")
            data = df.toPandas().to_dict(orient="records")
            return jsonify(data)
        
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500

    # THIS WILL WORK LIKE THE OTHER METHODS THAT CALL THE TABLE BY REFERENCING A SPECIFIC COLUMN (IN THIS CASE 'ADDED_AT')
    # SO IT WILL WORK AS LONG AS THE 'ADDED_AT' COLUMN EXISTS

# Return a snapshot of the table at a specific date?
class GetSnapshot(Resource):
    def get(self, table_name, date):
        try: 
            date = pd.to_datetime(date).strftime("%Y-%m-%d")
            df = spark.sql(f"""
                SELECT * FROM iceberg.employee_db.{table_name}
                FOR SYSTEM_TIME AS OF '{date}'
                """)
            data = df.toPandas().to_dict(orient="records")
            return jsonify(data)
        
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
        
    # THIS WILL WORK EVERY TIME, AND IT WILL RETURN AN ERROR MESSAGE IF THERE IS NO SNAPSHOT FOR THE DATE PROVIDED 
    # OR IF THE DATE FORMAT IS INCORRECT

class GetColumn(Resource):
    def get(self, table_name, column):
        try:
            columns_set = set(spark.table(f"iceberg.employee_db.{table_name}").columns)

            if column in columns_set:
                query = spark.sql(f"SELECT `{column}` FROM iceberg.employee_db.{table_name}")
                data = query.toPandas().to_dict(orient="records")
                return jsonify(data)
            else:
                last_version_path = f"spark-warehouse/iceberg/employee_db/{table_name}/metadata/version-hint.text"
                with open(last_version_path, "r") as f:
                    last_version = f.read()
                metadata_path = f"spark-warehouse/iceberg/employee_db/{table_name}/metadata/v{last_version}.metadata.json"
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)
                
                column_name_versions = set()
                schema_id = 0
                # Instantiating the id of the column name we care about
                id_of_interest = None

                for schema in metadata.get("schemas", []):
                    schema_id = max(schema_id, schema['schema-id'])
                    for field in schema.get('fields', []):
                        column_name_versions.add(field['name'])
                        if field['name'] == column:
                        # Remembering the id of the column the user looked for, so that we can retrieve it later
                            id_of_interest = field['id']

                if column in column_name_versions:
                    last_schema_fields = metadata.get("schemas")[schema_id]['fields']
                    # We are looking for the name of the column that the user looked for in the LAST SCHEMA VERSION
                    last_schema_fields_of_interest = [field for field in last_schema_fields if field['id'] == id_of_interest]
                    query = spark.sql(f"SELECT `{last_schema_fields_of_interest[0]['name']}` FROM iceberg.employee_db.{table_name}")
                    data = query.toPandas().to_dict(orient="records")
                    # You can add a message to the user that the column name has changed with the following:
                    # {"message": f"Column '{column}' was found in a previous schema version, and is now called {last_schema_fields_of_interest[0]['name']}"}
                    return jsonify(data)
                else:
                    return {"error": f"Column '{column}' does not exist in the table and never has"}, 404

        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
        
class GetEmployeeById(Resource):
    def get(self, table_name, id):
        try:
            query = spark.sql(f"SELECT * FROM iceberg.employee_db.{table_name} WHERE Index = {id}")
            data = query.toPandas().to_dict(orient="records")
            return jsonify(data)
        
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
        
    # THIS ONLY WORKS IF THE COLUMN NAME IS 'Index' (STATIC)
    # IF WE INPUT AN ID THAT DOESN'T EXIST, IT WILL RETURN AN EMPTY LIST

class GetEmployeeByName(Resource):
    def get(self, table_name):
        try:
            employee_first_name = request.args.get("first_name")  
            if not employee_first_name:
                return {"error": "Missing 'first_name' parameter"}, 400

            column_index = 2
            last_version_path = f"spark-warehouse/iceberg/employee_db/{table_name}/metadata/version-hint.text"
            with open(last_version_path, "r") as f:
                last_version = f.read()
            metadata_path = f"spark-warehouse/iceberg/employee_db/{table_name}/metadata/v{last_version}.metadata.json"
            with open(metadata_path, "r") as f:
                metadata = json.load(f)

            column_info = metadata.get('schemas')[-1].get('fields')
            column_name = column_info[column_index - 1].get('name')

            query = spark.sql(f"SELECT * FROM iceberg.employee_db.{table_name} WHERE `{column_name}` = '{employee_first_name}'")
            data = query.toPandas().to_dict(orient="records")
            return jsonify(data)

        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500

    # THIS WORKS EVEN IF THE NAME OF THE 'FIRST NAME' COLUMN CHANGES
    # BUT THIS WILL ALWAYS RETRIEVE INFORMATION ABOUT THE COLUMN WHOSE INDEX IS 2, SO IF THE STRUCTURE OF THE TABLE COMPLETELY CHANGES, THIS WILL NOT WORK
    
    # IN SHORT, THIS WORKS WELL WITH 'RENAMES', BUT MAYBE NOT SO MUCH WHEN OTHER OPERATIONS TAKE PLACE
        

# Add API resource with dynamic table name
api.add_resource(GetColumn, "/<string:table_name>/<string:column>")
api.add_resource(GetEmployeeById, "/<string:table_name>/<int:id>")
api.add_resource(GetEmployeeByName, "/<string:table_name>/FirstName")

api.add_resource(IcebergTable, "/<string:table_name>")
api.add_resource(IcebergTableHistory, "/table_history/<string:table_name>")
api.add_resource(IcebergTableAge, "/table_age/<string:table_name>")
api.add_resource(NewColumn, "/<string:table_name>/add_column/<string:column_name>/type/<string:column_type>")
api.add_resource(DeleteColumn, "/<string:table_name>/delete_column/<string:column_name>")
api.add_resource(GetFromDate, "/<string:table_name>/from_date/<string:date>")
api.add_resource(GetSnapshot, "/<string:table_name>/snapshot/<string:date>")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)  # Allow external connections
