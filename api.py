from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from pyspark.sql import SparkSession
import traceback
import pandas as pd
import os
import json
from openai import OpenAI

client = OpenAI(
    api_key="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)

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

def ai_rewrite_api(table_name, column, new_column):

    prompt = f"""The name of the column '{column}' has just been changed to '{new_column}'.
            Please rewrite the API code to reflect this change.
            The structure you are to use is the following:
            'class GetColumnName(Resource):
                def get(self, table_name):
                    try:
                        query = spark.sql(f"SELECT `Column name` FROM iceberg.employee_db.{table_name}")
                        data = query.toPandas().to_dict(orient="records")
                        return jsonify(data)
                    
                    except Exception as e:
                            print("Error occurred:", e)
                            traceback.print_exc()
                            return {{"error": str(e)}}, 500'. 
            Please do not include any other information, just the code, and pay attention to indentation
            since this code will be directly copied into another API file.
            Do not include '''python at the beginning of the code, and do not include ''' at the end of the code,
            since this will be actual code written in a .py file.
    """

    response = client.responses.create(
        model="gpt-4o",
        input=prompt
    )
    return response.output_text

def create_new_api_file(table_name, column_name, new_column_name):

    # Get the current directory
    current_directory = "/Users/francescogalli/Desktop/Iceberg_Thesis_Work"
    table_name = "employee"

    # Find the latest version number (remember we are calling this function only AFTER a column name is changed)
    last_version_path = f"spark-warehouse/iceberg/employee_db/{table_name}/metadata/version-hint.text"
    with open(last_version_path, "r") as f:
        last_version = f.read()

    new_file_name = f"Get{new_column_name}_apiv{last_version}.py"

    # Create the new file in the curernt_directory
    new_file_path = os.path.join(current_directory, new_file_name)
    with open(new_file_path, "w") as file:
        file.write(f"# This is the file with the new static API for the column {new_column_name}\n")
        file.write(f"# Version: {last_version}\n")
        file.write(f"# New API code:\n")
        file.write(ai_rewrite_api(table_name, column_name, new_column_name))
        


    print(f"Created new file: {new_file_name}")


class ChangeColumnName(Resource):
    def patch(self, table_name, column, new_column):
        try:
            query = spark.sql(f"ALTER TABLE iceberg.employee_db.{table_name} RENAME COLUMN `{column}` TO `{new_column}`")
            data = query.toPandas().to_dict(orient="records")
            create_new_api_file(table_name, column, new_column)
            return jsonify(data)
        
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500

class GetPhoneColumn(Resource):
    def get(self, table_name):
        try:
            query = spark.sql(f"SELECT `Phone number` FROM iceberg.employee_db.{table_name}")
            data = query.toPandas().to_dict(orient="records")
            return jsonify(data)
        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
        
        # THIS WORKS ONLY IF THE COLUMN NAME IS 'Phone number' (STATIC)


# Function to find the semantically closest column name using OpenAI API
def find_closest_column(table_name, column):

    current_schema_columns = set(spark.table(f"iceberg.employee_db.{table_name}").columns)

    #prompt = f"The column '{column}' does not exist. Based on these columns {latest_columns}, which one is the closest match in meaning?"
    prompt = f"The column '{column}' does not exist. Based on these columns: {', '.join(current_schema_columns)}, \
                is there a column that is very close in meaning? \
                If there is, please return the name of the column and nothing else. \
                If there is no such column, please return 'NO MATCH'. \
                If there are multiple columns that are close in meaning, please return 'AMBIGUOUS'."

    response = client.responses.create(
        model="gpt-4o",
        input=prompt
    )

    return response.output_text

class GetColumnAI(Resource):
    def get(self, table_name, column):
        try:
            current_schema_columns = set(spark.table(f"iceberg.employee_db.{table_name}").columns)

            if column in current_schema_columns:
                query = spark.sql(f"SELECT `{column}` FROM iceberg.employee_db.{table_name}")
                data = query.toPandas().to_dict(orient="records")
                return jsonify(data)
            else:
                result = find_closest_column(table_name, column)

                if result == "NO MATCH":
                    return {"error": f"Column '{column}' does not exist in the table and never has"}, 404
                elif result == "AMBIGUOUS":
                    return {"error": f"Column '{column}' is ambiguous, please specify which one you mean"}, 400
                else:
                    query = spark.sql(f"SELECT `{result}` FROM iceberg.employee_db.{table_name}")
                    data = query.toPandas().to_dict(orient="records")
                    return jsonify(data)


        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500
        
    # Using this method, we will return the semantically closest column name even if the column name was changed 

# Add API resource with dynamic table name
api.add_resource(GetColumn, "/<string:table_name>/<string:column>")
api.add_resource(GetEmployeeById, "/<string:table_name>/<int:id>")
api.add_resource(GetEmployeeByName, "/<string:table_name>/FirstName")
api.add_resource(GetPhoneColumn, "/<string:table_name>/PhoneNumber")

api.add_resource(GetColumnAI, "/<string:table_name>/ai/<string:column>")

api.add_resource(ChangeColumnName, "/<string:table_name>/rename_column/<string:column>/<string:new_column>")



# API RESOURCES FOR TESTING
api.add_resource(IcebergTable, "/<string:table_name>")
api.add_resource(IcebergTableHistory, "/table_history/<string:table_name>")
api.add_resource(IcebergTableAge, "/table_age/<string:table_name>")
api.add_resource(NewColumn, "/<string:table_name>/add_column/<string:column_name>/type/<string:column_type>")
api.add_resource(DeleteColumn, "/<string:table_name>/delete_column/<string:column_name>")
api.add_resource(GetFromDate, "/<string:table_name>/from_date/<string:date>")
api.add_resource(GetSnapshot, "/<string:table_name>/snapshot/<string:date>")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)  # Allow external connections
