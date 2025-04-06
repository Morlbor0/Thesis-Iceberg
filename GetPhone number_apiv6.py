# This is the file with the new static API for the column Phone number
# Version: 6
# New API code:
class GetColumnName(Resource):
    def get(self, table_name):
        try:
            query = spark.sql(f"SELECT `Phone number` FROM iceberg.employee_db.employee")
            data = query.toPandas().to_dict(orient="records")
            return jsonify(data)

        except Exception as e:
            print("Error occurred:", e)
            traceback.print_exc()
            return {"error": str(e)}, 500