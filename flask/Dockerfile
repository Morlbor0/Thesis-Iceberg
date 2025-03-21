# Use an official Python image as the base
FROM python:3.9

# Set the working directory in the container
WORKDIR /pyspark_test_BAKUP/

# Copy the Flask application code into the container
COPY . .

# Install dependencies
RUN pip install flask flask-restful pyspark

# Expose Flask's port
EXPOSE 5001

# Run the Flask application
CMD ["python", "api.py"]