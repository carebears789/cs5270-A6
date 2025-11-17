# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set a working directory inside the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Command to run your program
CMD ["python", "consumer.py", "-rq", "https://sqs.us-east-1.amazonaws.com/637423576340/cs5270-requests", "-swb", "usu-cs5270-scuba-requests"]
