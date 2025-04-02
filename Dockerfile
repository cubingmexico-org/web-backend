# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app.py .

# Expose the Flask port
EXPOSE 5000

# Run the app using Gunicorn as the production WSGI server
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]