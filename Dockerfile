# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app.py .
COPY utils.py .

# Expose the Flask port (optional)
EXPOSE 8080

# Run the app using Gunicorn as the production WSGI server,
# binding to the port provided via the PORT environment variable.
CMD sh -c "gunicorn --bind 0.0.0.0:${PORT:-8080} --reload --workers 1 --threads 8 --timeout 0 app:app"