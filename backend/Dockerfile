# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project
COPY . /app/

# We will run the server via docker-compose command, so no CMD here is strictly necessary 
# if we define it there, but it's good practice.
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
