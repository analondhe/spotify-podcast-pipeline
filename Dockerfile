FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Pre-install dbt packages so they're baked into the image
RUN cd dbt_project && dbt deps --profiles-dir .
