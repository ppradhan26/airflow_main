# Use the base Airflow image
FROM apache/airflow:2.10.3

# Install the FTP provider
RUN pip install apache-airflow-providers-ftp
