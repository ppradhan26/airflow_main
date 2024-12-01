from airflow import DAG
from datetime import datetime
from airflow.providers.ftp.sensors.ftp import FTPSensor
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator

# Define the DAG
with DAG(
    dag_id="ftp_file_check_download_email",
    start_date=datetime(2023, 12, 1),  # Set an appropriate start date
    schedule_interval="@daily",  # Adjust schedule as needed
    catchup=False,  # Prevent backfilling past runs
) as dag:

    # Task 1: Check if the file exists on the FTP server
    check_file = FTPSensor(
        task_id="check_ftp_file",
        ftp_conn_id="nielsen_ftp_main",  # Your Airflow FTP connection ID
        path="/EDI II Mediaplus/2018/EDI II Mediaplus.zip",  # File path on the FTP server
        poke_interval=60,  # Time in seconds between checks
        timeout=300,  # Maximum wait time in seconds
    )

    # Task 2: Download the file if it exists
    download_file = FTPFileTransmitOperator(
        task_id="download_file",
        ftp_conn_id="nielsen_ftp_main",
        remote_filepath="/EDI II Mediaplus/2018/EDI II Mediaplus.zip",  # File path on the FTP server
        local_filepath="/Users/pradhanp/Documents/nielson_raw_files/2018/EDI II Mediaplus.zip",  # Where to save the file locally
        operation="get",  # 'get' for download
        create_intermediate_dirs=True
    )

    # Task 3: Send an email to notify the file has been downloaded
    send_email = EmailOperator(
        task_id="send_email_notification",
        to="p.pradhan@house-of-communication.com",
        subject="FTP File Downloaded",
        html_content="<p>The file <b>file.csv</b> has been downloaded successfully from the FTP server.</p>",
        files=["/path/to/local/file.csv"],  # Attach the downloaded file
    )

    # Dummy task: Do nothing if file doesn't exist
    no_op = DummyOperator(task_id="no_op")

    # Define dependencies
    check_file >> [download_file, no_op]
    download_file >> send_email
