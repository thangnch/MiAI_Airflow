from datetime import datetime, timedelta

import csv
import sys
from airflow import DAG
import pandas as pd

from airflow.operators.python import PythonOperator
import pymysql

def extract():
    db_opts = {
        'user': 'root',
        'password': 'tt1234',
        'host': '192.168.56.1',
        'database': 'it_face_db',
        'port': 3306
    }

    db = pymysql.connect(**db_opts)
    cur = db.cursor()

    sql = 'SELECT * from it_face_db.tbldummy'
    csv_file_path = '/home/thangnc/tmp/my_csv_file.csv'

    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        db.close()

    # Continue only if there are rows returned.
    if rows:
        # New empty list called 'result'. This will be written to a file.
        result = list()

        # The row name is the first entry for each entity in the description tuple.
        column_names = list()
        for i in cur.description:
            column_names.append(i[0])

        result.append(column_names)
        for row in rows:
            result.append(row)

        # Write result to file.
        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in result:
                csvwriter.writerow(row)
    else:
        sys.exit("No rows found for query: {}".format(sql))

    return True


def transform():
    in_csv_file_path = '/home/thangnc/tmp/my_csv_file.csv'
    out_csv_file_path = '/home/thangnc/tmp/my_csv_file_square.csv'

    df = pd.read_csv(in_csv_file_path)
    df["amount_square"] = df["amount"] * 2

    df.to_csv(out_csv_file_path, index=False)
    return True


def email():
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
    out_csv_file_path = '/home/thangnc/tmp/my_csv_file_square.csv'
    import base64
    message = Mail(
        from_email='ainoodle.tech@gmail.com',
        to_emails='thangnch@gmail.com;thang.nc@shb.com.vn',
        subject='Your file is here!',
        html_content='<img src="https://www.shb.com.vn/wp-content/uploads/2016/03/Logo-SHB-VN.png"> Dear Customer,<br>Welcome to SHB. Your file is in attachment<br>Thank you!'
    )

    with open(out_csv_file_path, 'rb') as f:
        data = f.read()
        f.close()
    encoded_file = base64.b64encode(data).decode()

    attachedFile = Attachment(
        FileContent(encoded_file),
        FileName('data.csv'),
        FileType('text/csv'),
        Disposition('attachment')
    )
    message.attachment = attachedFile


    try:
        sg = SendGridAPIClient("SG.Send Grid Token here.")
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
        print(datetime.now())
    except Exception as e:
        print(e.message)

    return True

dag = DAG(
    'thangnc_dag02',
    default_args={
        'email': ['thangnch@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A complex DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 6, 1),
    tags=['thangnc'])


extract_operator = PythonOperator(
    task_id='load_from_mysql',
    python_callable=extract,
    dag=dag
)

transform_operator = PythonOperator(
    task_id='caculate_square_amount',
    python_callable=transform,
    dag=dag
)

email_operator = PythonOperator(
    task_id='email_to_admin',
    python_callable=email,
    dag=dag
)

extract_operator >> transform_operator >> email_operator