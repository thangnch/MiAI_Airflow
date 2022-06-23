from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


import datetime
import airflow

default_args = {
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
    "retry_delay": datetime.timedelta(hours=5),
}

task_name = 'file_sensor_task'

def email(**context):
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)

    out_csv_file_path = "/home/thangnc/tmp_in/guisep.txt"
    print(out_csv_file_path)

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
        sg = SendGridAPIClient("SG Token here")
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)

    except Exception as e:
        print(e.message)

    import os
    os.remove(out_csv_file_path)
    return True


dag = airflow.DAG("thangnc_dag04", default_args=default_args,
                  schedule_interval="@once",
                  tags=['thangnc'])


sensor_task = FileSensor(task_id=task_name, poke_interval=30, filepath="guisep.txt", fs_conn_id="my_consolidation_folder", dag = dag)

email_operator = PythonOperator(
    task_id='email_to_admin',
    provide_context=True,
    python_callable=email, dag = dag
)
#
# trigger = TriggerDagRunOperator(
#     task_id='trigger_dag_rerun', trigger_dag_id=task_name, dag=dag)



sensor_task >> email_operator