from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Görevimizin temel ayarları (Hata verirse 5 dakika bekle tekrar dene vs.)
default_args = {
    'owner': 'veri_muhendisi',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. DAG'ın (Otomasyonun) Tanımlanması
with DAG(
    'nyc_emlak_otomasyonu', # Airflow panelinde görünecek isim
    default_args=default_args,
    description='Her ayın 1inde NYC emlak verisini çeker ve temizler',
    schedule_interval='@monthly', # ZAMANLAMA: Her ay çalışır! (@daily, @weekly de yazılabilir)
    start_date=datetime(2026, 4, 1),
    catchup=False, # Geçmişteki çalışmayan günleri telafi etmeye çalışma
    tags=['emlak', 'nyc', 'elt'],
) as dag:

    # 3. Yapılacak İş (Task)
    # BashOperator kullanarak terminal komutu veriyormuşuz gibi Python dosyamızı çalıştırıyoruz.
    calistir_pipeline = BashOperator(
        task_id='python_kodunu_atesle',
        bash_command='python /opt/airflow/dags/pipeline.py',
    )