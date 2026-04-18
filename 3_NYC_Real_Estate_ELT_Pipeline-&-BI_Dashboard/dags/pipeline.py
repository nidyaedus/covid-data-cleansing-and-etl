import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv

# .env dosyasındaki şifreyi yükle
load_dotenv()

def download_nyc_data(url):
    su_an = datetime.now()
    tarih_etiketi = su_an.strftime("%Y_%m")
    dosya_adi = f"nyc_sales_{tarih_etiketi}.csv"
    
    print(f"{dosya_adi} indirme işlemi başlatılıyor...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status() 
        
        with open(dosya_adi, 'wb') as file:
            file.write(response.content)
            
        print("İndirme başarılı, dosya yerel diske kaydedildi!")
        return dosya_adi
        
    except requests.exceptions.RequestException as e:
        print(f"Kritik hata: {e}")
        raise

def load_to_staging(csv_dosya_adi):
    print(f"'{csv_dosya_adi}' veritabanına yükleniyor...")
    
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL bulunamadı! Lütfen .env dosyasını kontrol et.")
        
    engine = create_engine(db_url)
    df = pd.read_csv(csv_dosya_adi)
    
    # Veriyi Postgres'e basıyoruz
    df.to_sql('staging_sales', engine, if_exists='replace', index=False)
    
    print("Veri başarıyla 'staging_sales' tablosuna yüklendi!")
    return engine

def transform_and_model_data(engine):
    print("Veri modelleniyor ve temizleniyor (Yıldız Şema oluşturuluyor)...")
    
    # SQLAlchemy ile veritabanında kalıcı işlemler yapmak için bağlantı (connection) açıyoruz
    with engine.begin() as conn:
        
        # 1. TABLOLARI OLUŞTURMA (DDL)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_location (
                location_id SERIAL PRIMARY KEY,
                borough TEXT,
                zip_code TEXT,
                UNIQUE(borough, zip_code)
            );
            
            CREATE TABLE IF NOT EXISTS dim_property (
                property_id SERIAL PRIMARY KEY,
                address TEXT,
                building_class TEXT,
                UNIQUE(address, building_class)
            );
            
            CREATE TABLE IF NOT EXISTS fact_sales (
                sale_id SERIAL PRIMARY KEY,
                property_id INT REFERENCES dim_property(property_id),
                location_id INT REFERENCES dim_location(location_id),
                sale_date TEXT,
                sale_price NUMERIC
            );
        """))
        
        # 2. BOYUT (DIMENSION) TABLOLARINI DOLDURMA (Tekrarları önleyerek)
        # Sütun isimleri NYC verisindeki büyük harfli ve boşluklu orijinal isimler varsayılarak yazıldı.
        conn.execute(text("""
            INSERT INTO dim_location (borough, zip_code)
            SELECT DISTINCT "BOROUGH"::TEXT, "ZIP CODE"::TEXT
            FROM staging_sales
            WHERE "ZIP CODE" IS NOT NULL
            ON CONFLICT (borough, zip_code) DO NOTHING;
        """))
        
        conn.execute(text("""
            INSERT INTO dim_property (address, building_class)
            SELECT DISTINCT "ADDRESS"::TEXT, "BUILDING CLASS CATEGORY"::TEXT
            FROM staging_sales
            WHERE "ADDRESS" IS NOT NULL
            ON CONFLICT (address, building_class) DO NOTHING;
        """))
        
        # 3. GERÇEK (FACT) TABLOSUNU DOLDURMA VE TEMİZLEME (Cleanse)
        conn.execute(text("""
            INSERT INTO fact_sales (property_id, location_id, sale_date, sale_price)
            SELECT 
                p.property_id,
                l.location_id,
                s."SALE DATE"::TEXT,
                CAST(REPLACE(REPLACE(s."SALE PRICE"::TEXT, '$', ''), ',', '') AS NUMERIC) 
            FROM staging_sales s
            JOIN dim_property p ON s."ADDRESS"::TEXT = p.address AND s."BUILDING CLASS CATEGORY"::TEXT = p.building_class
            JOIN dim_location l ON s."BOROUGH"::TEXT = l.borough AND s."ZIP CODE"::TEXT = l.zip_code
            WHERE s."SALE PRICE" IS NOT NULL 
              AND CAST(REPLACE(REPLACE(s."SALE PRICE"::TEXT, '$', ''), ',', '') AS NUMERIC) > 100;
        """))
        
    print("Modelleme ve temizleme başarıyla tamamlandı.")

if __name__ == "__main__":
    DATA_URL = "https://data.cityofnewyork.us/api/views/w2pb-icbu/rows.csv?accessType=DOWNLOAD"
    
    # 1. Veriyi İndir
    inen_dosya_adi = download_nyc_data(DATA_URL)
    
    # 2. Staging'e Yükle (Bize motoru/engine geri döndürüyor)
    db_motoru = load_to_staging(inen_dosya_adi)
    
    # 3. Veriyi Temizle ve Modelle
    transform_and_model_data(db_motoru)