"""
Script untuk menghapus data dari tabel-tabel staging di database.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Konfigurasi database
DB_CONFIG = {
    'dbname': 'staging',
    'user': 'postgres',
    'password': 'chriscakra15',
    'host': 'localhost',
    'port': '5432'
}

def get_database_connection():
    """Membuat koneksi ke database"""
    try:
        conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        return create_engine(conn_str)
    except Exception as e:
        logger.error(f"Gagal terhubung ke database: {e}")
        raise

def truncate_tables():
    """Menghapus semua data dari tabel-tabel staging"""
    tables = [
        'staging_external_sentiment',
        'staging_market_share_report',
        'staging_warehouse_temp_sensor'
    ]
    
    engine = get_database_connection()
    
    try:
        with engine.connect() as conn:
            # Mulai transaksi
            with conn.begin():
                for table in tables:
                    try:
                        # Gunakan TRUNCATE dengan RESTART IDENTITY untuk mereset serial/identity columns
                        sql = text(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE')
                        conn.execute(sql)
                        logger.info(f"Berhasil menghapus data dari tabel {table}")
                    except Exception as e:
                        logger.error(f"Gagal menghapus data dari tabel {table}: {e}")
                        # Lanjut ke tabel berikutnya meskipun ada error
                        continue
            
            logger.info("Semua operasi penghapusan data selesai")
            return True
            
    except SQLAlchemyError as e:
        logger.error(f"Terjadi kesalahan saat menghapus data: {e}")
        return False
    except Exception as e:
        logger.error(f"Terjadi kesalahan yang tidak terduga: {e}")
        return False

def main():
    """Fungsi utama"""
    logger.info("Memulai proses penghapusan data dari tabel staging...")
    
    if truncate_tables():
        logger.info("✅ Semua data berhasil dihapus dari tabel-tabel staging")
    else:
        logger.error("❌ Terjadi kesalahan saat menghapus data")

if __name__ == "__main__":
    main()