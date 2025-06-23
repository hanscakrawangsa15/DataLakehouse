"""
Script untuk menghapus semua data dari database AdventureworksDW.
Hanya menghapus isi data dari tabel-tabel, tidak menghapus schema atau struktur tabel.
"""

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import logging

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Konfigurasi database AdventureworksDW
DB_CONFIG = {
    'dbname': 'AdventureworksDW',  # Nama database AdventureworksDW
    'user': 'postgres',
    'password': 'chriscakra15',
    'host': 'localhost',
    'port': '5432',
    'schemas': ['dwh']  # Schema yang akan dibersihkan
}

def get_database_connection():
    """Membuat koneksi ke database"""
    try:
        conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        return create_engine(conn_str)
    except Exception as e:
        logger.error(f"Gagal terhubung ke database {DB_CONFIG['dbname']}: {e}")
        raise

def get_tables_in_schema(engine, schema):
    """Mendapatkan daftar tabel dalam schema tertentu"""
    try:
        inspector = inspect(engine)
        return inspector.get_table_names(schema=schema)
    except Exception as e:
        logger.error(f"Gagal mendapatkan daftar tabel dari schema {schema}: {e}")
        return []

def truncate_tables_in_schema(engine, schema):
    """Menghapus semua data dari tabel-tabel dalam schema tertentu"""
    try:
        tables = get_tables_in_schema(engine, schema)
        if not tables:
            logger.warning(f"Tidak ada tabel yang ditemukan di schema {schema}")
            return False
            
        with engine.connect() as conn:
            with conn.begin():
                # Nonaktifkan trigger sementara untuk menghindari constraint violation
                conn.execute(text('SET session_replication_role = replica;'))
                
                # Hapus data dari semua tabel
                for table in tables:
                    try:
                        # Gunakan TRUNCATE dengan RESTART IDENTITY CASCADE
                        sql = text(f'TRUNCATE TABLE {schema}."{table}" RESTART IDENTITY CASCADE')
                        conn.execute(sql)
                        logger.info(f"✅ Berhasil menghapus data dari tabel {schema}.\"{table}\"")
                    except Exception as e:
                        logger.error(f"❌ Gagal menghapus data dari tabel {schema}.\"{table}\": {e}")
                        continue
                
                # Aktifkan kembali trigger
                conn.execute(text('SET session_replication_role = DEFAULT;'))
        
        logger.info(f"✅ Semua operasi penghapusan data di schema {schema} selesai")
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"❌ Terjadi kesalahan saat menghapus data di schema {schema}: {e}")
        return False

def main():
    """Fungsi utama"""
    logger.info(f"Memulai proses penghapusan data dari database {DB_CONFIG['dbname']}...")
    
    try:
        # Buat koneksi ke database
        engine = get_database_connection()
        
        # Hapus data dari setiap schema yang ditentukan
        success = True
        for schema in DB_CONFIG['schemas']:
            logger.info(f"\nMemproses schema: {schema}")
            if not truncate_tables_in_schema(engine, schema):
                success = False
        
        if success:
            logger.info("\n✅ Semua data berhasil dihapus dari database")
        else:
            logger.warning("\n⚠️  Ada beberapa masalah saat menghapus data. Silakan periksa log di atas.")
    
    except Exception as e:
        logger.error(f"❌ Terjadi kesalahan yang tidak terduga: {e}")
    finally:
        logger.info("\nProses selesai")

if __name__ == "__main__":
    main()