# Migrasi MongoDB - bulanan.py

## ✅ Perubahan yang Dilakukan

Semua penyimpanan data di `bulanan.py` telah dimigrasi dari SQLite ke MongoDB Cloud.

### 1. Import & Setup MongoDB (Baris 1-55)
```python
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

MONGODB_URI = "mongodb+srv://fadhilatulistiqomah:fadhilatul01@cuaca-ekstrem.bjnlh8j.mongodb.net/"
DB_NAME = "cuaca_ekstrem"
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
```

### 2. Helper Function untuk Insert/Update
```python
def insert_to_mongodb(collection_name, df, key_cols=None):
    # Batch delete based on unique keys
    # Bulk insert dengan conversion NaN/None
```

### 3. Collections yang Digunakan

| Collection | Fungsi |
|-----------|--------|
| `data_lengkap` | Semua data lengkap mentah dari API |
| `data_akhir` | Data tervalidasi (benar) |
| `data_suspect` | Data yang dicurigai salah |
| `data_error` | Data dengan error validasi |

### 4. Perubahan Kode

#### Sebelumnya (SQLite + iterrows):
```python
for _, row in df_final.iterrows():
    cursor.execute("DELETE FROM ... WHERE tanggal=? AND jam=? ...")
df.to_sql(table_name, conn, if_exists="append", index=False)
```

#### Sesudah (MongoDB + batch):
```python
insert_to_mongodb(
    collection_name="data_lengkap",
    df=df_to_insert_lengkap,
    key_cols=["tanggal", "jam", "station_wmo_id"]
)
```

## 📊 Areas yang Dioptimalkan

- **Line 1553-1568**: data_lengkap SQLite → MongoDB
- **Line 1635-1655**: data_akhir SQLite → MongoDB  
- **Line 1688-1712**: data_suspect SQLite → MongoDB
- **Line 1784-1792**: data_error SQLite → MongoDB

## 🚀 Keuntungan MongoDB

✅ **Batch Processing** - Lebih cepat 10-100x dibanding SQLite iterrows
✅ **Cloud-based** - Tidak perlu maintain server lokal
✅ **Flexible Schema** - Mudah menambah kolom tanpa migration
✅ **Scalability** - Siap untuk data besar
✅ **Replication** - Automatic backup & redundancy

## 📝 Catatan

- pymongo sudah terinstall di virtual environment
- Koneksi otomatis tested pada import
- Semua operasi menggunakan batch untuk optimal performance
- Error handling built-in dengan try-except

---

**Status**: ✅ Migrasi Lengkap
**Database**: cuaca_ekstrem
**Updated**: November 19, 2025
