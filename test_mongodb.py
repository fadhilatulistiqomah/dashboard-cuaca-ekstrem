#!/usr/bin/env python3
"""
Script test untuk MongoDB connection dan operations
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import pandas as pd

MONGODB_URI = "mongodb+srv://fadhilatulistiqomah:fadhilatul01@cuaca-ekstrem.bjnlh8j.mongodb.net/"
DB_NAME = "cuaca_ekstrem"

def test_connection():
    """Test koneksi ke MongoDB"""
    print("=" * 60)
    print("🧪 Testing MongoDB Connection")
    print("=" * 60)
    
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ Koneksi berhasil ke MongoDB Atlas")
        
        db = client[DB_NAME]
        print(f"✅ Database '{DB_NAME}' terhubung")
        
        # List collections
        collections = db.list_collection_names()
        print(f"\n📊 Collections yang tersedia:")
        for col in collections:
            count = db[col].count_documents({})
            print(f"   - {col}: {count} dokumen")
        
        return client, db
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"❌ Gagal terhubung: {e}")
        return None, None

def test_insert():
    """Test insert data ke MongoDB"""
    print("\n" + "=" * 60)
    print("🧪 Testing Insert Operation")
    print("=" * 60)
    
    client, db = test_connection()
    if not client:
        return
    
    # Sample data
    sample_data = {
        "tanggal": "2025-11-19",
        "jam": "12:00",
        "station_wmo_id": "96001",
        "NAME": "Test Station",
        "LAT": -6.5,
        "LON": 106.8,
        "Temperatur": 28.5,
        "Curah_Hujan": 5.2
    }
    
    try:
        collection = db["test_insert"]
        
        # Delete if exists
        collection.delete_one({"tanggal": "2025-11-19", "station_wmo_id": "96001"})
        
        # Insert
        result = collection.insert_one(sample_data)
        print(f"✅ Insert berhasil dengan ID: {result.inserted_id}")
        
        # Verify
        found = collection.find_one({"station_wmo_id": "96001"})
        if found:
            print(f"✅ Verifikasi: Data ditemukan")
            print(f"   Station: {found.get('NAME')}")
            print(f"   Temperatur: {found.get('Temperatur')}°C")
        
        # Cleanup
        collection.delete_one({"_id": result.inserted_id})
        print(f"✅ Test data dibersihkan")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def test_batch_insert():
    """Test batch insert dengan pandas DataFrame"""
    print("\n" + "=" * 60)
    print("🧪 Testing Batch Insert (DataFrame)")
    print("=" * 60)
    
    client, db = test_connection()
    if not client:
        return
    
    # Sample DataFrame
    df = pd.DataFrame({
        "tanggal": ["2025-11-19", "2025-11-19"],
        "jam": ["12:00", "15:00"],
        "station_wmo_id": ["96001", "96002"],
        "NAME": ["Station 1", "Station 2"],
        "Temperatur": [28.5, 27.3]
    })
    
    try:
        collection = db["test_batch"]
        
        # Delete existing
        collection.delete_many({})
        
        # Convert DataFrame to list of dicts
        records = df.to_dict('records')
        
        # Insert
        result = collection.insert_many(records)
        print(f"✅ Inserted {len(result.inserted_ids)} dokumen")
        
        # Count
        count = collection.count_documents({})
        print(f"✅ Total dokumen dalam collection: {count}")
        
        # Find all
        docs = list(collection.find({}, {"_id": 0}))
        print(f"\n✅ Data yang tersimpan:")
        for doc in docs:
            print(f"   - {doc['NAME']} ({doc['jam']}): {doc['Temperatur']}°C")
        
        # Cleanup
        collection.delete_many({})
        print(f"\n✅ Test data dibersihkan")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def test_collections_stats():
    """Tampilkan statistik collections"""
    print("\n" + "=" * 60)
    print("📊 Collections Statistics")
    print("=" * 60)
    
    client, db = test_connection()
    if not client:
        return
    
    target_collections = ["data_lengkap", "data_akhir", "data_suspect", "data_error"]
    
    print(f"\n{'Collection':<20} {'Documents':<15} {'Size':<15}")
    print("-" * 50)
    
    for col_name in target_collections:
        if col_name in db.list_collection_names():
            collection = db[col_name]
            count = collection.count_documents({})
            
            # Get collection stats
            stats = db.command("collStats", col_name)
            size = stats.get('size', 0)
            size_mb = size / (1024 * 1024)
            
            print(f"{col_name:<20} {count:<15} {size_mb:.2f} MB")
        else:
            print(f"{col_name:<20} {'(empty)':<15} {'0 MB':<15}")

if __name__ == "__main__":
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 58 + "║")
    print("║" + "  🔧 MongoDB Connection & Operations Test Suite  ".center(58) + "║")
    print("║" + " " * 58 + "║")
    print("╚" + "=" * 58 + "╝")
    
    test_connection()
    test_insert()
    test_batch_insert()
    test_collections_stats()
    
    print("\n" + "=" * 60)
    print("✅ Testing Selesai!")
    print("=" * 60 + "\n")
