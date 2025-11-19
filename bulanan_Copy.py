import requests
import urllib3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from datetime import date, datetime, timedelta
import sqlite3
import calendar # Ditambahkan untuk proses looping bulanan
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

# Hilangkan warning SSL (karena verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =======================================
# 3️⃣ Konfigurasi login & periode data
# =======================================
USERNAME = "pusmetbang"      # ganti dengan username BMKG Satu kamu
PASSWORD = "oprpusmetbang"   # ganti dengan password BMKG Satu kamu

# Tentukan TAHUN dan BULAN yang akan diproses
TAHUN = 2025
BULAN = 10

# =======================================
# 4️⃣ Fungsi untuk ambil token
# =======================================
def ambil_token(username, password):
    url_login = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu/@login"
    payload = {"username": username, "password": password}
    response = requests.post(url_login, json=payload, verify=False)

    if response.status_code == 200:
        data = response.json()
        print("Respon Login:", data)  # debug isi respon
        token = data.get("token") or data.get("access_token")
        if token:
            print("✅ Token berhasil diambil")
            return token
        else:
            raise ValueError("❌ Token tidak ditemukan di response")
    else:
        raise ValueError(f"❌ Gagal login. Status code: {response.status_code}")

# =======================================
# 5️⃣ Fungsi untuk ambil data GTS (01 - 00 esok hari)
# =======================================
def ambil_data_gts(tanggal, token):
    tgl_akhir = datetime.strptime(tanggal, "%Y-%m-%d")
    tgl_awal = tgl_akhir - timedelta(days=1)
    
    # URL dan parameter disesuaikan dengan skrip Anda
    url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu//@search"
    params = {
        "type_name": "GTSMessage",
        "_metadata": "type_message,timestamp_data,timestamp_sent_data,station_wmo_id,sandi_gts,ttaaii,cccc,need_ftp",
        "_size": "10000",
        "type_message": "1",
        "timestamp_data__gte": f"{tgl_awal.strftime('%Y-%m-%d')}T01:00:00",
        "timestamp_data__lte": f"{tgl_akhir.strftime('%Y-%m-%d')}T00:59:59",
    }
    headers = {
        "authorization": f"Bearer {token}",
        "accept": "application/json"
    }
    response = requests.get(url, params=params, headers=headers, verify=False)

    if response.status_code == 200:
        print(f"✅ Data berhasil diambil untuk periode {params['timestamp_data__gte']} s/d {params['timestamp_data__lte']}")
        return response.json()
    else:
        raise ValueError(f"❌ Gagal mengambil data: {response.status_code} - {response.text}")

# =======================================
# 6️⃣ Jalankan proses secara looping per bulan
# =======================================

# Dapatkan jumlah hari dalam bulan yang ditentukan
_, num_days = calendar.monthrange(TAHUN, BULAN)

print(f"Memulai proses untuk bulan {BULAN}-{TAHUN}, total {num_days} hari.")

# Loop untuk setiap hari
for hari in range(1, num_days + 1):
    # Set tanggal yang akan diproses di setiap iterasi
    TANGGAL = f"{TAHUN}-{BULAN:02d}-{hari:02d}"
    
    print(f"\n{'='*30}")
    print(f"🚀 MEMPROSES TANGGAL: {TANGGAL}")
    print(f"{'='*30}")

    # --- SCRIPT ASLI ANDA DIMULAI DI SINI (DENGAN INDENTASI) ---
    try:
        token = ambil_token(USERNAME, PASSWORD)
        data_json = ambil_data_gts(TANGGAL, token)

        # pastikan ada data
        if "items" not in data_json or not data_json["items"]:
            print(f"⚠️ Data kosong untuk tanggal {TANGGAL}, lanjut ke hari berikutnya.")
            continue # Melompat ke iterasi (hari) selanjutnya

        # ambil hanya kolom yang diperlukan
        df = pd.DataFrame(data_json["items"])[[
            "timestamp_data",
            "timestamp_sent_data",
            "station_wmo_id",
            "ttaaii",
            "cccc",
            "sandi_gts"
        ]]

        print("✅ Data berhasil dimuat ke DataFrame")

        df['timestamp_data'] = pd.to_datetime(df['timestamp_data'], errors='coerce')
        df['timestamp_sent_data'] = pd.to_datetime(df['timestamp_sent_data'], errors='coerce')

        # Format ulang supaya semua ada microseconds
        df['timestamp_data'] = df['timestamp_data'].dt.strftime("%Y-%m-%dT%H:%M:%S")
        df['timestamp_sent_data'] = df['timestamp_sent_data'].dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Urutkan agar timestamp_sent_data terbaru berada di atas
        data_sorted = df.sort_values(['station_wmo_id','timestamp_data', 'timestamp_sent_data'], ascending=[True, True, False])

        # Ambil satu data per timestamp_data, yang paling baru dikirim
        data = data_sorted.drop_duplicates(subset=['station_wmo_id', 'timestamp_data'], keep='first')

        data['timestamp_data'] = pd.to_datetime(data['timestamp_data'], errors='coerce')
        data['tanggal'] = data['timestamp_data'].dt.day
        data['jam'] = data['timestamp_data'].dt.hour

        def ambil_aaxx_beserta_isi(teks):
            """
            Ambil blok mulai dari AAXX (atau variasinya seperti AXX, AAAXX, AAX)
            hingga sebelum kelompok 333 (atau akhir pesan).
            Mengembalikan:
                - seksi01: potongan teks
                - false_aaxx: deskripsi kesalahan jika format AAXX tidak standar
            """
            if not isinstance(teks, str) or teks.strip() == "":
                return None, "teks kosong"

            # 1️⃣ Cari versi standar 'AAXX'
            match = re.search(r'(AAXX\s.*?\b333\b)', teks, re.DOTALL)
            if match:
                return match.group(1).strip(), ""  # tidak ada kesalahan

            # 2️⃣ Jika tidak ada, coba versi yang mirip tapi salah ketik (AXX, AAAXX, AAX)
            match_salah = re.search(r'\b(A{0,3}X{2,3})\s.*?\b333\b', teks, re.DOTALL)
            if match_salah:
                token = match_salah.group(1)
                if token != "AAXX":  # kalau bukan persis AAXX, berarti salah
                    return match_salah.group(0).strip(), "Tanda pengenal AAXX tidak valid"
                else:
                    return match_salah.group(0).strip(), ""

            # 3️⃣ Kalau tidak ditemukan sama sekali
            return None, "Tanda pengenal AAXX pada seksi 0 tidak valid"

        data[['seksi01', 'false_aaxx']] = data['sandi_gts'].apply(
            lambda x: pd.Series(ambil_aaxx_beserta_isi(x))
        )

        data['seksi01'] = (
        data['seksi01']
            .str.replace(r'CCA', '', regex=True)             # hapus semua CCA
            .str.replace(r'CCB', '', regex=True)             # hapus semua CCA
            .str.replace(r'\s+', ' ', regex=True)            # rapikan spasi
            .str.strip()                                     # hilangkan spasi awal/akhir
        )


        # --- 1. Lokasi folder-file
        #folder_coba = '/content/drive/MyDrive/CPNS BMKG Penerbangan/OBP/TugasOBP/Data_Excel/'  # Ganti ke folder tempat file Excel stasiun berada
        file_lokasi = 'Stasiun.xlsx'  # Ganti ke path file lokasi (lon, lat)

        # --- 2. Baca data lokasi stasiun
        df_lokasi = pd.read_excel(file_lokasi, sheet_name="Stasiun")  # pastikan file ini punya kolom: WMO_ID, Nama_stasiun, Longitude, Latitude

        # --- 🔧 Konversi tipe data WMO_ID agar bisa di-merge
        data['station_wmo_id'] = data['station_wmo_id'].astype(str)
        df_lokasi['station_wmo_id'] = df_lokasi['station_wmo_id'].astype(str)

        # --- 5. Gabungkan dengan data lokasi
        df_final = pd.merge(data, df_lokasi, on='station_wmo_id', how='inner')#[['timestamp_data','station_wmo_id', 'NAME','LAT', 'LON','ELEV','sandi_gts', 'Curah_Hujan','Heavy_Rain','Curah_Hujan_Jam','Gale','Kecepatan_angin','Arah_angin','Temperatur','Dew_Point','Tekanan_Permukaan','Tmin','Tmax','Evaporasi','Nh','CL','CM','CH']]
        df_final = df_final.dropna(subset=["LAT", "LON"])

        # --- Pisahkan kolom timestamp menjadi tanggal & jam ---
        df_final["timestamp_data"] = pd.to_datetime(df_final["timestamp_data"], errors="coerce")
        df_final["tanggal"] = df_final["timestamp_data"].dt.date.astype(str)
        df_final["jam"] = df_final["timestamp_data"].dt.strftime("%H:%M")
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        db_path_lengkap = "data_lengkap3.db"
        table_name_lengkap = "data_lengkap"
        conn_lengkap = sqlite3.connect(db_path_lengkap)
        cursor_lengkap = conn_lengkap.cursor()
        cursor_lengkap.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_lengkap} (
            tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL, REGION_DESC TEXT,
            sandi_gts TEXT,nddff TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
            Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL, Dew_Point REAL
        )""")
        for _, row in df_final.iterrows():
            cursor_lengkap.execute(f"DELETE FROM {table_name_lengkap} WHERE tanggal = ? AND jam = ? AND station_wmo_id = ?", (row["tanggal"], row["jam"], row["station_wmo_id"]))
        df_to_insert_lengkap = df_final[["tanggal","jam","station_wmo_id",'NAME','LAT',"LON",'ELEV',"REGION_DESC","sandi_gts","nddff","Curah_Hujan","Heavy_Rain","Curah_Hujan_Jam","Gale","Kecepatan_angin","Arah_angin","Temperatur","Tekanan_Permukaan","Tmin","Tmax","Dew_Point"]]
        df_to_insert_lengkap.to_sql(table_name_lengkap, conn_lengkap, if_exists="append", index=False)
        conn_lengkap.commit()
        conn_lengkap.close()
        tanggal_batch_lengkap = df_final["tanggal"].unique().tolist()
        print(f"✅ Data LENGKAP untuk tanggal {tanggal_batch_lengkap} berhasil diupdate ke {db_path_lengkap}")

#---------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
        df_final["timestamp_data"] = pd.to_datetime(df_final["timestamp_data"])

        # --- [FIX 1] Hitung 'tanggal_observasi' SEBELUM filter jam 00 ---
        # Ini adalah kunci terpenting: jam 00 dianggap milik hari sebelumnya
        df_final["tanggal_observasi"] = df_final["timestamp_data"].dt.date
        mask_jam00 = df_final["timestamp_data"].dt.hour == 0
        df_final.loc[mask_jam00, "tanggal_observasi"] = (
            df_final.loc[mask_jam00, "tanggal_observasi"] - pd.Timedelta(days=1)
        )
        # Konversi kembali ke object 'date' murni untuk merge yang konsisten
        df_final["tanggal_observasi"] = pd.to_datetime(df_final["tanggal_observasi"]).dt.date

        # --- Ambil data jam 00 (sekarang sudah punya 'tanggal_observasi' yang benar) ---
        df_jam00 = df_final[df_final["timestamp_data"].dt.hour == 0].copy()
        kolom_pilihan = [
            "timestamp_data", "station_wmo_id", "NAME", "sandi_gts", "LAT", "LON", "ELEV",
            "Arah_angin", "Kecepatan_angin", "Gale", "Temperatur", "Tmin", "Tmax",
            "Tekanan_Permukaan", "Curah_Hujan", "Heavy_Rain", "tanggal_observasi" # <-- Penting
        ]
        # Filter hanya kolom yang ada di df_final
        kolom_tersedia = [col for col in kolom_pilihan if col in df_jam00.columns]
        data_00 = df_jam00[kolom_tersedia]

        # --- [FIX 2] Hitung total curah hujan per stasiun DAN per tanggal observasi ---
        df_harian = df_final.groupby(
            ["station_wmo_id", "tanggal_observasi"], as_index=False
        )[["Curah_Hujan_Jam"]].sum()

        # --- [FIX 3] Gabungkan data jam 00 dan akumulasi harian (merge on DUA keys) ---
        data_merge = data_00.merge(
            df_harian, on=["station_wmo_id", "tanggal_observasi"], how="left"
        )

        # --- Pastikan tidak ada nilai NaN & hitung selisih ---
        data_merge["Curah_Hujan"] = data_merge["Curah_Hujan"].fillna(0)
        data_merge["Curah_Hujan_Jam"] = data_merge["Curah_Hujan_Jam"].fillna(0)
        data_merge["selisih"] = (data_merge["Curah_Hujan_Jam"] - data_merge["Curah_Hujan"]).abs()
        data_merge["selisih"] = data_merge["selisih"].round(2)

        # print("--- Hasil Merge dan Perhitungan Selisih (data_merge) ---")
        # print(data_merge[['station_wmo_id', 'tanggal_observasi', 'Curah_Hujan', 'Curah_Hujan_Jam', 'selisih']])
        # print("-" * 40, "\n")

        # --- Pisahkan Data BENAR ---
        toleransi = 2.0 + 1e-6 # Toleransi 2.0, dengan buffer kecil untuk float
        kondisi_benar = (data_merge["selisih"] <= toleransi) | \
                        ((data_merge["Curah_Hujan_Jam"] == 0) & (data_merge["Curah_Hujan"] == 0))
        data_akhir = data_merge[kondisi_benar].copy()

        # --- Pisahkan Data SALAH ---
        data_salah = data_merge.loc[~data_merge.index.isin(data_akhir.index)].copy()

        # print(f"--- Hasil Pemisahan ---")
        # print(f"Data BENAR (masuk data_akhir.db): {len(data_akhir)} stasiun")
        # print(data_akhir[['station_wmo_id', 'tanggal_observasi', 'selisih']])
        # print(f"Data SALAH (masuk data_salah2.db): {len(data_salah)} stasiun")
        # print(data_salah[['station_wmo_id', 'tanggal_observasi', 'selisih']])
        # print("-" * 40, "\n")


#---------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
 
        # --- Tambahkan kolom tanggal dan jam untuk penyimpanan ---
        data_akhir["timestamp_data"] = pd.to_datetime(data_akhir["timestamp_data"])
        data_akhir["tanggal"] = data_akhir["timestamp_data"].dt.date.astype(str)
        data_akhir["jam"] = data_akhir["timestamp_data"].dt.strftime("%H:%M")

        db_path_akhir = "data_akhir1.db"
        table_name_akhir = "data_akhir"
        conn_akhir = sqlite3.connect(db_path_akhir)
        cursor_akhir = conn_akhir.cursor()

        cursor_akhir.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_akhir} (
            tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL, REGION_DESC TEXT,
            sandi_gts TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
            Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL
        )""")

        # Tentukan kolom yang akan disimpan
        cols_akhir = [
            "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV","REGION_DESC", "sandi_gts",
            "Curah_Hujan", "Heavy_Rain", "Curah_Hujan_Jam", "Gale", "Kecepatan_angin",
            "Arah_angin", "Temperatur", "Tekanan_Permukaan", "Tmin", "Tmax"
        ]
        # Pastikan hanya menyimpan kolom yang ada
        cols_to_insert_akhir = [col for col in cols_akhir if col in data_akhir.columns]
        df_to_insert_akhir = data_akhir[cols_to_insert_akhir]

        # --- [FIX 4] Hapus data lama secara efisien (per batch tanggal) ---
        tanggal_batch_akhir = df_to_insert_akhir["tanggal"].unique().tolist()
        if tanggal_batch_akhir:
            for tgl in tanggal_batch_akhir:
                cursor_akhir.execute(f"DELETE FROM {table_name_akhir} WHERE tanggal = ?", (tgl,))
                print(f"🗑️ Hapus data lama tanggal {tgl} dari {table_name_akhir}")

            # Simpan data baru
            df_to_insert_akhir.to_sql(table_name_akhir, conn_akhir, if_exists="append", index=False)
            conn_akhir.commit()
            print(f"✅ Data AKHIR untuk tanggal {tanggal_batch_akhir} berhasil disimpan ke {db_path_akhir}")
        else:
            print("ℹ️ Tidak ada data AKHIR untuk disimpan.")

        conn_akhir.close()
        print("-" * 40, "\n")
#---------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------

        # Cek dulu apakah ada data yang salah
        if not data_salah.empty:
            
            # --- [BAGIAN 1: LOGIKA SCRIPT BARU YANG BENAR] ---
            # Ambil data full jam HANYA untuk stasiun & tanggal observasi yang salah
            # Ini JAUH LEBIH BAIK daripada filter 'Script Lama' Anda
            keys_salah = data_salah[["station_wmo_id", "tanggal_observasi"]].drop_duplicates()
            
            # Merge df_final dengan keys_salah untuk filter multi-kolom
            # Ini akan mengambil jam 03, 06, 09, ... DAN jam 00
            data_salah2 = df_final.merge(
                keys_salah, on=["station_wmo_id", "tanggal_observasi"], how="inner"
            ).copy()

            # --- [BAGIAN 2: FIX UNTUK MASALAH "jam 00 tidak muncul"] ---
            data_salah2["timestamp_data"] = pd.to_datetime(data_salah2["timestamp_data"])
            
            # !!! INI ADALAH KUNCI MASALAH ANDA !!!
            # JANGAN gunakan 'timestamp_data' untuk 'tanggal'
            # data_salah2["tanggal"] = data_salah2["timestamp_data"].dt.date.astype(str) # <-- JANGAN INI
            
            # GUNAKAN 'tanggal_observasi' sebagai 'tanggal'
            # Ini "memaksa" baris jam 00:00 (yang timestamp-nya hari +1)
            # untuk masuk ke 'tanggal' observasi yang sama.
            data_salah2["tanggal"] = data_salah2["tanggal_observasi"].astype(str)
            
            # Kolom 'jam' tetap normal
            data_salah2["jam"] = data_salah2["timestamp_data"].dt.strftime("%H:%M")
            data_salah2["tanggal_observasi"] = data_salah2["tanggal_observasi"].astype(str)
        else:
            # Buat DataFrame kosong jika tidak ada data salah
            data_salah2 = pd.DataFrame(columns=df_final.columns.tolist() + ["tanggal", "jam"])

        # --- Simpan ke DB ---
        db_path_salah = "data_suspect4.db"
        table_name_salah = "data_salah"
        conn_salah = sqlite3.connect(db_path_salah)
        cursor_salah = conn_salah.cursor()

        cursor_salah.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_salah} (
            tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL,REGION_DESC TEXT,
            sandi_gts TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
            Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL, 
            tanggal_observasi TEXT
        )""")

        # Tentukan kolom yang akan disimpan
        cols_salah = [
            "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV","REGION_DESC", "sandi_gts",
            "Curah_Hujan", "Heavy_Rain", "Curah_Hujan_Jam", "Gale", "Kecepatan_angin",
            "Arah_angin", "Temperatur", "Tekanan_Permukaan", "Tmin", "Tmax", "tanggal_observasi"
        ]
        cols_to_insert_salah = [col for col in cols_salah if col in data_salah2.columns]
        df_to_insert_salah = data_salah2[cols_to_insert_salah].dropna(subset=['tanggal'])


        # --- Hapus data lama secara efisien (per batch tanggal) ---
        tanggal_batch_salah = df_to_insert_salah["tanggal"].unique().tolist()

        if tanggal_batch_salah:
            for tgl in tanggal_batch_salah:
                if tgl is None: continue 
                cursor_salah.execute(f"DELETE FROM {table_name_salah} WHERE tanggal = ?", (tgl,))
                print(f"🗑️ Hapus data lama tanggal {tgl} dari {table_name_salah}")

            # Simpan data baru
            df_to_insert_salah.to_sql(table_name_salah, conn_salah, if_exists="append", index=False)
            conn_salah.commit()
            print(f"✅ Data SALAH untuk tanggal {tanggal_batch_salah} berhasil disimpan ke {db_path_salah}")
        else:
            print("ℹ️ Tidak ada data SALAH untuk disimpan.")

        conn_salah.close()

#---------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
        # 0. (opsional) sinkronkan index
        df_final = df_final.reset_index(drop=True)

        # 1. kolom utama yang ingin disimpan (ambil hanya yang ada di df_final)
        kolom_utama = [
            "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON",
            "ELEV", "REGION_DESC", "sandi_gts","seksi0", "seksi1","seksi3",
            "iihvv","yy","gg","sandi6","sandi63"
        ]
        kolom_utama = [c for c in kolom_utama if c in df_final.columns]

        # 2. ambil daftar kolom false_ dari df_final (pastikan hanya yang ada)
        #    <-- PERUBAHAN: Mengambil dari df_final, bukan 'data'
        kolom_false = [c for c in df_final.columns if c.startswith("false_")]

        # 3. buat mask: True kalau kolom false_ memiliki isi non-empty (bukan NaN dan bukan "")
        #    logika: untuk setiap kolom false_ -> (notna) AND (strip() != "")
        if len(kolom_false) == 0:
            print("⚠️ Tidak ditemukan kolom yang diawali 'false_' di dataframe `df_final`.")
            # <-- PERUBAHAN: Menggunakan panjang df_final
            mask_error = pd.Series([False]*len(df_final), index=df_final.index)
        else:
            mask_per_kol = []
            for c in kolom_false:
                # <-- PERUBAHAN: Mengambil dari df_final, bukan 'data'
                s = df_final[c]
                # convert to string for strip but keep check notna()
                has_value = s.notna() & s.astype(str).str.strip().ne("")
                mask_per_kol.append(has_value)
            # gabungkan: baris error jika ada minimal satu kolom false_ yang berisi
            mask_error = pd.concat(mask_per_kol, axis=1).any(axis=1)

        # 4. kolom akhir yang akan disimpan: gabungkan kolom_utama + kolom_false
        #    (Sekarang lebih sederhana karena semua kolom pasti ada di df_final)
        kolom_simpan = list(kolom_utama)  # salin
        for c in kolom_false:
            if c not in kolom_simpan:
                kolom_simpan.append(c)

        # 5. slice df_final berdasarkan mask_error
        data_error = df_final.loc[mask_error, kolom_simpan].copy()

        # 6. buat kolom Daftar_Kesalahan (gabungan isi kolom_false dari df_final)
        if len(data_error) > 0 and len(kolom_false) > 0:
            # <-- PERUBAHAN: Mengambil dari df_final, bukan 'data'
            #    Kita gunakan df_final.loc[mask_error] untuk mengambil baris yang sama
            #    yang baru saja kita masukkan ke data_error.
            daftar = df_final.loc[mask_error, kolom_false].apply(
                lambda row: "; ".join([str(v).strip() for v in row if pd.notna(v) and str(v).strip() != ""]),
                axis=1
            ).reset_index(drop=True)
            
            # reset index supaya align dengan data_error
            data_error = data_error.reset_index(drop=True)
            data_error["Daftar_Kesalahan"] = daftar
        else:
            # Pastikan kolom ada meskipun tidak ada error, untuk konsistensi skema
            data_error["Daftar_Kesalahan"] = "" 

       # 7. simpan ke sqlite (strategi: hapus data lama yg cocok, lalu append data baru)
        if len(data_error) > 0:
            db_path = "data_suspect4.db"
            table_name = "data_suspect"
            conn = sqlite3.connect(db_path)
            
            # --- TAMBAHAN UNTUK MENCEGAH DUPLIKAT ---
            # Kunci unik untuk data Anda
            key_cols = ["tanggal", "jam", "station_wmo_id"]

            # Pastikan kolom kunci ada di dataframe error
            if all(c in data_error.columns for c in key_cols):
                
                # 1. Ambil daftar tuple kunci unik dari data_error
                #    Kita pakai list(set(...)) untuk memastikan benar-benar unik
                unique_keys_to_process = list(set(
                    data_error[key_cols].itertuples(index=False, name=None)
                ))

                if unique_keys_to_process:
                    # 2. Buat query DELETE untuk menghapus baris lama yg cocok
                    delete_query = f"DELETE FROM {table_name} WHERE tanggal = ? AND jam = ? AND station_wmo_id = ?"
                    
                    # 3. Eksekusi penghapusan
                    cursor = conn.cursor()
                    try:
                        # executemany lebih cepat daripada looping
                        cursor.executemany(delete_query, unique_keys_to_process)
                        print(f"🔄 Dihapus {cursor.rowcount} baris lama yang cocok untuk di-replace...")
                        conn.commit() # Simpan perubahan (penghapusan)
                    except sqlite3.Error as e:
                        print(f"⚠️ Gagal menghapus data lama: {e}")
                        conn.rollback() # Batalkan jika gagal
                    finally:
                        cursor.close()
            else:
                print(f"⚠️ Kolom kunci {key_cols} tidak ditemukan. Melewatkan proses 'delete-then-append'.")
            # --- SELESAI TAMBAHAN ---

            # 4. Simpan data baru (data lama yang cocok sudah dihapus)
            try:
                data_error.to_sql(table_name, conn, index=False, if_exists="append")
                conn.close()
                print(f"✅ Disimpan/Diperbarui {len(data_error)} baris error ke {db_path} table {table_name}")
            except Exception as e:
                print(f"⚠️ Gagal menyimpan data ke SQL: {e}")
                conn.close()

        else:
            print("🎉 Tidak ada baris error — tidak ada yang disimpan.")


    except Exception as e:
        print(f"❌ GAGAL TOTAL memproses tanggal {TANGGAL}. Error: {e}")
        continue # Lanjut ke hari berikutnya jika terjadi error

print(f"\n{'='*30}")
print(f"✅ SELURUH PROSES UNTUK BULAN {BULAN}-{TAHUN} TELAH SELESAI.")
print(f"{'='*30}")