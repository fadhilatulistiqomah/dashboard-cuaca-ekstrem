import streamlit as st
from pymongo import MongoClient
import pandas as pd
from datetime import date, timedelta
import re

# --- MongoDB Setup ---
MONGODB_URI = "mongodb+srv://fadhilatulistiqomah:fadhilatul01@cuaca-ekstrem.bjnlh8j.mongodb.net/"
DB_NAME = "cuaca_ekstrem"

try:
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    db = client[DB_NAME]
except Exception as e:
    st.error(f"❌ Gagal terhubung ke MongoDB: {e}")
    st.stop()

def get_data_from_mongodb(collection_name, query_filter):
    """Helper function untuk query data dari MongoDB"""
    collection = db[collection_name]
    data = list(collection.find(query_filter))
    df = pd.DataFrame(data)
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)
    return df

# --- Konfigurasi halaman ---
st.set_page_config(page_title="Data Suspect", layout="wide")
from utils.ui import setup_header,setup_sidebar_footer
setup_header()
setup_sidebar_footer()
# ==========================================================
# 🧭 KONTEN SIDEBAR
# ==========================================================
#st.sidebar.image("Logo_BMKG.png", caption="Dashboard Monitoring Cuaca Ekstrem")
# st.sidebar.markdown("""
#     <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
#         © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
#     </div>
# """, unsafe_allow_html=True)

# ==========================================================
# 📅 WIDGET TANGGAL (Digunakan oleh semua tab)
# ==========================================================
#st.title("Pengecekan Data Suspect")

pilih_tanggal = st.date_input(
    "📅 Pilih tanggal acuan untuk pengecekan data:",
    #value=date(2025, 1, 2),
    value=date.today(),
    min_value=date(2025, 1, 1),
    max_value=date(2025, 12, 31),
    help="Untuk Heavy Rain, tanggal ini akan menampilkan data dari jam 01:00 hari sebelumnya hingga 00:00 tanggal yang dipilih."
)

# ==========================================================
# 📑 PEMBUATAN TABS
# ==========================================================
tab1, tab2, tab3 = st.tabs(["Heavy Rain Suspect", "Gale Suspect", "Sandi SYNOP Suspect"])
st.markdown("""
    <style>
    /* Atur layout tab agar lebar terbagi rata */
    div[data-baseweb="tab-list"] {
        display: flex;
        justify-content: space-around;  /* bisa diganti space-evenly */
        width: 100%;
    }

    /* Tampilan teks dan padding */
    button[data-baseweb="tab"] {
        font-size: 18px !important;
        font-weight: 600 !important;
        color: #003366 !important;
        flex: 1;  /* biar tiap tab punya lebar yang sama */
        text-align: center !important;
        padding: 12px 0 !important;
        border-radius: 10px 10px 0 0 !important;
    }

    /* Saat tab aktif */
    button[data-baseweb="tab"][aria-selected="true"] {
        background-color: #e6f0ff !important;
        border-bottom: 3px solid #0078D4 !important;
        color: #000 !important;
    }
    </style>
""", unsafe_allow_html=True)


# --- KODE UNTUK TAB 1: HEAVY RAIN SUSPECT ---
with tab1:
    # --- MongoDB Collection Setup ---
    collection_suspect = "data_suspect"
    
    # --- Logika Tanggal untuk Heavy Rain ---
    tanggal_untuk_query = pilih_tanggal - timedelta(days=1)

    # --- Query untuk Heavy Rain dari MongoDB ---
    query_filter = {"tanggal": tanggal_untuk_query.strftime("%Y-%m-%d")}
    df_hr = get_data_from_mongodb(collection_suspect, query_filter)
    df_hr = df_hr.sort_values(by=["station_wmo_id", "jam"]) if not df_hr.empty else df_hr

    # --- Pemrosesan Data Heavy Rain ---
    mask = ~(
        (df_hr["jam"] == "00:00") &
        (df_hr.groupby("station_wmo_id")["jam"].transform("count") == 1)
    )
    df_hr = df_hr[mask].copy()

    df_hr["sort_order"] = df_hr["jam"].apply(lambda x: 1 if x == "00:00" else 0)
    df_hr = df_hr.sort_values(by=["station_wmo_id", "sort_order", "jam"]).drop(columns="sort_order")

    if df_hr.empty:
        st.info(f"Tidak ada data curah hujan suspect untuk periode yang berakhir pada {pilih_tanggal.strftime('%d %B %Y')} pukul 00:00 UTC.")
    else:




# --- Tampilkan data per stasiun ---
        for station_id, group in df_hr.groupby(["station_wmo_id", "NAME"]):
            st.subheader(f"{station_id[0]} - {station_id[1]}")

            # --- MODIFIKASI DIMULAI DI SINI ---

            # 1. Hitung total curah hujan per jam (dari kode Anda di bawah)
            curah_hujan_jam = group["Curah_Hujan_Jam"].sum()

            # 2. Buat tabel data utama seperti sebelumnya
            df_table = group[["jam", "sandi_gts", "Curah_Hujan_Jam"]].reset_index(drop=True)
            df_table = df_table.fillna("-")
            df_table.columns = ["Jam", "Sandi Synop", "Curah Hujan"]

            df_tabel_display = df_table.copy()

            # 3. Definisikan regex
            regex_ekstraksi_kondisional = r'(\b(AAXX|AXX|AAAXX|AAX|AXXX)\b.*?(?:=|$))'
            
            # 4. Terapkan pembersihan ke SALINAN (df_tabel_gale_display)
            #    PERHATIKAN: Kita pakai nama kolom 'Sandi GTS' (hasil rename)
            df_tabel_display['Sandi Synop'] = (
                df_tabel_display['Sandi Synop']
                .str.extract(regex_ekstraksi_kondisional, flags=re.DOTALL)[0] # Ekstrak
                .fillna("")                                   # Ganti error/NaN jadi string kosong
                .str.replace(r'CCA', '', regex=True)          # Hapus CCA
                .str.replace(r'CCB', '', regex=True)          # Hapus CCB
                .str.replace(r'\s+', ' ', regex=True)         # Rapikan spasi
                .str.strip()                                  # Hapus spasi awal/akhir
            )

            # 3. Buat DataFrame baru untuk baris total
            #    Kita gunakan format tebal (bold) agar menonjol
            total_row_df = pd.DataFrame({
                "Jam": [""],
                "Sandi Synop": ["Total"],
                "Curah Hujan": [f"{curah_hujan_jam:.1f} mm" if curah_hujan_jam != 0 else "-"]
            })

            # 4. Gabungkan tabel data dengan tabel total
            df_table_final = pd.concat([df_tabel_display, total_row_df], ignore_index=True)

            # 5. Tampilkan tabel yang sudah digabung
            st.write(df_table_final.to_html(index=False, escape=False), unsafe_allow_html=True)

            # --- MODIFIKASI SELESAI ---

            # Sisa kode Anda untuk summary table
            curah_hujan_00 = group.loc[group["jam"] == "00:00", "Curah_Hujan"].sum()
            # Baris di bawah ini tidak diperlukan lagi karena sudah dihitung di atas
            # curah_hujan_jam = group["Curah_Hujan_Jam"].sum() 
            selisih = curah_hujan_00 - curah_hujan_jam

            df_summary = pd.DataFrame({
                "Curah Hujan 24 Jam (RRR)": [f"{curah_hujan_00} mm" if curah_hujan_00 != 0 else "-"],
                "Total Akumulasi Per Jam (6RRRtR)": [f"{curah_hujan_jam} mm" if curah_hujan_jam != 0 else "-"],
                "Selisih": [f"{selisih:.1f} mm" if selisih != 0 else "-"]
            })

            st.write(df_summary.to_html(index=False, escape=False, classes="summary-table"), unsafe_allow_html=True)
            st.markdown("---")
        # --- Tampilkan data per stasiun ---
        # for station_id, group in df_hr.groupby(["station_wmo_id", "NAME"]):
        #     st.subheader(f"{station_id[0]} - {station_id[1]}")

        #     df_table = group[["jam", "sandi_gts", "Curah_Hujan_Jam"]].reset_index(drop=True)
        #     df_table = df_table.fillna("-")
        #     df_table.columns = ["Jam", "Sandi Synop", "Curah Hujan"]
        #     st.write(df_table.to_html(index=False, escape=False), unsafe_allow_html=True)

        #     curah_hujan_00 = group.loc[group["jam"] == "00:00", "Curah_Hujan"].sum()
        #     curah_hujan_jam = group["Curah_Hujan_Jam"].sum()
        #     selisih = curah_hujan_00 - curah_hujan_jam

        #     df_summary = pd.DataFrame({
        #         "Curah Hujan 24 Jam (RRR)": [f"{curah_hujan_00} mm" if curah_hujan_00 != 0 else "-"],
        #         "Total Akumulasi Per Jam (6RRRtR)": [f"{curah_hujan_jam} mm" if curah_hujan_jam != 0 else "-"],
        #         "Selisih": [f"{selisih:.1f} mm" if selisih != 0 else "-"]
        #     })

        #     st.write(df_summary.to_html(index=False, escape=False, classes="summary-table"), unsafe_allow_html=True)
        #     st.markdown("---")


# --- KODE UNTUK TAB 2: GALE SUSPECT ---
with tab2:
    #st.header("Laporan Angin Kencang (Gale) dengan Data 'nddff' Kosong")

    # --- MongoDB Collection Setup ---
    collection_gale = "data_suspect"

    # --- Query untuk mencari data Gale dengan nddff kosong dari MongoDB ---
    query_filter = {
        "tanggal": pilih_tanggal.strftime("%Y-%m-%d"),
        "false_nddff": "Sandi nddff tidak ditemukan di seksi 1"
    }
    df_gale = get_data_from_mongodb(collection_gale, query_filter)
    df_gale = df_gale.sort_values(by=["station_wmo_id", "jam"]) if not df_gale.empty else df_gale

    # --- Tampilkan hasil ---
    if df_gale.empty:
        st.info(f"✅ Tidak ada data Gale suspect (nddff kosong) pada tanggal {pilih_tanggal.strftime('%d %B %Y')}.")
    else:
        st.warning(f" Ditemukan {len(df_gale)} laporan Gale suspect (nddff kosong) pada tanggal {pilih_tanggal.strftime('%d %B %Y')}.")
        
        for station_id, group in df_gale.groupby(["station_wmo_id", "NAME"]):
            st.subheader(f"{station_id[0]} - {station_id[1]}")
            
            # Buat tabel detail hanya dengan kolom yang diminta
            df_table = group[["jam", "sandi_gts"]].reset_index(drop=True)
            df_table.columns = ["Jam", "Sandi Synop"]

            df_tabel_display = df_table.copy()

            # 3. Definisikan regex
            regex_ekstraksi_kondisional = r'(\b(AAXX|AXX|AAAXX|AAX|AXXX)\b.*?(?:=|$))'
            
            # 4. Terapkan pembersihan ke SALINAN (df_tabel_gale_display)
            #    PERHATIKAN: Kita pakai nama kolom 'Sandi GTS' (hasil rename)
            df_tabel_display['Sandi Synop'] = (
                df_tabel_display['Sandi Synop']
                .str.extract(regex_ekstraksi_kondisional, flags=re.DOTALL)[0] # Ekstrak
                .fillna("")                                   # Ganti error/NaN jadi string kosong
                .str.replace(r'CCA', '', regex=True)          # Hapus CCA
                .str.replace(r'CCB', '', regex=True)          # Hapus CCB
                .str.replace(r'\s+', ' ', regex=True)         # Rapikan spasi
                .str.strip()                                  # Hapus spasi awal/akhir
            )
            
            # Tampilkan tabel
            st.write(df_tabel_display.to_html(index=False, escape=False), unsafe_allow_html=True)
            st.markdown("---")

# --- CSS styling (diletakkan di akhir agar berlaku untuk semua tabel) ---
st.markdown("""
    <style>
    table {
        table-layout: fixed;
        width: 100%;
    }
    th:nth-child(1), td:nth-child(1) {
        width: 80px !important;
        text-align: center;
    }
    th:nth-child(2), td:nth-child(2) {
        white-space: pre-wrap !important;
        word-wrap: break-word !important;
        text-align: left;
    }
    th:nth-child(3), td:nth-child(3) {
        width: 400px !important;
        text-align: center;
    }
    /* CSS khusus tabel ringkasan */
    .summary-table td, .summary-table th {
        text-align: center !important;
        width: 33% !important;
    }
    </style>
""", unsafe_allow_html=True)

# 
# --- KODE UNTUK TAB 3: DATA SUSPECT ---
with tab3:
    st.header("Data Suspect - Kesalahan Sandi SYNOP")

    # --- MongoDB Collection Setup ---
    collection_susp = "data_suspect"

    # --- Query semua dokumen untuk tanggal yang dipilih ---
    query_filter = {"tanggal": pilih_tanggal.strftime("%Y-%m-%d")}
    df_susp = get_data_from_mongodb(collection_susp, query_filter)
    df_susp = df_susp.sort_values(by=["station_wmo_id", "jam"]) if not df_susp.empty else df_susp

    # --- Cek apakah data kosong ---
    if df_susp.empty:
        st.info(f"✅ Tidak ada kesalahan sandi SYNOP pada tanggal {pilih_tanggal.strftime('%d %B %Y')}.")
    else:
        # --- Cari kolom yang diawali dengan 'false_' ---
        false_cols = [col for col in df_susp.columns if col.startswith("false_")]

        # # --- Gabungkan isi kolom false_* menjadi satu kolom 'Daftar_Kesalahan' ---
        # def gabung_kesalahan(row):
        #     errors = [str(row[col]) for col in false_cols if pd.notna(row[col]) and str(row[col]).strip() != ""]
        #     return "; ".join(errors) if errors else None

    # --- Gabungkan isi kolom false_* menjadi satu kolom 'Daftar_Kesalahan' ---
        def gabung_kesalahan(row):
            errors = [str(row[col]) for col in false_cols if pd.notna(row[col]) and str(row[col]).strip() != ""]
            # Ganti separator menjadi tag HTML <br> untuk baris baru
            return "<br>".join(errors) if errors else None # <--- BARIS YANG DIUBAH

        df_susp["Kesalahan"] = df_susp.apply(gabung_kesalahan, axis=1)

        # --- Hapus baris tanpa kesalahan sama sekali ---
        df_susp = df_susp[df_susp["Kesalahan"].notna()].reset_index(drop=True)

        # --- Tampilkan hasil ---
        if df_susp.empty:
            st.info(f"✅ Tidak ada kesalahan sandi SYNOP pada tanggal {pilih_tanggal.strftime('%d %B %Y')}.")
        else:
            st.warning(f"⚠️ Ditemukan {len(df_susp)} laporan kesalahan sandi SYNOP pada tanggal {pilih_tanggal.strftime('%d %B %Y')}.")
            
            # --- Tampilkan per stasiun ---
            for station_id, group in df_susp.groupby(["station_wmo_id", "NAME"]):
                st.subheader(f"{station_id[0]} - {station_id[1]}")
                
                # Pilih kolom untuk ditampilkan
                df_table = group[["jam", "sandi_gts", "Kesalahan"]].reset_index(drop=True)
                df_table.columns = ["Jam", "Sandi Synop", "Kesalahan"]

                df_tabel_display = df_table.copy()

                # 3. Definisikan regex
                regex_ekstraksi_kondisional = r'(\b(AAXX|AXX|AAAXX|AAX|AXXX)\b.*?(?:=|$))'
                
                # 4. Terapkan pembersihan ke SALINAN (df_tabel_gale_display)
                #    PERHATIKAN: Kita pakai nama kolom 'Sandi GTS' (hasil rename)
                df_tabel_display['Sandi Synop'] = (
                    df_tabel_display['Sandi Synop']
                    .str.extract(regex_ekstraksi_kondisional, flags=re.DOTALL)[0] # Ekstrak
                    .fillna("")                                   # Ganti error/NaN jadi string kosong
                    .str.replace(r'CCA', '', regex=True)          # Hapus CCA
                    .str.replace(r'CCB', '', regex=True)          # Hapus CCB
                    .str.replace(r'\s+', ' ', regex=True)         # Rapikan spasi
                    .str.strip()                                  # Hapus spasi awal/akhir
                )
                
                # Tampilkan tabel HTML dengan style
                st.write(df_tabel_display.to_html(index=False, escape=False), unsafe_allow_html=True)
                st.markdown("---")
