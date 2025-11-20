import streamlit as st
import folium
from folium import plugins
from streamlit_folium import st_folium
import pandas as pd
from folium import CustomIcon
from pymongo import MongoClient
from datetime import date, timedelta
#from deep_translator import GoogleTranslator
import re
import pickle
from pathlib import Path    
import streamlit_authenticator as stauth    


# ⚡ MongoDB Connection Setup
MONGODB_URI = "mongodb+srv://fadhilatulistiqomah:fadhilatul01@cuaca-ekstrem.bjnlh8j.mongodb.net/"
DB_NAME = "cuaca_ekstrem"
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

# Helper function untuk query data dari MongoDB
def get_data_from_mongodb(collection_name, query_filter):
    """
    Ambil data dari MongoDB collection
    """
    try:
        collection = db[collection_name]
        cursor = collection.find(query_filter)
        df = pd.DataFrame(list(cursor))
        # Hapus kolom _id jika ada
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        return df
    except Exception as e:
        st.error(f"Error mengambil data dari {collection_name}: {e}")
        return pd.DataFrame()

# --- Konfigurasi halaman (hanya boleh dipanggil sekali di paling atas) ---
st.set_page_config(page_title="🌦️ Peta Cuaca Ekstrem", layout="wide")

from utils.ui import setup_header,setup_sidebar_footer
setup_header()
setup_sidebar_footer()


file_path = Path(__file__).parent / "hashed_pw.pkl"
with file_path.open("rb") as file:
    credentials = pickle.load(file)

authenticator = stauth.Authenticate(
    credentials["usernames"],
    "abcdef",
    "dashboard_cookie",
    cookie_expiry_days=30
)

names, authentication_status, username = authenticator.login("Login", "main")

if authentication_status == False:
    st.error("Username/password salah")
    st.stop()
if authentication_status == None:
    st.warning("Silakan masukkan username dan password")
    st.stop()
if authentication_status:   

    # ==========================================================
    # 🧭 1️⃣ KONFIGURASI SIDEBAR DENGAN LOGO
    # ==========================================================

    # st.sidebar.markdown("""
    #     <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
    #         © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
    #     </div>
    # """, unsafe_allow_html=True)
    #st.title("Peta Cuaca Ekstrem")

    # ==========================================================
    # 💾 2️⃣ PENGAMBILAN DATA
    # ==========================================================

    # --- MongoDB Collections ---
    collection_akhir = "data_akhir"
    collection_lengkap = "data_lengkap"

    # st.markdown("""
    #     <style>
                
    #     label, .stDateInput label {
    #         font-size: 20px !important;
    #         font-weight: bold;
    #     }
                
    #     /* Ubah ukuran font di tabel Streamlit */
    #     .stDataFrame div[data-testid="stMarkdownContainer"] {
    #         font-size: 18px !important;
    #     }

    #     /* Ubah ukuran font di header tabel */
    #     .stDataFrame th {
    #         font-size: 18px !important;
    #         font-weight: bold !important;
    #     }

    #     /* Ubah ukuran font di isi tabel */
    #     .stDataFrame td {
    #         font-size: 16px !important;
    #     }
    #     </style>
    # """, unsafe_allow_html=True)


    # --- Widget utama untuk memilih tanggal ---
    pilih_tanggal = st.date_input(
        "📅 Pilih tanggal:",
        #value=date(2025, 1, 1),
        value=date.today(),
        min_value=date(2025, 1, 1),
        max_value=date(2025, 12, 31)
    )

    # --- 2a. Ambil data STASIUN & HEAVY RAIN dari MongoDB collection `data_akhir` ---
    query_filter_main = {"tanggal": pilih_tanggal.strftime("%Y-%m-%d")}
    df_main = get_data_from_mongodb(collection_akhir, query_filter_main)

    # Sort data
    if not df_main.empty:
        df_main = df_main.sort_values(by=['station_wmo_id', 'jam']).reset_index(drop=True)

    tanggal_sebelumnya = pilih_tanggal - timedelta(days=1)

    # --- 2b. Ambil data GALE dari MongoDB collection `data_lengkap` ---
    # Query untuk data jam 01-23 UTC hari sebelumnya DAN jam 00 UTC hari yang dipilih
    collection_gale = db[collection_lengkap]

    query_filter_gale = {
        "$or": [
            {
                "tanggal": tanggal_sebelumnya.strftime("%Y-%m-%d"),
                "jam": {"$ne": "00:00"},
                "Kecepatan_angin": {"$gte": 34}
            },
            {
                "tanggal": pilih_tanggal.strftime("%Y-%m-%d"),
                "jam": "00:00",
                "Kecepatan_angin": {"$gte": 34}
            }
        ]
    }

    df_gale = pd.DataFrame(list(collection_gale.find(query_filter_gale)))
    if '_id' in df_gale.columns:
        df_gale = df_gale.drop('_id', axis=1)

    # Sort data
    if not df_gale.empty:
        df_gale = df_gale.sort_values(by=['tanggal', 'jam', 'station_wmo_id']).reset_index(drop=True)

    # Pastikan tipe data numeric untuk filtering
    if not df_gale.empty:
        df_gale['Kecepatan_angin'] = pd.to_numeric(df_gale['Kecepatan_angin'], errors='coerce').fillna(0)


    # ==========================================================
    # 🗺️ 3️⃣ PEMBUATAN PETA
    # ==========================================================

    # st.markdown("<h1 style='margin-top: -30px;'>Peta Cuaca Ekstrem</h1>", unsafe_allow_html=True)

    #m = folium.Map(location=[-2, 118], zoom_start=5, tiles="CartoDB Voyager")
    m = folium.Map(location=[-2, 118], zoom_start=5, tiles=None)

    # Tambahkan tile CartoDB Voyager tanpa label attribution
    m = folium.Map(location=[-2, 118], zoom_start=5, tiles=None)

    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png",
        attr='© OpenStreetMap | © CARTO',
        name='Base Map',
        control=False
    ).add_to(m)
    # Buat LayerGroup untuk event cuaca ekstrem
    gale_layer = folium.FeatureGroup(name="Gale", show=True)
    hr_layer   = folium.FeatureGroup(name="Heavy Rain", show=True)

    # --- Loop 1: Plot semua stasiun dasar dan ikon Heavy Rain dari `df_main` ---
    if not df_main.empty:
        # Buat satu set untuk melacak stasiun yang sudah diplot agar tidak duplikat
        plotted_stations = set()
        for _, row in df_main.iterrows():
            lat, lon, station_id = row["LAT"], row["LON"], row["station_wmo_id"]
            wind_text = "Calm" if pd.isna(row['Kecepatan_angin']) else f"{row['Kecepatan_angin']} knot"
            rain_text = "-" if pd.isna(row['Curah_Hujan']) else f"{row['Curah_Hujan']} mm/hari"
            # Hanya plot titik biru stasiun SEKALI saja
            if pd.notna(lat) and pd.notna(lon) and station_id not in plotted_stations:

                folium.CircleMarker(
                    location=[lat, lon],
                    radius=2,
                    color="blue",
                    fill=True,
                    fill_color="blue",
                    fill_opacity=0.7,
                    popup=(
                        f"<div style='font-size:10px;width:200px;'>"
                        f"<b>{row['NAME']}</b><br>"
                        f"ID Stasiun: {row['station_wmo_id']} <br>"
                        f"Temperatur: {row['Temperatur']} °C<br>"
                        f"Curah Hujan: {rain_text} <br>"
                        f"Kecepatan Angin: {wind_text}"
                        f"</div>"
                    ),
                    tooltip=row["NAME"]
                ).add_to(m)
                plotted_stations.add(station_id)

            # Cek dan plot ikon Heavy Rain untuk setiap kejadian
            try:
                hr_value = float(row["Heavy_Rain"])
                if hr_value > 0:
                    rain_icon = CustomIcon("cloud_rain.png", icon_size=(30, 30))
                    folium.Marker(
                        location=[lat, lon], icon=rain_icon,
                        popup=(
                            f"<div style='font-size:10px;width:200px;'>"
                            f"<b>{row['NAME']}</b><br>{station_id}<br>🌧️ Heavy Rain: {hr_value} mm/hari"
                            f"</div>"
                        )
                    ).add_to(hr_layer)
            except (ValueError, TypeError):
                pass

    # --- Loop 2: Plot ikon Gale dari `df_gale` ---
    if not df_gale.empty:
        for _, row in df_gale.iterrows():
            lat, lon = row["LAT"], row["LON"]
            if pd.notna(lat) and pd.notna(lon):
                wind_icon = CustomIcon("wind.png", icon_size=(30, 30))
                folium.Marker(
                    location=[lat, lon], icon=wind_icon,
                    popup=(
                        f"<div style='font-size:10px;width:200px;'>"
                        f"<b>{row['NAME']}</b><br>{row['station_wmo_id']}<br> Gale: {row['Kecepatan_angin']} knot <br> Pukul : {row['jam']} UTC"
                        f"</div>"
                    )
                ).add_to(gale_layer)

    # Tambahkan layer ke peta
    gale_layer.add_to(m)
    hr_layer.add_to(m)
    folium.LayerControl().add_to(m)

    # --- TAMBAHKAN BLOK KODE INI ---
    plugins.Fullscreen(
        position="topright",  # Bisa 'topleft', 'topright', 'bottomleft', 'bottomright'
        title="Lihat Fullscreen", # Teks saat mouse hover
        title_cancel="Keluar Fullscreen", # Teks saat mouse hover (dalam mode fullscreen)
        force_separate_button=True,
    ).add_to(m)

    st_folium(m, width=None, height=500)


    # ==========================================================
    # 📋 4️⃣ HIGHLIGHT & TABEL DATA
    # ==========================================================
    tanggal_sebelumnya = pilih_tanggal - timedelta(days=1)
    st.markdown(f"""
    <hr style="border: 1px solid #ccc; margin: 20px 0;">
    <div style='background-color:#e9f2fb; padding:15px; border-radius:10px;'>
    <b>📌 Highlight:</b><br>
    1. Data yang ditampilkan merupakan data dari 00.01–23.59 UTC (<b>{tanggal_sebelumnya.strftime("%d %B %Y")}</b>)<br>
    2. Threshold untuk Heavy Rain adalah <b>50 mm/hari</b><br>
    3. Threshold untuk Gale adalah <b>34 knot</b>
    </div>
    """, unsafe_allow_html=True)


    # --- Tabel 1: Heavy Rain (dari df_main) ---
    st.markdown("### Daftar Stasiun dengan Kejadian Heavy Rain")
    df_hr = df_main.copy()
    df_hr["Heavy_Rain"] = pd.to_numeric(df_hr["Heavy_Rain"], errors="coerce")
    df_hr_filtered = df_hr[df_hr["Heavy_Rain"] > 0]
    if not df_hr_filtered.empty:
        df_tabel_hr = df_hr_filtered[[
            "station_wmo_id", "NAME","sandi_gts" ,"Heavy_Rain"
        ]].sort_values(by=["Heavy_Rain"], ascending=False).reset_index(drop=True)
        df_tabel_hr.index = df_tabel_hr.index + 1
        # 2. Buat salinan KHUSUS TAMPILAN (pola yang aman dan konsisten)
        df_tabel_hr_display = df_tabel_hr.copy()

        # 3. Definisikan regex
        regex_ekstraksi_kondisional = r'(\b(AAXX|AXX|AAAXX|AAX|AXXX)\b.*?(?:=|$))'
        
        # 4. Terapkan pembersihan ke SALINAN (df_tabel_hr_display)
        df_tabel_hr_display['sandi_gts'] = (
            df_tabel_hr_display['sandi_gts']
            .str.extract(regex_ekstraksi_kondisional, flags=re.DOTALL)[0] # Ekstrak
            .fillna("")                                   # Ganti error/NaN jadi string kosong
            .str.replace(r'CCA', '', regex=True)          # Hapus CCA
            .str.replace(r'CCB', '', regex=True)          # Hapus CCB
            .str.replace(r'\s+', ' ', regex=True)         # Rapikan spasi
            .str.strip()                                  # Hapus spasi awal/akhir
        )
        # ================================================================
        #         AKHIR DARI BAGIAN TAMBAHAN
        # ================================================================

        # 5. Tampilkan DataFrame salinan yang sudah bersih
        st.dataframe(
            df_tabel_hr_display,  # <-- Gunakan df_tabel_hr_display
            use_container_width=True,
            column_config={
                "station_wmo_id": st.column_config.Column("WMO ID Stasiun"),
                "NAME": st.column_config.Column("Nama Stasiun"),
                "sandi_gts": st.column_config.Column("Sandi GTS"), # Ini sudah bersih
                "Heavy_Rain": st.column_config.Column("Curah Hujan (mm/hari)")
            }
        )
        # #st.dataframe(df_tabel_hr, use_container_width=True)
        # st.dataframe(
        #     df_tabel_hr,  # DataFrame Anda yang sudah di-style
        #     use_container_width=True,
        #     column_config={
        #         "station_wmo_id": st.column_config.Column("WMO ID Stasiun"),
        #         "NAME": st.column_config.Column("Nama Stasiun"),
        #         "sandi_gts": st.column_config.Column("Sandi GTS"),
        #         "Heavy_Rain": st.column_config.Column("Curah Hujan (mm/hari)")
        #         # Format angka (misal "35.2") sudah diatur oleh .format() Anda,
        #         # jadi kita hanya perlu mengganti labelnya saja.
        #     }
        # )

        st.info(f"Total kejadian Heavy Rain (curah hujan ≥ 50 mm/hari) terdeteksi: {len(df_hr_filtered)} stasiun.")
    else:
        st.info("Tidak ada stasiun dengan kejadian Heavy Rain pada tanggal yang dipilih.")


    # # --- Tabel 2: Gale (dari df_gale) ---
    # st.markdown("### Daftar Stasiun dengan Kejadian Gale")
    # if not df_gale.empty:
    #     df_tabel_gale = df_gale[['station_wmo_id', 'NAME', 'jam', 'sandi_gts', 'Kecepatan_angin']].rename(columns={
    #         'station_wmo_id': 'WMO ID Stasiun', 'NAME': 'Nama Stasiun', 'jam': 'Jam Observasi (UTC)',
    #         'sandi_gts': 'Sandi GTS', 'Kecepatan_angin': 'Kecepatan Angin (knot)'
    #     }).sort_values(by="Kecepatan Angin (knot)", ascending=False).reset_index(drop=True)
    #     df_tabel_gale.index = df_tabel_gale.index + 1
    #     st.dataframe(df_tabel_gale, use_container_width=True)
    #     st.info(f"Total kejadian Gale (kecepatan angin ≥ 34 knot) terdeteksi: {len(df_gale)} stasiun.")
    # else:
    #     st.info("Tidak ada stasiun dengan kejadian Gale pada tanggal yang dipilih.")
    # --- Tabel 2: Gale (dari df_gale) ---
    st.markdown("### Daftar Stasiun dengan Kejadian Gale")

    if not df_gale.empty:
        # 1. Ini DataFrame asli Anda, disiapkan untuk tabel
        #    Kolom 'sandi_gts' diubah namanya menjadi 'Sandi GTS'
        df_tabel_gale = df_gale[['station_wmo_id', 'NAME', 'jam', 'sandi_gts', 'Kecepatan_angin']].rename(columns={
            'station_wmo_id': 'WMO ID Stasiun', 'NAME': 'Nama Stasiun', 'jam': 'Jam Observasi (UTC)',
            'sandi_gts': 'Sandi GTS', 'Kecepatan_angin': 'Kecepatan Angin (knot)'
        }).sort_values(by="Kecepatan Angin (knot)", ascending=False).reset_index(drop=True)
        
        df_tabel_gale.index = df_tabel_gale.index + 1
        
        # ================================================================
        #           BAGIAN TAMBAHAN UNTUK MEMPROSES TAMPILAN
        # ================================================================

        # 2. Buat salinan KHUSUS TAMPILAN
        df_tabel_gale_display = df_tabel_gale.copy()

        # 3. Definisikan regex
        regex_ekstraksi_kondisional = r'(\b(AAXX|AXX|AAAXX|AAX|AXXX)\b.*?(?:=|$))'
        
        # 4. Terapkan pembersihan ke SALINAN (df_tabel_gale_display)
        #    PERHATIKAN: Kita pakai nama kolom 'Sandi GTS' (hasil rename)
        df_tabel_gale_display['Sandi GTS'] = (
            df_tabel_gale_display['Sandi GTS']
            .str.extract(regex_ekstraksi_kondisional, flags=re.DOTALL)[0] # Ekstrak
            .fillna("")                                   # Ganti error/NaN jadi string kosong
            .str.replace(r'CCA', '', regex=True)          # Hapus CCA
            .str.replace(r'CCB', '', regex=True)          # Hapus CCB
            .str.replace(r'\s+', ' ', regex=True)         # Rapikan spasi
            .str.strip()                                  # Hapus spasi awal/akhir
        )
        # ================================================================
        #         AKHIR DARI BAGIAN TAMBAHAN
        # ================================================================

        # 5. Tampilkan DataFrame salinan yang sudah bersih
        st.dataframe(df_tabel_gale_display, use_container_width=True)
        
        st.info(f"Total kejadian Gale (kecepatan angin ≥ 34 knot) terdeteksi: {len(df_gale)} stasiun.")
    else:
        st.info("Tidak ada stasiun dengan kejadian Gale pada tanggal yang dipilih.")