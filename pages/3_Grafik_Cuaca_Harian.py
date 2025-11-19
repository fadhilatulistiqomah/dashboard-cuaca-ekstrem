import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import numpy as np
from windrose import WindroseAxes
from datetime import date
import re

# --- Konfigurasi halaman ---
st.set_page_config(page_title="Data Cuaca Harian", layout="wide")

from utils.ui import setup_header,setup_sidebar_footer
setup_header()
setup_sidebar_footer()
# ==========================================================
# 🧭 1️⃣ KONFIGURASI SIDEBAR DENGAN LOGO
# ==========================================================
# --- CSS styling untuk sidebar ---
# st.markdown("""
#     <style>
#         [data-testid="stSidebar"] {
#             background-color: #f7f9fb;
#             padding-top: 0px;
#         }
#     </style>
# """, unsafe_allow_html=True)

# --- Konten Sidebar ---
# st.sidebar.image("Logo_BMKG.png", caption="Dashboard Monitoring Cuaca Ekstrem")
# st.sidebar.markdown("""
#     <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
#         © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
#     </div>
# """, unsafe_allow_html=True)

#st.title("Data Cuaca Harian per Stasiun")

# --- Koneksi ke database SQLite ---
db_path = "data_lengkap3.db"   # ganti sesuai path
table_name = "data_lengkap"    # ganti sesuai tabel
conn = sqlite3.connect(db_path)



# --- Ambil daftar stasiun ---
df_stasiun = pd.read_sql_query(
    f"SELECT DISTINCT station_wmo_id, NAME FROM {table_name} ORDER BY station_wmo_id", conn
)

# Buat dictionary untuk sinkronisasi dua arah
id_to_name = dict(zip(df_stasiun["station_wmo_id"], df_stasiun["NAME"]))
name_to_id = dict(zip(df_stasiun["NAME"], df_stasiun["station_wmo_id"]))

# --- Inisialisasi session_state ---
if "selected_wmo" not in st.session_state:
    st.session_state.selected_wmo = None
if "selected_name" not in st.session_state:
    st.session_state.selected_name = None

# --- Fungsi sinkronisasi dua arah ---
def update_from_wmo():
    wmo = st.session_state.selected_wmo
    st.session_state.selected_name = id_to_name.get(wmo, None)

def update_from_name():
    name = st.session_state.selected_name
    st.session_state.selected_wmo = name_to_id.get(name, None)

# --- Widget input ---
col1, col2, col3 = st.columns(3)

with col1:
    pilih_tanggal = st.date_input(
        "📅 Pilih tanggal:",
        value=date.today(),
        min_value=date(2025, 1, 1),
        max_value=date(2025, 12, 31)
    )

with col2:
    st.selectbox(
        "🏷️ Pilih WMO ID Stasiun:",
        options=[""] + list(id_to_name.keys()),
        key="selected_wmo",
        on_change=update_from_wmo
    )

with col3:
    st.selectbox(
        "🏷️ Pilih Nama Stasiun:",
        options=[""] + list(name_to_id.keys()),
        key="selected_name",
        on_change=update_from_name
    )

# --- Ambil nilai akhir yang tersinkronisasi ---
pilih_station = st.session_state.selected_wmo
pilih_nama = st.session_state.selected_name

# --- Tampilkan hasil ---
if pilih_station and pilih_nama:
    st.success(f"Stasiun terpilih: **{pilih_nama} ({pilih_station})**")
else:
    st.info("Silakan pilih salah satu: WMO ID atau Nama Stasiun.")


# --- Query data ---
query = f"""
SELECT station_wmo_id, NAME, jam, tanggal, sandi_gts,
       Tekanan_Permukaan, Temperatur, Kecepatan_angin, Arah_angin, Curah_Hujan_Jam,Dew_Point
FROM {table_name}
WHERE tanggal = ?
  AND station_wmo_id = ?
ORDER BY jam
"""
df = pd.read_sql_query(query, conn, params=(pilih_tanggal.strftime("%Y-%m-%d"), pilih_station))
conn.close()

if df.empty:
    st.warning("⚠️ Tidak ada data untuk tanggal dan stasiun yang dipilih.")
    st.stop()

# --- Pastikan jam urut ---
df["jam"] = pd.to_datetime(df["jam"], format="%H:%M").dt.strftime("%H:%M")
df.index = df.index + 1
# --- Tampilkan data utama ---

# 1. Definisikan regex Anda
regex_ekstraksi_kondisional = r'(\b(AAXX|AXX|AAAXX|AAX|AXXX)\b.*?(?:=|$))'

# 2. Buat SALINAN DataFrame khusus untuk tampilan
df_display = df.copy()

# 3. Terapkan ekstraksi DAN pembersihan pada kolom 'sandi_gts'
#    di DataFrame salinan tersebut.
df_display['sandi_gts'] = (
    df_display['sandi_gts']
    .str.extract(regex_ekstraksi_kondisional, flags=re.DOTALL)[0] # Ekstrak teks
    .fillna("")                                   # Ganti error/NaN jadi string kosong
    .str.replace(r'CCA', '', regex=True)          # Hapus CCA
    .str.replace(r'CCB', '', regex=True)          # Hapus CCB
    .str.replace(r'\s+', ' ', regex=True)         # Rapikan spasi
    .str.strip()                                  # Hapus spasi awal/akhir
)

# 4. Tampilkan DataFrame yang SUDAH DIMODIFIKASI ke dashboard
st.dataframe(
    df_display,  # <-- PENTING: Gunakan df_display, bukan df
    column_config={
        "station_wmo_id": st.column_config.Column("ID Stasiun"),
        "NAME": st.column_config.Column("Nama Stasiun"),
        "jam": st.column_config.Column("Jam"),
        "tanggal": st.column_config.Column("Tanggal"),
        "sandi_gts": st.column_config.Column("Sandi GTS"), # Label tetap "Sandi GTS"
        "Tekanan_Permukaan": st.column_config.Column("Tekanan Permukaan"),
        "Temperatur": st.column_config.Column("Temperatur"),
        "Kecepatan_angin": st.column_config.Column("Kecepatan Angin"),
        "Arah_angin": st.column_config.Column("Arah Angin"),
        "Curah_Hujan_Jam": st.column_config.Column("Curah Hujan"),
        "Dew_Point": st.column_config.Column("Titik Embun"),
    }
)
#st.subheader(f"📍 Data Stasiun {pilih_station} pada {pilih_tanggal.strftime('%d-%m-%Y')}")
#st.dataframe(df)
# st.dataframe(
#     df,  # DataFrame Anda yang sudah di-style
#     column_config={
#         "station_wmo_id": st.column_config.Column("ID Stasiun"),
#         "NAME": st.column_config.Column("Nama Stasiun"),
#         "jam": st.column_config.Column("Jam"),
#         "tanggal": st.column_config.Column("Tanggal"),
#         "sandi_gts": st.column_config.Column("Sandi GTS"),
#         "Tekanan_Permukaan": st.column_config.Column("Tekanan Permukaan"),
#         "Temperatur": st.column_config.Column("Temperatur"),
#         "Kecepatan_angin": st.column_config.Column("Kecepatan Angin"),
#         "Arah_angin": st.column_config.Column("Arah Angin"),
#         "Curah_Hujan_Jam": st.column_config.Column("Curah Hujan"),
#         "Dew_Point": st.column_config.Column("Titik Embun"),
#         # Format angka (misal "35.2") sudah diatur oleh .format() Anda,
#         # jadi kita hanya perlu mengganti labelnya saja.
#     }
# )

# --- Buat tabs untuk grafik ---
tab1, tab2, tab3, tab4, tab5= st.tabs([
    "Tekanan Permukaan",
    "Temperatur Permukaan",
    "Kelembaban Relatif",
    "Kecepatan Angin",
    "Windrose"
])

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


# --- TAB 1: Tekanan Permukaan ---
with tab1:
    st.subheader("Tekanan Permukaan")
    fig1 = px.bar(df, x="jam", y="Tekanan_Permukaan",
                   title="Tekanan Permukaan (hPa)")
    # Tambahkan baris ini untuk mengatur sumbu Y
    fig1.update_yaxes(
        range=[980, 1014],  # Menetapkan rentang minimum dan maksimum
        dtick=4            # Menetapkan interval antar-label menjadi 50
    )
    st.plotly_chart(fig1, use_container_width=True)

# --- TAB 2: Temperatur ---
with tab2:
    st.subheader("Temperatur Permukaan")
    fig2 = px.line(df, x="jam", y="Temperatur", markers=True,
                   title="Temperatur (°C)")
    st.plotly_chart(fig2, use_container_width=True)
    
# --- TAB 2: Temperatur ---
# with tab3:
#     st.subheader("Temperatur Titik Embun")
#     df = 100 - 5 * (df['Temperatur'] - df['Dew_Point'])
#     fig3 = px.line(df, x="jam", y="Relatif Humidity", markers=True,
#                    title="Kelembaban Relatif (%)")
#     st.plotly_chart(fig3, use_container_width=True)
with tab3:
    st.subheader("Kelembaban Relatif (RH)")

    # --- Hitung RH ---
    df["Kelembaban_Relatif"] = 100 - 5 * (df["Temperatur"] - df["Dew_Point"])

    # Pastikan nilai RH tidak lebih dari 100 atau kurang dari 0
    df["Kelembaban_Relatif"] = df["Kelembaban_Relatif"].clip(lower=0, upper=100)

    # --- Plot grafik RH ---
    fig3 = px.line(
        df,
        x="jam",
        y="Kelembaban_Relatif",
        markers=True,
        title="Kelembaban Relatif (%)"
    )

    st.plotly_chart(fig3, use_container_width=True)

# --- TAB 3: Kecepatan Angin ---
with tab4:
    st.subheader("Kecepatan Angin")
    fig4 = px.bar(df, x="jam", y="Kecepatan_angin",
                   title="Kecepatan Angin (knot)")
        # Tambahkan baris ini untuk mengatur sumbu Y
    fig4.update_yaxes(
        range=[0, 30],  # Menetapkan rentang minimum dan maksimum
        dtick=5            # Menetapkan interval antar-label menjadi 50
    )
    st.plotly_chart(fig4, use_container_width=True)

# --- TAB 4: Windrose ---
with tab5:
    st.subheader("Windrose (Distribusi Arah & Kecepatan Angin)")

    if df["Arah_angin"].notna().sum() > 0 and df["Kecepatan_angin"].notna().sum() > 0:
        fig = plt.figure(figsize=(6,6))
        ax = WindroseAxes.from_ax(fig=fig)

        ax.bar(
            df["Arah_angin"].dropna(),
            df["Kecepatan_angin"].dropna(),
            normed=True,
            opening=0.8,
            edgecolor="white",
            bins=np.arange(0, 12, 2)
        )

        ax.set_title(f"Windrose Stasiun {pilih_station}", fontsize=16, pad=20, fontweight='bold')
        ax.set_xticklabels(['E', 'NE', 'N', 'NW', 'W', 'SW', 'S', 'SE'], fontsize=12)

        ax.legend(
            title="Kecepatan Angin (knot)",
            loc='upper center',
            bbox_to_anchor=(1.2, 1.0),
            shadow=True,
            ncol=1
        )

        #st.pyplot(fig)
        st.pyplot(fig, use_container_width=True)
    else:
        st.info("⚠️ Data arah atau kecepatan angin tidak tersedia untuk tanggal ini.")
