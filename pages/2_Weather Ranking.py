import streamlit as st
import sqlite3
import pandas as pd
from datetime import date
from utils.ui import setup_header,setup_sidebar_footer
from datetime import timedelta
from pathlib import Path


# --- Konfigurasi halaman ---
st.set_page_config(page_title="Weather Ranking", layout="wide")
# st.markdown("""
#     <style>
#     /* Target label "Pilih tanggal:" (yang ada di dalam tag <p>) */
#     div[data-testid="stDateInput"] label[data-testid="stWidgetLabel"] p {
#          font-size: 18px !important;
#     }

#     /* Target label "Tampilkan berapa stasiun teratas:" (yang ada di dalam tag <p>) */
#     div[data-testid="stSlider"] label[data-testid="stWidgetLabel"] p {
#          font-size: 18px !important;
#     }

#     /* Target header tabel (WMO ID, Nama Stasiun, dll.) */
#     /* Kita target kelas AG Grid-nya langsung */
#     .st-ag-header-cell-text {
#         font-size: 16px !important;
#     }

#     /* Target sel/isi data di dalam tabel (angka dan nama stasiun) */
#     .st-ag-cell-value {
#         font-size: 16px !important;
#     }
#     </style>
# """, unsafe_allow_html=True)
st.markdown("""
    <style>
    /* [INI SUDAH BERHASIL] Target label "Pilih tanggal:" */
    div[data-testid="stDateInput"] label[data-testid="stWidgetLabel"] p {
         font-size: 18px !important;
    }

    /* [INI SUDAH BERHASIL] Target label "Tampilkan berapa stasiun teratas:" */
    div[data-testid="stSlider"] label[data-testid="stWidgetLabel"] p {
         font-size: 18px !important;
    }

    /* [BARU - V3] Target header tabel */
    div[data-testid="stDataFrame"] .st-ag-header-cell-text {
        font-size: 16px !important; 
    }

    /* [BARU - V3] Target sel/isi data di dalam tabel */
    div[data-testid="stDataFrame"] .st-ag-cell-value {
        font-size: 16px !important;
    }
    </style>
""", unsafe_allow_html=True)
setup_header()
setup_sidebar_footer()





# # --- Footer Sidebar ---
# st.sidebar.markdown("""
#     <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
#         © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
#     </div>
# """, unsafe_allow_html=True)

# --- Koneksi ke database utama (Tmax, Tmin) ---
db_path1 = "data_lengkap3.db"
table_name1 = "data_lengkap"
conn1 = sqlite3.connect(db_path1)

# --- Ambil semua tanggal unik dari database utama ---
tanggal_list = pd.read_sql_query(
    f"SELECT DISTINCT tanggal FROM {table_name1} ORDER BY tanggal", conn1
)["tanggal"].tolist()

# --- Widget pilih tanggal (default = hari ini) ---
pilih_tanggal = st.date_input(
    "📅 Pilih tanggal:",
    value=date.today(),
    min_value=date(2025, 1, 1),
    max_value=date(2025, 12, 31)
)

# --- Buat Tabs ---
tab1, tab2, tab3 = st.tabs(["Temperatur Minimum", 
                            "Temperatur Maksimum", 
                            "Curah Hujan"])

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

# ===================== TAB 2: Tmin =====================
with tab1:
    # --- Input jumlah baris dinamis ---
    limit_n_tmin = st.slider("Tampilkan berapa stasiun teratas:", min_value=5, max_value=50, value=10, step=5, key="limit_tmin")

    # --- Query data ---
    query_tmin = f"""
    SELECT station_wmo_id, NAME, Tmin
    FROM {table_name1}
    WHERE tanggal = ? AND Tmin IS NOT NULL
    ORDER BY Tmin ASC
    LIMIT {limit_n_tmin}
    """
    df_tmin = pd.read_sql_query(query_tmin, conn1, params=(pilih_tanggal.strftime("%Y-%m-%d"),))

    # --- Judul dinamis ---
    st.subheader(f"{limit_n_tmin} Temperatur Minimum Terendah ({pilih_tanggal.strftime('%d-%m-%Y')})")

    # --- Cek data ---
    if df_tmin.empty:
        st.warning("⚠️ Tidak ada data Tmin untuk tanggal ini.")
    else:
        # Mulai index dari 1
        df_tmin.index = df_tmin.index + 1

        # --- Fungsi pewarnaan ---
        # def highlight_cold(val):
        #     color = 'lightblue' if pd.notnull(val) and val <= 16 else ''
        #     return f'background-color: {color}'

        def highlight_cold(val):
            if pd.notnull(val) and val <= 16:
                return 'background-color: lightblue; font-weight: bold; color: black;'
            else:
                return ''
        
        styled_df = (
            df_tmin.style
            .map(highlight_cold, subset=['Tmin'])
            .format({'Tmin': '{:.1f}'}, na_rep='-')
        )

        # --- Tampilkan tabel ---
        st.dataframe(
            styled_df,
            use_container_width=True,
            column_config={
                "station_wmo_id": st.column_config.Column("WMO ID Stasiun"),
                "NAME": st.column_config.Column("Nama Stasiun"),
                "Tmin": st.column_config.Column("Temperatur Minimum (°C)")
            }
        )
    
    # --- Highlight penjelasan ---
    st.markdown(f"""
    <hr style="border: 1px solid #ccc; margin: 20px 0;">
    <div style='background-color:#e9f2fb; padding:15px; border-radius:10px;'>
    <b>📌 Highlight:</b><br>
    1. Temperatur Minimum merupakan Temperatur Minimum Absolut <br>
    2. Temperatur Minimum dilaporkan pada {pilih_tanggal.strftime("%d %B %Y")} - 00 UTC<br>
    3. Threshold untuk Temperatur Minimum adalah <b>16°C</b>
    </div>
    """, unsafe_allow_html=True)
# ===================== TAB 1: Tmax =====================
with tab2:
    # --- Input jumlah baris ---
    limit_n = st.slider("Tampilkan berapa stasiun teratas:", min_value=5, max_value=50, value=10, step=5)

    # --- Query data ---
    query_tmax = f"""
    SELECT station_wmo_id, NAME, Tmax
    FROM {table_name1}
    WHERE tanggal = ?
    ORDER BY Tmax DESC
    LIMIT {limit_n}
    """
    df_tmax = pd.read_sql_query(query_tmax, conn1, params=(pilih_tanggal.strftime("%Y-%m-%d"),))

    # --- Judul dinamis ---
    st.subheader(f"{limit_n} Temperatur Maksimum Teratas ({pilih_tanggal.strftime('%d-%m-%Y')})")

    # --- Cek ketersediaan data ---
    if df_tmax.empty:
        st.warning("⚠️ Data Tmax belum tersedia untuk tanggal ini (biasanya update sekitar pukul 19.00 WIB).")
    else:
        # Jadikan index mulai dari 1, bukan 0
        df_tmax.index = df_tmax.index + 1

        # Fungsi highlight suhu >= 35
        # def highlight_hot(val):
        #     color = 'lightcoral' if pd.notnull(val) and val >= 35 else ''
        #     return f'background-color: {color}'
        
        def highlight_hot(val):
            if pd.notnull(val) and val >= 35:
                return 'background-color: lightcoral; font-weight: bold; color: black;'
            else:
                return ''

        styled_df = (
            df_tmax.style
            .map(highlight_hot, subset=['Tmax'])
            .format({'Tmax': '{:.1f}'}, na_rep='-')
        )

        # --- Tampilkan tabel ---
        st.dataframe(
            styled_df,
            use_container_width=True,
            column_config={
                "station_wmo_id": st.column_config.Column("WMO ID Stasiun"),
                "NAME": st.column_config.Column("Nama Stasiun"),
                "Tmax": st.column_config.Column("Temperatur Maksimum (°C)")
            }
        )

    # --- Highlight info ---
    st.markdown(f"""
    <hr style="border: 1px solid #ccc; margin: 20px 0;">
    <div style='background-color:#e9f2fb; padding:15px; border-radius:10px;'>
    <b>📌 Highlight:</b><br>
    1. Temperatur Maksimum merupakan Temperatur Maksimum Absolut <br>
    2. Temperatur Maksimum dilaporkan pada {pilih_tanggal.strftime("%d %B %Y")} - 12 UTC<br>
    3. Threshold untuk Temperatur Maksimum adalah <b>35°C</b>
    </div>
    """, unsafe_allow_html=True)


# Tutup koneksi pertama
conn1.close()

# ===================== TAB 3: Curah Hujan =====================
with tab3:
    db_path2 = "data_akhir1.db"
    table_name2 = "data_akhir"
    conn2 = sqlite3.connect(db_path2)

    limit_n_ch = st.slider("Tampilkan berapa stasiun teratas:", min_value=5, max_value=50, value=10, step=5, key="limit_ch")

    query_ch = f"""
    SELECT station_wmo_id, NAME, Curah_Hujan
    FROM {table_name2}
    WHERE tanggal = ?
    ORDER BY Curah_Hujan DESC
    LIMIT {limit_n_ch}
    """
    df_ch = pd.read_sql_query(query_ch, conn2, params=(pilih_tanggal.strftime("%Y-%m-%d"),))
    st.subheader(f"{limit_n_ch} Curah Hujan ({pilih_tanggal.strftime('%d-%m-%Y')})")

    if df_ch.empty:
        st.warning("⚠️ Tidak ada data Curah Hujan untuk tanggal ini.")
    else:
        df_ch.index = df_ch.index + 1
        # def highlight_rain(val):
        #     color = 'grey' if pd.notnull(val) and val >= 50 else ''
        #     return f'background-color: {color}'
        
        def highlight_rain(val):
            if pd.notnull(val) and val >= 50:
                return 'background-color: lightgrey; font-weight: bold; color: red;'
            else:
                return ''

        styled_df = df_ch.style\
            .map(highlight_rain, subset=['Curah_Hujan'])\
            .format({'Curah_Hujan': '{:.1f}'}, na_rep='-')

        #st.dataframe(styled_df, use_container_width=True)
        st.dataframe(
            styled_df,  # DataFrame Anda yang sudah di-style
            use_container_width=True,
            column_config={
                "station_wmo_id": st.column_config.Column("WMO ID Stasiun"),
                "NAME": st.column_config.Column("Nama Stasiun"),
                "Curah_Hujan": st.column_config.Column("Curah Hujan (mm/hari)")
                # Format angka (misal "35.2") sudah diatur oleh .format() Anda,
                # jadi kita hanya perlu mengganti labelnya saja.
            }
        )
    st.markdown(f"""
    <hr style="border: 1px solid #ccc; margin: 20px 0;">
    <div style='background-color:#e9f2fb; padding:15px; border-radius:10px;'>
    <b>📌 Highlight:</b><br>
    1. Curah hujan akumulasi dalam satu hari <br>
    2. Sudah diverifikasi antara seksi 1 dan seksi 3<br>
    3. Threshold untuk Curah Hujan adalah <b>50 mm/hari</b>
    </div>
    """, unsafe_allow_html=True)

    conn2.close()
