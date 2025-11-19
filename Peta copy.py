import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from folium import CustomIcon
import sqlite3
from datetime import date, timedelta

# --- Page config harus di paling atas ---
st.set_page_config(layout="wide")

# ==========================================================
# 🧭 1️⃣ KONFIGURASI HALAMAN & SIDEBAR DENGAN LOGO
# ==========================================================
st.set_page_config(page_title="🌦️ Peta Cuaca Ekstrem", layout="wide")

# --- CSS styling untuk sidebar ---
st.markdown("""
    <style>
        [data-testid="stSidebar"] {
            background-color: #f7f9fb;
            padding-top: 0px;
        }
        .sidebar-title {
            font-size: 20px;
            font-weight: 700;
            color: #004d80;
            margin-top: 10px;
            margin-bottom: 0px;
        }
        .divider {
            border-top: 2px solid #cccccc;
            margin-top: 8px;
            margin-bottom: 12px;
        }
        .sidebar-footer {
            font-size: 12px;
            color: #666666;
            text-align: center;
            margin-top: 60px;
        }
    </style>
""", unsafe_allow_html=True)
st.sidebar.image("Logo_BMKG.png", caption="Peta Cuaca Ekstrem")
# # --- Bagian logo dan judul di sidebar ---
# st.sidebar.markdown("""
#     <div style="text-align: center;">
#         <img src="C:/Users/Lenovo/Downloads/project/wind.png" width="100">
#         <p class="sidebar-title">Peta Cuaca Ekstrem</p>
#         <div class="divider"></div>
#     </div>
# """, unsafe_allow_html=True)

# --- Menu navigasi (opsional) ---
#menu = st.sidebar.radio("Navigasi", ["🗺️ Peta", "📋 Data"])

# --- Footer sidebar ---
st.sidebar.markdown("""
    <div class="sidebar-footer">
        © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
    </div>
""", unsafe_allow_html=True)




# --- Koneksi ke database SQLite ---
db_path = "data_akhir.db"
table_name = "data_akhir"

conn = sqlite3.connect(db_path)

# Ambil semua tanggal unik
tanggal_list = pd.read_sql_query(
    f"SELECT DISTINCT tanggal FROM {table_name} ORDER BY tanggal", conn
)["tanggal"].tolist()

# Widget pilih tanggal
pilih_tanggal = st.date_input(
    "📅 Pilih tanggal:",
    value=date(2025, 1, 1),
    min_value=date(2025, 1, 1),
    max_value=date(2025,12, 31)
)

# Tanggal pertama di database
tanggal_pertama = tanggal_list[0]

if pilih_tanggal == tanggal_pertama:
    # Kalau hari pertama → tidak ambil jam 00
    query = f"""
    SELECT station_wmo_id, NAME, LAT, LON, Temperatur, Curah_Hujan, 
           Kecepatan_angin, Gale, Heavy_Rain, jam, sandi_gts, tanggal
    FROM {table_name}
    WHERE tanggal = ?
      AND jam != '00:00'
    ORDER BY station_wmo_id, jam
    """
    df = pd.read_sql_query(query, conn, params=(pilih_tanggal,))
else:
    # Selain hari pertama → ambil semua jam + 00:00 hari berikutnya
    query = f"""
    SELECT station_wmo_id, NAME, LAT, LON, Temperatur, Curah_Hujan, 
           Kecepatan_angin, Gale, Heavy_Rain, jam, sandi_gts, tanggal
    FROM {table_name}
    WHERE tanggal = ?
       OR (tanggal = date(?, '+1 day') AND jam = '00:00')
    ORDER BY station_wmo_id, jam
    """
    df = pd.read_sql_query(query, conn, params=(pilih_tanggal, pilih_tanggal))

conn.close()

# --- Judul ---
st.markdown("<h1 style='margin-top: -30px;'>Peta Cuaca Ekstrem</h1>", unsafe_allow_html=True)

# Buat peta
m = folium.Map(location=[-2, 118], zoom_start=5, tiles=None)

# Tile Carto Voyager
folium.TileLayer(
    tiles='https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
    attr='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> contributors &copy; <a href="https://carto.com/">CARTO</a>',
    name="Carto Voyager",
    overlay=False,
    control=False
).add_to(m)

# Layer khusus
gale_layer = folium.FeatureGroup(name="Gale")
hr_layer   = folium.FeatureGroup(name="Heavy Rain")

# Loop data
for _, row in df.iterrows():
    lat, lon = row["LAT"], row["LON"]

    wind_text = "Calm" if pd.isna(row['Kecepatan_angin']) else f"{row['Kecepatan_angin']} knot"
    rain_text = "-" if pd.isna(row['Curah_Hujan']) else f"{row['Curah_Hujan']} mm"

    if pd.notna(lat) and pd.notna(lon):
        # --- Marker default stasiun ---
        folium.CircleMarker(
            location=[lat, lon],
            radius=4,
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

        # --- Gale ---
        try:
            gale_value = float(row["Gale"])
            if gale_value > 0:
                wind_icon = CustomIcon(
                    icon_image="C:/Users/Lenovo/Downloads/project/wind.png",
                    icon_size=(30, 30))
                folium.Marker(
                    location=[lat, lon],
                    icon=wind_icon,
                    popup=(
                        f"<div style='font-size:10px;width:200px;'>"
                        f"<b>{row['NAME']}</b><br>{row['station_wmo_id']}<br>💨 Gale: {gale_value} knot"
                        f"</div>"
                    )
                ).add_to(gale_layer)
        except:
            pass

        # --- Heavy Rain ---
        try:
            hr_value = float(row["Heavy_Rain"])
            if hr_value > 0:
                rain_icon = CustomIcon(
                    icon_image="C:/Users/Lenovo/Downloads/project/cloud_rain.png",
                    icon_size=(30, 30))
                folium.Marker(
                    location=[lat, lon],
                    icon=rain_icon,
                    popup=(
                        f"<div style='font-size:10px;width:200px;'>"
                        f"<b>{row['NAME']}</b><br>{row['station_wmo_id']}<br>🌧️ Heavy Rain: {hr_value} mm"
                        f"</div>"
                    )
                ).add_to(hr_layer)
        except:
            pass

# Tambahkan layer
gale_layer.add_to(m)
hr_layer.add_to(m)
folium.LayerControl().add_to(m)

# Tampilkan peta di Streamlit
st_folium(m, width=None, height=500)

# --- Highlight / Keterangan ---
tanggal_sebelumnya = pilih_tanggal - timedelta(days=1)

st.markdown(f"""
<hr style="border: 1px solid #ccc; margin: 20px 0;">
<div style='background-color:#f9f9f9; padding:15px; border-radius:10px;'>
<b>📌 Highlight:</b><br>
1. Data yang ditampilkan merupakan data dari 01–00 UTC (<b>{tanggal_sebelumnya.strftime("%d %B %Y")}</b>)<br>
2. Threshold untuk Heavy Rain adalah <b>50 mm</b><br>
3. Threshold untuk Gale adalah <b>30 knot</b>
</div>
""", unsafe_allow_html=True)

# --- Tabel Heavy Rain ---
st.markdown("### 🌧️ Daftar Stasiun dengan Heavy Rain")

# Filter hanya stasiun dengan Heavy_Rain > 0
df_hr = df.copy()
df_hr["Heavy_Rain"] = pd.to_numeric(df_hr["Heavy_Rain"], errors="coerce")
df_hr = df_hr[df_hr["Heavy_Rain"] > 0]

if not df_hr.empty:
    df_tabel = df_hr[[
        "station_wmo_id", "NAME", "tanggal", "jam", 
        "LAT", "LON", "Temperatur", "Curah_Hujan", 
        "Heavy_Rain", "sandi_gts"
    ]].sort_values(by=["Heavy_Rain"], ascending=False)

    st.dataframe(df_tabel, use_container_width=True)
else:
    st.info("Tidak ada stasiun dengan kejadian Heavy Rain pada tanggal yang dipilih.")



















import streamlit as st
import pandas as pd
import folium
from folium import plugins
from streamlit_folium import st_folium
import sqlite3
# 🔽 [MODIFIKASI] Import 'element' dari branca untuk menambahkan HTML kustom
import branca.element as element

# --- Konfigurasi halaman ---
st.set_page_config(page_title="Peta Frekuensi Heavy Rain", layout="wide")

from utils.ui import setup_header
setup_header()

st.sidebar.markdown("""
    <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
        © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
    </div>
""", unsafe_allow_html=True)
#st.title("Total Kejadian Bulanan")
# --- Koneksi ke database ---
db_path = "data_akhir.db"  # ganti sesuai file kamu
conn = sqlite3.connect(db_path)

# --- Ambil data ---
query = "SELECT station_wmo_id, NAME, LAT, LON, Heavy_Rain, tanggal FROM data_akhir"
df = pd.read_sql_query(query, conn)
conn.close()

# --- Pastikan kolom tanggal benar ---
df["tanggal"] = pd.to_datetime(df["tanggal"], errors="coerce")
df["bulan"] = df["tanggal"].dt.to_period("M").astype(str)

# --- Pilih bulan ---
bulan_tersedia = sorted(df["bulan"].dropna().unique())
bulan_pilih = st.selectbox("Pilih Bulan", bulan_tersedia)

# --- Filter data per bulan ---
df_bulan = df[df["bulan"] == bulan_pilih]

# --- Ambil hanya baris dengan hujan (Heavy_Rain > 0) ---
df_hujan = df_bulan[df_bulan["Heavy_Rain"].fillna(0) > 0]

# --- Hitung frekuensi kejadian per stasiun ---
frekuensi_stasiun = (
    df_hujan.groupby(["station_wmo_id", "NAME", "LAT", "LON"])
    .size()
    .reset_index(name="jumlah_kejadian")
)

# --- Total nasional ---
total_kejadian = frekuensi_stasiun["jumlah_kejadian"].sum()

# --- Hitung persentase kontribusi tiap stasiun ---
frekuensi_stasiun["persentase"] = (
    frekuensi_stasiun["jumlah_kejadian"])# / total_kejadian * 100
#).round(2)


# ==============================
# 🔸 1. Definisikan Warna dan Batas Nilai
# ==============================

batas_nilai = [0, 1, 2, 3, 4, 5, 6, 7,8]
warna_diskret = [
    "#f6b11d", "#fedc21", "#fcfd5b", "#cefd9c", "#97e0a4",
    "#05c6a3", "#0c9b9a", "#016f9f", "#033b57"
]
# Label yang akan ditampilkan di bawah color bar
labels = ["1", "2", "3", "4", "5", "6", "7","8"]

# ==============================
# 🔸 2. Buat Fungsi Pewarnaan Diskrit
# ==============================
# Fungsi ini akan menggantikan 'cmap(val)' untuk memastikan warna yang dipilih diskrit
def get_color_for_value(value):
    if value < 1: return warna_diskret[0]
    if value < 2: return warna_diskret[1]
    if value < 3: return warna_diskret[2]
    if value < 4: return warna_diskret[3]
    if value < 5: return warna_diskret[4]
    if value < 6: return warna_diskret[5]
    if value < 7: return warna_diskret[6]
    if value < 8: return warna_diskret[7]
    return warna_diskret[8]


# --- Buat peta ---
peta = folium.Map(location=[-2, 118], zoom_start=5, tiles="CartoDB Voyager")

# --- Tambahkan titik ---
for _, row in frekuensi_stasiun.iterrows():
    val = row["persentase"]
    # Gunakan fungsi baru untuk mendapatkan warna
    warna = get_color_for_value(val)
    popup_text = f"""
    <b>Stasiun:</b> {row['NAME']}<br>
    <b>Kejadian Heavy Rain:</b> {row['jumlah_kejadian']} kali<br>
    """
    folium.CircleMarker(
        location=[row["LAT"], row["LON"]],
        radius=7,
        color=warna,
        fill=True,
        fill_color=warna,
        fill_opacity=0.9,
        popup=folium.Popup(popup_text, max_width=300)
    ).add_to(peta)

# =================================================================
# 🔸 3. Buat Legenda Horizontal Kustom dengan HTML dan CSS (VERSI FINAL)
# =================================================================
# [MODIFIKASI] Ganti seluruh blok 'legend_html' Anda dengan yang ini.

# Lebar setiap blok warna dan ujungnya
block_width = 45  # Lebar satu blok warna dalam pixel
tip_width = 10    # Lebar satu ujung runcing dalam pixel

# [PERUBAHAN] Kita hanya akan membuat balok untuk warna di antara warna pertama dan terakhir
warna_balok = warna_diskret[1:-1]
lebar_balok_total = len(warna_balok) * block_width
lebar_legenda_total = lebar_balok_total + (2 * tip_width)

legend_html = f'''
<div style="
    position: fixed;
    bottom: 20px;
    left: 18%;
    transform: translateX(-50%);
    background-color: rgba(255, 255, 255, 0.9);
    border: 1px solid #bbb;
    border-radius: 8px;
    padding: 10px 15px;
    z-index: 9999;
    font-family: Arial, sans-serif;
    font-size: 12px;
    text-align: center;
    box-shadow: 0 0 10px rgba(0,0,0,0.2);
">
    <span style="font-weight: bold; display: block; margin-bottom: 8px;">Total Kejadian Heavy Rain</span>
    
    <div style="display: flex; flex-direction: column; align-items: center;">
    
        <div style="display: flex; align-items: center; margin-bottom: 2px;">
            <div style="
                width: 0; height: 0;
                border-top: 10px solid transparent;
                border-bottom: 10px solid transparent;
                border-right: {tip_width}px solid {warna_diskret[0]};
            "></div>
            {''.join([f'<div style="background-color: {color}; width: {block_width}px; height: 20px;"></div>' for color in warna_balok])}
            <div style="
                width: 0; height: 0;
                border-top: 10px solid transparent;
                border-bottom: 10px solid transparent;
                border-left: {tip_width}px solid {warna_diskret[-1]};
            "></div>
        </div>

        <div style="
            display: flex;
            justify-content: space-between;
            width: {lebar_legenda_total}px; /* LEBAR DISESUAIKAN DENGAN TOTAL (BALOK + UJUNG) */
        ">
            {''.join([f'<span>{label}</span>' for label in labels])}
        </div>
        
    </div>
</div>
'''

# Menambahkan elemen HTML ke peta
peta.get_root().html.add_child(element.Element(legend_html))


# --- Tampilkan ---
#st.subheader(f"Total kejadian Heavy Rain di seluruh Indonesia: {int(total_kejadian)} kali")
st.write(f"Total kejadian Heavy Rain di seluruh Indonesia: {int(total_kejadian)} kali")

# --- TAMBAHKAN BLOK KODE INI ---
plugins.Fullscreen(
    position="topright",  # Bisa 'topleft', 'topright', 'bottomleft', 'bottomright'
    title="Lihat Fullscreen", # Teks saat mouse hover
    title_cancel="Keluar Fullscreen", # Teks saat mouse hover (dalam mode fullscreen)
    force_separate_button=True,
).add_to(peta)

#st_folium(peta, width=1100, height=600)
st_folium(peta, width=None, height=500)