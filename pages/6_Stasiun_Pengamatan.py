import streamlit as st
import pandas as pd
# ==============================================================================
# 🚨 KONTROL AKSES: HARUS DI BARIS PALING ATAS
# ==============================================================================
# Cek apakah status autentikasi ada dan bernilai True
if not st.session_state.get('authentication_status'):
    st.error("🔒 Akses Ditolak! Silakan Login di halaman utama.")
    # st.info("Anda akan diarahkan kembali ke halaman utama...")
    # time.sleep(1) # Beri waktu pengguna membaca pesan
    # st.switch_page("app.py") # Opsi untuk mengarahkan kembali secara paksa (opsional)
    st.stop() # Sangat penting: Hentikan eksekusi sisa kode di halaman ini
# ==============================================================================
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
from utils.ui import setup_header,setup_sidebar_footer
setup_header()
setup_sidebar_footer()

#st.title("Data Stasiun")

df=pd.read_excel('Stasiun.xlsx', sheet_name="stasiun_fix")
df.index = df.index + 1
st.dataframe(df, use_container_width=True)


