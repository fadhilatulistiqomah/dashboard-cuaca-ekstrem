import streamlit as st
import pandas as pd
import base64  # <-- Import modul ini

# st.sidebar.markdown("""
#     <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
#         © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
#     </div>
# """, unsafe_allow_html=True)
from utils.ui import setup_header,setup_sidebar_footer
setup_header()
setup_sidebar_footer()

#st.markdown("### Dokumen Perka 2006")
st.markdown("""
### Dokumen Perka 2006 <span style="font-size:17px; font-style:italic; color:gray;">(sedang dalam tahap pembaruan)</span>
""", unsafe_allow_html=True)

# Link yang Anda berikan
url_perka = "https://web-aviation.bmkg.go.id/storage/files/15183/Perka_BMKG_Nomor_38_Tahun_2006_merged_compressed.pdf"

# Tombol ini akan membuka link di tab browser baru
st.link_button("Perka BMKG No. 38 Tahun 2006 Tentang Tata Cara Tetap Pelaksanaan Pengamatan, Penyandian, dan Pelaporan Hasil Pengamatan Meteorologi Permukaan", url_perka)
st.write("Peraturan Kepala BMKG Nomor 38 Tahun 2006 menetapkan tata cara tetap untuk pelaksanaan pengamatan, penyandian, dan pelaporan hasil pengamatan meteorologi permukaan di seluruh stasiun BMKG. Aturan ini bertujuan menyeragamkan waktu, metode, instrumen, serta format pelaporan agar data yang dihasilkan akurat, konsisten, dan dapat dibandingkan antarstasiun maupun digunakan secara nasional dan internasional. Dengan penerapan standar ini, kualitas data meteorologi meningkat sehingga mendukung analisis cuaca, iklim, serta pelayanan meteorologi lainnya secara lebih andal.")

st.markdown("---")
st.markdown("### Dokumen WMO No. 306")

# Link yang Anda berikan
url_wmo = "https://library.wmo.int/viewer/35713/download?file=306_i1_2019_en.pdf&type=pdf&navigator=1"

# Tombol ini akan membuka link di tab browser baru
st.link_button("Manual on Codes WMO No 306 : Annex II to the WMO Technical Regulations", url_wmo)
st.write("Panduan ini menetapkan kerangka standar alfanumerik yang digunakan oleh World Meteorological Organization untuk pertukaran internasional data meteorologi, iklim, dan hidrologi, sebagai lampiran regulasi teknis; meliputi daftar bentuk kode, simbol-huruf, prosedur pengkodean, dan tabel kode yang memastikan keseragaman dan interoperabilitas antar anggota WMO dalam menyampaikan data secara operasional.")

