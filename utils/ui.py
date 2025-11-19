import streamlit as st
import base64

def setup_header():
    # Baca file logo dan ubah ke base64
    with open("Logo_BMKG.png", "rb") as f:
        logo_base64 = base64.b64encode(f.read()).decode()

    st.markdown(f"""
    <style>
        .header-container {{
            display: flex;
            align-items: center;
            background-color: #e9f2fb;
            padding: 4px 14px;
            border-radius: 8px;
            position: sticky;
            top: 0;
            z-index: 999;
        }}
        .header-container img {{
            width: 55px;
            margin-right: 15px;
        }}
        .header-text h4 {{
            margin: 0;
            color: #1f4e79;
            line-height: 1; 
        }}
        .header-text h6 {{
            margin: 0;
            color: gray;
            font-weight: normal;
            line-height: 0.5;
        }}
    </style>

    <div class="header-container">
        <img src="data:image/png;base64,{logo_base64}" alt="BMKG Logo">
        <div class="header-text">
            <h4>Dashboard Monitoring Cuaca Ekstrem</h4>
            <h6>Tim Kerja Manajemen Observasi Meteorologi Permukaan</h6>
        </div>
    </div>
    <hr>
    """, unsafe_allow_html=True)

def setup_sidebar_footer():
    st.sidebar.markdown("""
        <div class="sidebar-footer"
             style="font-size: 12px; color: #666; text-align: center; margin-top: 300px;">
            © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
        </div>
    """, unsafe_allow_html=True)
# def setup_sidebar_footer():
#     st.sidebar.markdown("""
#         <style>
#         /* Pastikan sidebar menjadi container posisi relatif */
#         section[data-testid="stSidebar"] {
#             position: relative;
#         }

#         /* Footer menempel di bawah sidebar */
#         .sidebar-footer {
#             position: fixed;
#             bottom: 10px;          /* jarak dari bawah */
#             left: 0;
#             width: 18rem;          /* sesuaikan lebar sidebar default */
#             text-align: center;
#             font-size: 12px;
#             color: #666;
#             padding: 8px 5px;
#             background: none;
#         }

#         /* Responsif agar tetap pas saat zoom */
#         @media (max-width: 1200px) {
#             .sidebar-footer {
#                 width: 100%;
#                 font-size: 11px;
#             }
#         }
#         </style>

#         <div class="sidebar-footer">
#             © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
#         </div>
#     """, unsafe_allow_html=True)
