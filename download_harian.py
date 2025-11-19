import requests
import urllib3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from datetime import date, datetime, timedelta
import sqlite3
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

# Hilangkan warning SSL (karena verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =======================================
# 3️⃣ Konfigurasi login & tanggal data
# =======================================
USERNAME = "pusmetbang"       # ganti dengan username BMKG Satu kamu
PASSWORD = "oprpusmetbang"    # ganti dengan password BMKG Satu kamu
TANGGAL = str(date.today())   # otomatis ambil hari ini (format YYYY-MM-DD)
#TANGGAL = "2025-10-27"        # contoh tanggal

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

    url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu//@search"
    params = {
        "type_name": "GTSMessage",
        "_metadata": "type_message,timestamp_data,timestamp_sent_data,station_wmo_id,sandi_gts,ttaaii,cccc,need_ftp",
        "_size": "10000",
        "type_message": "1",
        # mulai jam 01 tanggal X
        "timestamp_data__gte": f"{tgl_awal.strftime('%Y-%m-%d')}T01:00:00",
        # sampai jam 00:59 tanggal X+1
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
# 6️⃣ Jalankan proses
# =======================================
try:
    token = ambil_token(USERNAME, PASSWORD)
    data = ambil_data_gts(TANGGAL, token)

    # pastikan ada data
    if "items" not in data:
        raise ValueError("❌ Data kosong atau format tidak sesuai")

    # ambil hanya kolom yang diperlukan
    df = pd.DataFrame(data["items"])[[
        "timestamp_data",
        "timestamp_sent_data",
        "station_wmo_id",
        "ttaaii",
        "cccc",
        "sandi_gts"
    ]]

    print("✅ Data berhasil dimuat ke DataFrame")
    print(df.head())  # tampilkan 5 baris pertama

except Exception as e:
    print(e)


df['timestamp_data'] = pd.to_datetime(df['timestamp_data'], errors='coerce')
df['timestamp_sent_data'] = pd.to_datetime(df['timestamp_sent_data'], errors='coerce')

# Format ulang supaya semua ada microseconds
df['timestamp_data'] = df['timestamp_data'].dt.strftime("%Y-%m-%dT%H:%M:%S")
df['timestamp_sent_data'] = df['timestamp_sent_data'].dt.strftime("%Y-%m-%dT%H:%M:%S")

# Urutkan agar timestamp_sent_data terbaru berada di atas
data_sorted = df.sort_values(['station_wmo_id','timestamp_data', 'timestamp_sent_data'], ascending=[True, True, False])

# Ambil satu data per timestamp_data, yang paling baru dikirim
data = data_sorted.drop_duplicates(subset=['station_wmo_id', 'timestamp_data'], keep='first')


def ambil_aaxx_beserta_isi(teks):
    match = re.search(r'(AAXX\s.*?\b333\b)', teks, re.DOTALL)
    return match.group(1).strip() if match else None

data['seksi01'] = data['sandi_gts'].apply(ambil_aaxx_beserta_isi)
#data['seksi01'] = data['seksi01'].str.replace(r'\s+', ' ', regex=True).str.strip()

data['seksi01'] = (
    data['seksi01']
    .str.replace(r'CCA', '', regex=True)             # hapus semua CCA
    .str.replace(r'CCB', '', regex=True)             # hapus semua CCA
    .str.replace(r'\s+', ' ', regex=True)            # rapikan spasi
    .str.strip()                                     # hilangkan spasi awal/akhir
)

data['seksi0'] = pd.DataFrame(data.seksi01.astype(str).apply(lambda x: x[0:16] ))

data['seksi1'] = data.apply(
    lambda row: str(row['seksi01']).split(str(row['station_wmo_id']), 1)[-1].strip()
    if pd.notna(row['station_wmo_id']) and str(row['station_wmo_id']) in str(row['seksi01'])
    else None,
    axis=1)

#data['iihvv'] = pd.DataFrame(data.seksi1.astype(str).apply(lambda x: x[0:5] if len(x) > 20 else None))
data['iihvv'] = data['seksi1'].astype(str).apply(
    lambda x: x[0:5] if len(x) > 20 and x[0] in ['0', '1', '2', '3', '4'] else None
)

# --- Ambil nddff: 5 digit setelah iihvv ---
def extract_nddff(teks, iihvv):
    try:
        if not isinstance(teks, str) or not isinstance(iihvv, str):
            return None
        #pattern = re.escape(iihvv) + r'\s*(\d{5})(?=\s1)'  # cari 5 digit setelah iihvv, diikuti spasi1
        pattern = re.escape(iihvv) + r'\s*/?\s*(\d{4,5})(?=\s1)'
        match = re.search(pattern, teks)
        return match.group(1) if match else None
    except:
        return None

data['nddff'] = data.apply(lambda row: extract_nddff(row['seksi1'], row['iihvv']), axis=1)
# def extract_nddff(x):
#     x = str(x)
#     match = re.search(r'(\d{5})(?=\s1)', x)  # cari 5 digit diikuti " spasi1"
#     return match.group(1) if match else None

# data['nddff'] = data['seksi1'].apply(extract_nddff)

# --- Ekstrak wd dan ws dari kolom nddff ---
# data['wd'] = data['nddff'].astype(str).apply(lambda x: x[1:3] if len(x) >= 3 else None)
# data['wd'] = data['wd'].astype(str).str.zfill(2)

# data['ws'] = data['nddff'].astype(str).apply(lambda x: x[3:5] if len(x) >= 5 else None)
# data['ws'] = data['ws'].astype(str).str.zfill(2)
# konversi ke string agar tidak error
data['nddff'] = data['nddff'].astype(str)

# ambil arah (dd) dari posisi 1-3, kecepatan (ff) dari 3-5
data['wd'] = data['nddff'].apply(lambda x: x[1:3] if len(x) >= 3 and x[1:3] != "//" else None)
data['ws'] = data['nddff'].apply(lambda x: x[3:5] if len(x) >= 5 and x[3:5] != "//" else None)

# tambahkan zero-padding agar dua digit
data['wd'] = data['wd'].astype(str).str.zfill(2)
data['ws'] = data['ws'].astype(str).str.zfill(2)

# --- Fungsi interpretasi Arah Angin ---
def interpret_wd(row):
    try:
        wd = int(row['wd'])
        ws = int(row['ws'])
        
        # Kasus khusus: tidak ada angin
        if wd == 0 and ws == 0:
            return None  
        
        # Kasus arah utara (ada angin tapi wd = 00)
        if wd == 0 and ws > 0:
            return 0  
        
        # Normal: kode wd dikali 10 derajat
        if 1 <= wd <= 36:
            return wd * 10
        
        return None
    except:
        return None


# --- Fungsi interpretasi Kecepatan Angin ---
def interpret_ws(ws):
    try:
        ws = int(ws)
        if ws == 0:
            return None
        elif 1 <= ws <= 99:
            return ws
        else:
            return None
    except:
        return None


# --- Fungsi interpretasi Gale (angin kencang) ---
def interpret_gale(ws):
    try:
        ws = int(ws)
        if ws >= 30:
            return ws
        else:
            return None
    except:
        return None


# --- Terapkan ke DataFrame ---
data['Arah_angin'] = data.apply(interpret_wd, axis=1)
data['Kecepatan_angin'] = data['ws'].apply(interpret_ws)
data['Gale'] = data['ws'].apply(interpret_gale)

def ambil_seksi1_1(teks):
    teks = str(teks).replace('\n', ' ').strip()
    match = re.search(r'(1[0-9/]{4}\s2[0-9/]{4}.*)', teks)
    if match:
        return match.group(1).strip()
    return None
data['seksi1_1'] = data['seksi1'].apply(ambil_seksi1_1)

# Loop otomatis dari sandi1 sampai sandi8
for i in range(1, 9):
    # Regex: angka pertama = nomor sandi, 4 karakter berikut boleh angka atau '/'
    regex = fr'({i}[0-9/]{{4}})'
    data[f'sandi{i}'] = data['seksi1_1'].astype(str).str.extract(regex, expand=False)

def interpret_ttt(sandi1):
    try:
        sandi1 = str(sandi1).strip()
        if len(sandi1) < 5:
            return None
        
        # Ambil substring TTT (3 digit terakhir dari karakter ke-2 sampai ke-4)
        ttt_str = sandi1[2:5]
        
        # Pastikan cukup panjang
        if len(ttt_str) != 3:
            return None
        
        # Angka kedua dari sandi1 menentukan tanda (0 = positif, 1 = negatif)
        sign_digit = sandi1[1]
        value = int(ttt_str)
        suhu = value / 10
        
        if sign_digit == '0':
            return suhu
        elif sign_digit == '1':
            return -suhu
        else:
            return None
    except:
        return None

data['Temperatur'] = data['sandi1'].apply(interpret_ttt)

def interpret_tdtdtd(sandi2):
    try:
        sandi2 = str(sandi2).strip()
        if len(sandi2) < 5:
            return None
        
        # Ambil 3 digit terakhir sebagai TdTdTd
        tdtdtd_str = sandi2[2:5]
        
        if len(tdtdtd_str) != 3:
            return None
        
        # Angka kedua dari sandi2 menentukan tanda (0 = positif, 1 = negatif)
        sign_digit = sandi2[1]
        value = int(tdtdtd_str)
        suhu = value / 10
        
        if sign_digit == '0':
            return suhu
        elif sign_digit == '1':
            return -suhu
        else:
            return None
    except:
        return None

data['Dew_Point'] = data['sandi2'].apply(interpret_tdtdtd)       

def interpret_qfe(sandi3):
    try:
        sandi3 = str(sandi3).strip()
        if len(sandi3) < 5:
            return None

        # Ambil 4 digit PoPoPoPo (mulai dari karakter ke-2)
        popopopo = sandi3[1:5]
        qfe_int = int(popopopo)
        qfe_str = popopopo.zfill(4)

        # Aturan konversi
        if qfe_str.startswith('0'):
            return (qfe_int / 10) + 1000
        else:
            return qfe_int / 10
    except:
        return None

data['Tekanan_Permukaan'] = data['sandi3'].apply(interpret_qfe)

def interpret_qff(sandi4):
    try:
        sandi4 = str(sandi4).strip()
        if len(sandi4) < 5:
            return None

        # Ambil 4 digit PPPP (mulai dari karakter ke-2)
        pppp = sandi4[1:5]
        qff_int = int(pppp)
        qff_str = pppp.zfill(4)

        # Aturan konversi tekanan
        if qff_str.startswith('0'):
            return (qff_int / 10) + 1000
        else:
            return qff_int / 10
    except:
        return None

data['Tekanan_Laut'] = data['sandi4'].apply(interpret_qff)      

def interpret_ppp(sandi5):
    try:
        sandi5 = str(sandi5).strip()
        if len(sandi5) < 5:
            return None

        # Ambil 3 digit PPP (mulai dari karakter ke-3)
        ppp = sandi5[2:5]
        ppp_str = ppp.zfill(3)
        ppp_int = int(ppp_str)

        # Interpretasi nilai selisih tekanan
        return ppp_int / 10
    except:
        return None

data['Selisih_Tekanan'] = data['sandi5'].apply(interpret_ppp)

def interpret_rain(sandi6):
    try:
        sandi6 = str(sandi6).strip()
        
        # Validasi format: harus diawali '6' dan diakhiri '4'
        if not (len(sandi6) >= 5 and sandi6[0] == '6' and sandi6[4] == '4'):
            return None
        
        # Ambil 3 digit curah hujan (ch)
        ch_str = sandi6[1:4].zfill(3)
        ch = int(ch_str)
        
        # Interpretasi sesuai kode
        if 1 <= ch < 990:
            return ch               # Curah hujan dalam 0.1 mm
        elif ch == 990:
            return None             # Tidak ada data
        elif 991 <= ch <= 999:
            return (ch - 990) / 10  # Dalam satuan mm (kode 991–999)
        else:
            return None
    except:
        return None

data['Curah_Hujan'] = data['sandi6'].apply(interpret_rain)


def interpret_heavy_rain(ch):
    try:
        if ch is None:
            return None
            ch = float(ch)
        if ch >= 50:
            return ch
            return None
    except:
        return None

data['Heavy_Rain'] = data['Curah_Hujan'].apply(interpret_heavy_rain)     

data['ww'] = pd.DataFrame(data.sandi7.astype(str).apply(lambda x: x[1:3] if len(x) >= 3 and x[0]=='7' else None))

data['ww'] = data['ww'].astype(str).str.zfill(2)
mapping_df = pd.read_excel('ww.xlsx')
mapping_df['kode'] = mapping_df['kode'].astype(str).str.zfill(2)
mapping_dict = mapping_df.set_index('kode')['interpretasi'].to_dict()
data['ww_interpretasi'] = data['ww'].map(mapping_dict)

data['W1'] = pd.DataFrame(data.sandi7.astype(str).apply(lambda x: x[3:4] if len(x) >= 4 and x[0]=='7' else None))

def interpret_w(W1):
    try:
        W1 = int(W1)
        if W1 == 0:
            return 'Awan menutupi langit setengah atau kurang selama jangka waktu yang ditentukan'
        elif W1 == 1 :
            return 'Awan menutupi langit lebih dari setengah selama sebagian dari jangka waktu yang ditetaokan dan setengah atau kurang selama sebagian dari jangka waktu itu'
        elif W1 == 2 :
            return 'Awan menutupi langit lebih dari setengah selama jangka waktu yang ditetapkan'
        elif W1 == 3 :
            return 'Badai pasir, badai debu, atau salju hembus'
        elif W1 == 4 :
            return 'Kabut atau kekaburan tebal'
        elif W1 == 5 :
            return 'Drizzlo'
        elif W1 == 6 :
            return 'Hujan'
        elif W1 == 7 :
            return 'Salju atau hujan bercampur salju'
        elif W1 == 8 :
            return 'Hujan tiba-tiba (Showers)'
        elif W1 == 9 :
            return 'Badai guntur disertai endapan atau tidak disertai endapan'
    except:
        return None

data['W1_interpretasi'] = data['W1'].apply(interpret_w)
data['W2'] = pd.DataFrame(data.sandi7.astype(str).apply(lambda x: x[4:5] if len(x) >= 5 and x[0]=='7' else None))
data['W2_interpretasi'] = data['W2'].apply(interpret_w)

data['Nh'] = pd.DataFrame(data.sandi8.astype(str).apply(lambda x: x[1:2] if len(x) >= 3 and x[0]=='8' else None))

data['Nh'] = data['Nh'].astype(str).str.zfill(1)

def interpret_cloudL(C):
    try:
        C = int(C)
        if C == 0:
            return 'Tidak ada awan'
        elif C == 1 :
            return 'Cumulus humilis atau fracto cumulus atau kedua-duanya'
        elif C == 2 :
            return 'Cumulus mediocris atau congestus, disertai atau tidak disertai fracto cumulus atau humilis atau strato cumulus, dengan tinggi dasar sama'
        elif C == 3 :
            return 'Cumulunimbus tanpa landasan, disertai atau tidak disertai cumulus, strato cumulus atau stratus'
        elif C == 4 :
            return 'Stratocumulus yang terjadi dari bentangan cumulus'
        elif C == 5 :
            return 'Stratocumulus yang tidak terjadi dari bentangan cumulus'
        elif C == 6 :
            return 'Stratus'
        elif C == 7 :
            return 'Fraktotratus atau fraktocumulus yang menyertai cuaca buruk, biasanya di bawah As atau Ns '
        elif C == 8 :
            return 'Cumulus dan stratocumulus yang tidak terjadi dari bentangan cumulus, dengan tinggi dasar berlainan'
        elif C == 9 :
            return 'Cumulunimbus, biasanya berlandaskan disertai cumulus, stratocumulus, stratus, cumulunimbus yang tidak berlandaskan'
    except:
        return 'Tidak terlihat'
    

def interpret_cloudM(C):
    try:
        C = int(C)
        if C == 0:
            return 'Tidak ada awan'
        elif C == 1 :
            return 'Altostratus tipis'
        elif C == 2 :
            return 'Altostratus tebal atau nimbostratus'
        elif C == 3 :
            return 'Altocumulus tipis dalam suatu lapisan '
        elif C == 4 :
            return 'Altocumulus tipis berbentuk terpisah-pisah, sering sekali berbentuk lensa, terus berubah dan terdapat pada satu lapisan atau lebih'
        elif C == 5 :
            return 'Altocumulus tipis berbentuk pias-pias atau beberapa lapisan altocumulus tipis atau tebal dalam keadaan bertambah '
        elif C == 6 :
            return 'Altocumulus yang terjadi dari bentangan cumulus'
        elif C == 7 :
            return 'Altocumulus tipis atau tebal dalam beberapa lapisan, atau satu lapisan altocumulus tebal, tidak dalam keadaan bertambah, atau altocumulus serta altostratus atau nimbostratus'
        elif C == 8 :
            return 'Altocumulus castellatus (bertanduk) atau berbentuk bayangan bintik '
        elif C == 9 :
            return 'Altocumulus dalam berbagai-bagai lapisan dan bentuk, kelihatan tidak teratur'
    except:
        return 'Tidak terlihat'

def interpret_cloudH(C):
    try:
        C = int(C)
        if C == 0:
            return 'Tidak ada awan'
        elif C == 1 :
            return 'Cirrus halus seperti bulu ayam, tidak dalam keadaan bertambah '
        elif C == 2 :
            return 'Cirrus padat, terpisah-pisah atau masa yang kusut, biasanya tidak bertambah, kadang-kadang seperti sisa-sisa landasan cumulunimbus'
        elif C == 3 :
            return 'Cirrus padat, terjadi dari landasan cumulunimbus '
        elif C == 4 :
            return 'Cirrus halus dalam bentuk koma, atau bulu ayam, menjadi lebih padat atau bertambah'
        elif C == 5 :
            return 'Cirrus dan cirrostratus, cirrostratus sendirian, dalam keadaan bertambah akan tetapi lapisan tidak mencapai ketinggian 45o di atas cakrawala'
        elif C == 6 :
            return 'Cirrus dan cirrostratus, atau cirrostratus sendirian, menjadi lebih padat dan dalam keadaan bertambah, lapisan meluas lebih dari 45o di atas cakrawala akan tetapi langit tidak tertutup semuanya '
        elif C == 7 :
            return 'Lapisan cirrostratus yang menutupi seluruh langit '
        elif C == 8 :
            return 'Cirrostratus yang tidak menutupi seluruh langit dan tidak bertambah'
        elif C == 9 :
            return 'Cirrocumulus, cirrocumulus yang terbanyak dengan sedikit cirrus dan / atau cirrostratus'
    except:
        return 'Tidak terlihat'
    

def awan_rendah(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('8') and len(t) >= 3:
            return interpret_cloudL(t[2])  # ambil angka ke-3 dan interpretasikan
    return None
data['CL'] = data['sandi8'].apply(awan_rendah)

def awan_menengah(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('8') and len(t) >= 3:
            return interpret_cloudM(t[3])  # ambil angka ke-3 dan interpretasikan
    return None
data['CM'] = data['sandi8'].apply(awan_menengah)

def awan_tinggi(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('8') and len(t) >= 3:
            return interpret_cloudH(t[4])  # ambil angka ke-3 dan interpretasikan
    return None
data['CH'] = data['sandi8'].apply(awan_tinggi) 

def ambil_setelah_333(teks):
    if not isinstance(teks, str):
        return None
    match = re.search(r'333\s+(.*?)=', teks, re.DOTALL)
    return match.group(1).strip() if match else None

data['seksi3'] = data['sandi_gts'].apply(ambil_setelah_333)
data['seksi3'] = data['seksi3'].str.replace(r'\s+', ' ', regex=True).str.strip()

# --- Ambil sandi 2 (TnTnTn) ---
def ambil_sandi1(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('1'):
            return t
    return None

data['sn1'] = data['seksi3'].apply(ambil_sandi1)

#data['TxTxTx'] = pd.DataFrame(data.sn1.astype(str).apply(lambda x: x[2:5] if len(x) >= 4  else None))

def interpret_tmax(sandi7):
    try:
        sandi7 = str(sandi7).strip()
        if len(sandi7) < 5 or sandi7[0] != '1':
            return None
        
        # Ambil 3 digit suhu maksimum (TxTxTx)
        tx_str = sandi7[2:5]
        
        # Ambil tanda (0 = positif, 1 = negatif)
        sign_digit = sandi7[1]
        value = int(tx_str)
        suhu = value / 10

        if sign_digit == '0':
            return suhu
        elif sign_digit == '1':
            return -suhu
        else:
            return None
    except:
        return None

data['Tmax'] = data['sn1'].apply(interpret_tmax)

# --- Ambil sandi 2 (TnTnTn) ---
def ambil_sandi2(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('2'):
            return t
    return None

data['sn2'] = data['seksi3'].apply(ambil_sandi2)

def interpret_tmin(sn2):
    try:
        sn2 = str(sn2).strip()
        if len(sn2) < 5 or sn2[0] != '2':
            return None

        # Ambil tiga digit suhu minimum (TnTnTn)
        tn_str = sn2[2:5]

        # Ambil tanda (0 = positif, 1 = negatif)
        sign_digit = sn2[1]
        value = int(tn_str)
        suhu = value / 10

        if sign_digit == '0':
            return suhu
        elif sign_digit == '1':
            return -suhu
        else:
            return None
    except:
        return None

data['Tmin'] = data['sn2'].apply(interpret_tmin)

def ambil_sandi53(teks):
    teks = str(teks).replace('\n', ' ')
    match = re.search(r'2[0-9/]{4}\s(5[0-46-9/][0-9/]{3})', teks)
    if match:
        return match.group(1)
    return None

data['sandi53'] = data['seksi3'].apply(ambil_sandi53)

# Ekstrak EEE dari sandi 5EEEiE
data['Evaporasi'] = data['sandi53'].apply(
    lambda x: x[1:4] if isinstance(x, str) and len(x) >= 5 and x.startswith('5') else None
)

# Konversi ke numerik aman
data['Evaporasi'] = pd.to_numeric(data['Evaporasi'], errors='coerce')

# Skala (dibagi 10)
data['Evaporasi'] = data['Evaporasi'] / 10

# --- Ambil sandi 55 ---
def ambil_sandi55(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('55'):
            return t
    return None

data['sandi55'] = data['seksi3'].apply(ambil_sandi55)

def interpret_lama_penyinaran(sandi55):
    try:
        # Validasi awal: harus string, diawali '55', dan panjang minimal 4
        if not (isinstance(sandi55, str) and sandi55.startswith('55') and len(sandi55) >= 4):
            return None
        
        # Ambil 3 digit SSS (lama penyinaran)
        sss_str = sandi55[2:5]
        sss = pd.to_numeric(sss_str, errors='coerce')
        if pd.isna(sss):
            return None

        # Konversi ke jam dan menit
        jam_desimal = sss / 10
        jam = int(jam_desimal)
        menit = int(round((jam_desimal - jam) * 60))
        return f"{jam} jam {menit} menit"
    except:
        return None

data['Lama_Penyinaran'] = data['sandi55'].apply(interpret_lama_penyinaran)

# --- Ambil sandi 56 ---
def ambil_sandi56(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('56'):
            return t
    return None

data['sandi56'] = data['seksi3'].apply(ambil_sandi56)

def interpret_cloudmove(C):
    try:
        C = int(C)
        if C == 0:
            return 'Awan tidak bergerak'
        elif C == 1 :
            return 'NE'
        elif C == 2 :
            return 'E'
        elif C == 3 :
            return 'SE'
        elif C == 4 :
            return 'S'
        elif C == 5 :
            return 'SW'
        elif C == 6 :
            return 'W'
        elif C == 7 :
            return 'NW'
        elif C == 8 :
            return 'N'
        elif C == 9 :
            return 'Tidak diketahui'
    except:
        return None
    
    
def ambil_arah_awan_L(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('56') and len(t) >= 3:
            return interpret_cloudmove(t[2])
    return None

def ambil_arah_awan_M(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('56') and len(t) >= 4:  # ubah dari >=3 ke >=4
            return interpret_cloudmove(t[3])
    return None

def ambil_arah_awan_H(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('56') and len(t) >= 5:  # ubah dari >=3 ke >=5
            return interpret_cloudmove(t[4])
    return None
# def ambil_arah_awan_L(teks):
#     if not isinstance(teks, str):
#         return None
#     tokens = teks.split()
#     for t in tokens:
#         if t.startswith('56') and len(t) >= 3:
#             return interpret_cloudmove(t[2])  # ambil angka ke-3 dan interpretasikan
#     return None

data['DL'] = data['sandi56'].apply(ambil_arah_awan_L)

# def ambil_arah_awan_M(teks):
#     if not isinstance(teks, str):
#         return None
#     tokens = teks.split()
#     for t in tokens:
#         if t.startswith('56') and len(t) >= 3:
#             return interpret_cloudmove(t[3])  # ambil angka ke-3 dan interpretasikan
#     return None

data['DM'] = data['sandi56'].apply(ambil_arah_awan_M)

# def ambil_arah_awan_H(teks):
#     if not isinstance(teks, str):
#         return None
#     tokens = teks.split()
#     for t in tokens:
#         if t.startswith('56') and len(t) >= 3:
#             return interpret_cloudmove(t[4])  # ambil angka ke-3 dan interpretasikan
#     return None

data['DH'] = data['sandi56'].apply(ambil_arah_awan_H)



# --- Ambil sandi 57 ---
def ambil_sandi57(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('57'):
            return t
    return None

data['sandi57'] = data['seksi3'].apply(ambil_sandi57)

def awan_L(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('57') and len(t) >= 3:
            return interpret_cloudL(t[2])  # ambil angka ke-3 dan interpretasikan
    return None

data['Awan_Rendah'] = data['sandi57'].apply(awan_L)

def arah_sebenarnya(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('57') and len(t) >= 4:
            return interpret_cloudmove(t[3])  # ambil angka ke-3 dan interpretasikan
    return None

data['Arah_Sebenarnya'] = data['sandi57'].apply(arah_sebenarnya)

def interpretasi_elevasi(C):
    try:
        C = int(C)
        if C == 0:
            return 'Puncak awan tidak terlihat'
        elif C == 1 :
            return '45°'
        elif C == 2 :
            return '30°'
        elif C == 3 :
            return '20°'
        elif C == 4 :
            return '15°'
        elif C == 5 :
            return '12°'
        elif C == 6 :
            return '9°'
        elif C == 7 :
            return '7°'
        elif C == 8 :
            return '6°'
        elif C == 9 :
            return '5°'
    except:
        return None

def sudut_elevasi(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('57') and len(t) >= 5:
            return interpretasi_elevasi(t[4])  # ambil angka ke-3 dan interpretasikan
    return None

data['Elevasi'] = data['sandi57'].apply(sudut_elevasi)

# --- Ambil sandi 63 (curah hujan) ---
def ambil_sandi63(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('6'):
            return t
    return None

data['sandi63'] = data['seksi3'].apply(ambil_sandi63)

# --- Interpretasi curah hujan langsung dari sandi63 ---
def interpret_rain(x):
    try:
        # Pastikan x adalah string dengan format 6xxx
        if isinstance(x, str) and len(x) >= 5 and x.startswith('6'):
            ch = int(x[1:4])  # ambil 3 digit di tengah
            
            if 1 <= ch < 990:
                return ch
            elif ch == 990:
                return None
            elif 991 <= ch <= 999:
                return (ch - 990) / 10
    except:
        return None
    return None

data['Curah_Hujan_Jam'] = data['sandi63'].apply(interpret_rain)

# --- Ambil sandi 63 (curah hujan) ---
def ambil_sandi83(teks):
    if not isinstance(teks, str):
        return None
    tokens = teks.split()
    for t in tokens:
        if t.startswith('8'):
            return t
    return None

data['sandi83'] = data['seksi3'].apply(ambil_sandi83)

data['Nh3'] = pd.DataFrame(data.sandi83.astype(str).apply(lambda x: x[1:2] if len(x) >= 3 and x[0]=='8' else None))

data['Nh3'] = data['Nh3'].astype(str).str.zfill(1)

def interpret_cloud(x):
    try:
        # Pastikan string dan formatnya diawali 8
        if isinstance(x, str) and len(x) >= 4 and x[0] == '8':
            C = int(x[2])
            return {
                0: 'Cirrus (Ci)',
                1: 'Cirrocumulus (Cc)',
                2: 'Cirrostratus (Cs)',
                3: 'Altocumulus (Ac)',
                4: 'Altostratus (As)',
                5: 'Nimbostratus (Ns)',
                6: 'Stratocumulus (Sc)',
                7: 'Stratus (St)',
                8: 'Cumulus (Cu)',
                9: 'Cumulonimbus (Cb)'
            }.get(C, None)
    except:
        return None
    return None

data['C_interpretasi'] = data['sandi83'].apply(interpret_cloud)

# --- Bersihkan 'None' string jadi NaN ---
data.replace('None', np.nan, inplace=True)

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

db_path_lengkap = "data_lengkap2.db"
table_name_lengkap = "data_lengkap"
conn_lengkap = sqlite3.connect(db_path_lengkap)
cursor_lengkap = conn_lengkap.cursor()
cursor_lengkap.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_lengkap} (
    tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL,
    sandi_gts TEXT,nddff TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
    Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL, Dew_Point REAL
)""")
for _, row in df_final.iterrows():
    cursor_lengkap.execute(f"DELETE FROM {table_name_lengkap} WHERE tanggal = ? AND jam = ? AND station_wmo_id = ?", (row["tanggal"], row["jam"], row["station_wmo_id"]))
df_to_insert_lengkap = df_final[["tanggal","jam","station_wmo_id",'NAME','LAT',"LON",'ELEV',"sandi_gts","nddff","Curah_Hujan","Heavy_Rain","Curah_Hujan_Jam","Gale","Kecepatan_angin","Arah_angin","Temperatur","Tekanan_Permukaan","Tmin","Tmax","Dew_Point"]]
df_to_insert_lengkap.to_sql(table_name_lengkap, conn_lengkap, if_exists="append", index=False)
conn_lengkap.commit()
conn_lengkap.close()
tanggal_batch_lengkap = df_final["tanggal"].unique().tolist()
print(f"✅ Data LENGKAP untuk tanggal {tanggal_batch_lengkap} berhasil diupdate ke {db_path_lengkap}")

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


# ==============================================================================
# === 3. Simpan data BENAR ke database data_akhir.db ===========================
# ==============================================================================

# --- Tambahkan kolom tanggal dan jam untuk penyimpanan ---
data_akhir["timestamp_data"] = pd.to_datetime(data_akhir["timestamp_data"])
data_akhir["tanggal"] = data_akhir["timestamp_data"].dt.date.astype(str)
data_akhir["jam"] = data_akhir["timestamp_data"].dt.strftime("%H:%M")

db_path_akhir = "data_akhir.db"
table_name_akhir = "data_akhir"
conn_akhir = sqlite3.connect(db_path_akhir)
cursor_akhir = conn_akhir.cursor()

cursor_akhir.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_akhir} (
    tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL,
    sandi_gts TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
    Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL
)""")

# Tentukan kolom yang akan disimpan
cols_akhir = [
    "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV", "sandi_gts",
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

# ==============================================================================
# === 4. Simpan data SALAH ke database data_salah2.db ==========================
# ==============================================================================


# ==============================================================================
# === 4. Simpan data SALAH ke database data_salah2.db ==========================
# ==============================================================================

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
db_path_salah = "data_salah2.db"
table_name_salah = "data_salah"
conn_salah = sqlite3.connect(db_path_salah)
cursor_salah = conn_salah.cursor()

cursor_salah.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_salah} (
    tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL,
    sandi_gts TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
    Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL, 
    tanggal_observasi TEXT
)""")

# Tentukan kolom yang akan disimpan
cols_salah = [
    "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV", "sandi_gts",
    "Curah_Hujan", "Heavy_Rain", "Curah_Hujan_Jam", "Gale", "Kecepatan_angin",
    "Arah_angin", "Temperatur", "Tekanan_Permukaan", "Tmin", "Tmax", "tanggal_observasi"
]
cols_to_insert_salah = [col for col in cols_salah if col in data_salah2.columns]
df_to_insert_salah = data_salah2[cols_to_insert_salah].dropna(subset=['tanggal'])


# --- Hapus data lama secara efisien (per batch tanggal) ---
#tanggal_batch_salah = df_to_insert_salah["tanggal"].unique().tolist()

try:
    tanggal_batch_salah = df_to_insert_salah["tanggal"].squeeze().unique().tolist()
except Exception as e:
    print("⚠️ Gagal membaca daftar tanggal dari df_to_insert_salah:", e)
    tanggal_batch_salah = []

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



# # pastikan timestamp sudah datetime
# df_final["timestamp_data"] = pd.to_datetime(df_final["timestamp_data"])

# # --- Ambil data jam 00 ---
# df_jam00 = df_final[df_final["timestamp_data"].dt.hour == 0]

# kolom_pilihan = [
#     "timestamp_data", "station_wmo_id", "NAME", "sandi_gts", "LAT", "LON", "ELEV",
#     "Arah_angin", "Kecepatan_angin", "Gale", "Temperatur", "Tmin", "Tmax",
#     "Tekanan_Permukaan", "Curah_Hujan", "Heavy_Rain"
# ]
# data_00 = df_jam00[kolom_pilihan]

# # --- Tanggal observasi (jam 00 dianggap milik hari sebelumnya) ---
# df_final["tanggal_observasi"] = df_final["timestamp_data"].dt.date
# mask_jam00 = df_final["timestamp_data"].dt.hour == 0
# df_final.loc[mask_jam00, "tanggal_observasi"] = (
#     df_final.loc[mask_jam00, "tanggal_observasi"] - pd.Timedelta(days=1)
# )

# # --- Hitung total curah hujan per stasiun ---
# df_harian = df_final.groupby(["station_wmo_id"], as_index=False)[["Curah_Hujan_Jam"]].sum()

# # --- Gabungkan data jam 00 dan akumulasi harian ---
# data_merge = data_00.merge(df_harian, on="station_wmo_id", how="left")

# # --- Pastikan tidak ada nilai NaN ---
# data_merge["Curah_Hujan"] = data_merge["Curah_Hujan"].fillna(0)
# data_merge["Curah_Hujan_Jam"] = data_merge["Curah_Hujan_Jam"].fillna(0)

# # --- Hitung selisih dan perbaiki error float ---
# data_merge["selisih"] = (data_merge["Curah_Hujan_Jam"] - data_merge["Curah_Hujan"]).abs()
# data_merge["selisih"] = data_merge["selisih"].round(2)

# # --- Data BENAR ---
# data_akhir = data_merge[
#     (data_merge["selisih"] <= 2.0 + 1e-6) |  # toleransi kecil untuk float
#     ((data_merge["Curah_Hujan_Jam"] == 0) & (data_merge["Curah_Hujan"] == 0))
# ].copy()

# # --- Data SALAH ---
# data_salah = data_merge.loc[~data_merge.index.isin(data_akhir.index)].copy()

# # --- Tambahkan kolom tanggal dan jam untuk penyimpanan ---
# for df in [data_akhir, data_salah]:
#     df["timestamp_data"] = pd.to_datetime(df["timestamp_data"])
#     df["tanggal"] = df["timestamp_data"].dt.date.astype(str)
#     df["jam"] = df["timestamp_data"].dt.strftime("%H:%M")

# # ==============================================================
# # === Simpan data BENAR ke database data_akhir.db
# # ==============================================================

# db_path_akhir = "data_akhir.db"
# table_name_akhir = "data_akhir"
# conn_akhir = sqlite3.connect(db_path_akhir)
# cursor_akhir = conn_akhir.cursor()

# cursor_akhir.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_akhir} (
#     tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL,
#     sandi_gts TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
#     Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL
# )""")

# # Hapus semua data di database untuk tanggal yang akan diupdate
# tanggal_batch_akhir = data_akhir["tanggal"].unique().tolist()
# for tgl in tanggal_batch_akhir:
#     cursor_akhir.execute(f"DELETE FROM {table_name_akhir} WHERE tanggal = ?", (tgl,))
#     print(f"🗑️ Hapus data lama tanggal {tgl} dari {table_name_akhir}")

# # Simpan data baru
# cols_akhir = [
#     "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV", "sandi_gts",
#     "Curah_Hujan", "Heavy_Rain", "Curah_Hujan_Jam", "Gale", "Kecepatan_angin",
#     "Arah_angin", "Temperatur", "Tekanan_Permukaan", "Tmin", "Tmax"
# ]
# data_akhir[cols_akhir].to_sql(table_name_akhir, conn_akhir, if_exists="append", index=False)
# conn_akhir.commit()
# conn_akhir.close()

# print(f"✅ Data AKHIR untuk tanggal {tanggal_batch_akhir} berhasil disimpan ke {db_path_akhir}")

# # ==============================================================
# # === Simpan data SALAH ke database data_salah.db
# # ==============================================================

# # buat key unik untuk match di df_final
# data_salah["key"] = data_salah["station_wmo_id"].astype(str) + "_" + data_salah["timestamp_data"].astype(str)
# df_final_copy = df_final.copy()
# df_final_copy["timestamp_data"] = pd.to_datetime(df_final_copy["timestamp_data"])
# df_final_copy["key"] = df_final_copy["station_wmo_id"].astype(str) + "_" + df_final_copy["timestamp_data"].astype(str)

# # ambil data full jam untuk stasiun & waktu yang masuk kategori salah
# keys_salah = set(data_salah["key"].unique())
# data_salah2 = df_final_copy[df_final_copy["key"].isin(keys_salah)].copy()

# # tambahkan tanggal & jam
# data_salah2["tanggal"] = data_salah2["timestamp_data"].dt.date.astype(str)
# data_salah2["jam"] = data_salah2["timestamp_data"].dt.strftime("%H:%M")
# if "tanggal_observasi" not in data_salah2.columns:
#     data_salah2["tanggal_observasi"] = data_salah2["timestamp_data"].dt.date.astype(str)

# # simpan ke DB
# db_path_salah = "data_salah.db"
# table_name_salah = "data_salah"
# conn_salah = sqlite3.connect(db_path_salah)
# cursor_salah = conn_salah.cursor()

# cursor_salah.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_salah} (
#     tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL,
#     sandi_gts TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
#     Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL, tanggal_observasi TEXT
# )""")

# # Hapus semua data di database untuk tanggal yang akan diupdate
# tanggal_batch_salah = data_salah2["tanggal"].unique().tolist()
# for tgl in tanggal_batch_salah:
#     cursor_salah.execute(f"DELETE FROM {table_name_salah} WHERE tanggal = ?", (tgl,))
#     print(f"🗑️ Hapus data lama tanggal {tgl} dari {table_name_salah}")

# # Simpan data baru
# cols_salah = [
#     "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV", "sandi_gts",
#     "Curah_Hujan", "Heavy_Rain", "Curah_Hujan_Jam", "Gale", "Kecepatan_angin",
#     "Arah_angin", "Temperatur", "Tekanan_Permukaan", "Tmin", "Tmax", "tanggal_observasi"
# ]
# data_salah2[cols_salah].to_sql(table_name_salah, conn_salah, if_exists="append", index=False)
# conn_salah.commit()
# conn_salah.close()

# print(f"✅ Data SALAH untuk tanggal {tanggal_batch_salah} berhasil disimpan ke {db_path_salah}")

# # --- Logika Pemisahan Data Benar dan Salah ---
# df_final["timestamp_data"] = pd.to_datetime(df_final["timestamp_data"])

# # --- Ambil data jam 00 ---
# df_jam00 = df_final[df_final["timestamp_data"].dt.hour == 0]

# kolom_pilihan = [
#     "timestamp_data", "station_wmo_id", "NAME", "sandi_gts", "LAT", "LON", "ELEV",
#     "Arah_angin", "Kecepatan_angin", "Gale", "Temperatur", "Tmin", "Tmax",
#     "Tekanan_Permukaan", "Curah_Hujan", "Heavy_Rain"
# ]
# data_00 = df_jam00[kolom_pilihan]

# # --- Tanggal observasi (jam 00 dianggap milik hari sebelumnya) ---
# df_final["tanggal_observasi"] = df_final["timestamp_data"].dt.date
# mask_jam00 = df_final["timestamp_data"].dt.hour == 0
# df_final.loc[mask_jam00, "tanggal_observasi"] = (
#     df_final.loc[mask_jam00, "tanggal_observasi"] - pd.Timedelta(days=1)
# )

# # --- Hitung total curah hujan per stasiun ---
# df_harian = df_final.groupby(["station_wmo_id"], as_index=False)[["Curah_Hujan_Jam"]].sum()

# # --- Gabungkan data jam 00 dan akumulasi harian ---
# data_merge = data_00.merge(df_harian, on="station_wmo_id", how="left")

# # --- Pastikan tidak ada nilai NaN ---
# data_merge["Curah_Hujan"] = data_merge["Curah_Hujan"].fillna(0)
# data_merge["Curah_Hujan_Jam"] = data_merge["Curah_Hujan_Jam"].fillna(0)

# # --- Hitung selisih dan perbaiki error float ---
# data_merge["selisih"] = data_merge["Curah_Hujan_Jam"] - data_merge["Curah_Hujan"]
# data_merge.loc[data_merge["selisih"].abs() < 1e-6, "selisih"] = 0
# data_merge["selisih"] = data_merge["selisih"].abs().round(2)

# # --- Data BENAR ---
# data_akhir = data_merge[
#     (data_merge["selisih"] <= 2) |
#     ((data_merge["Curah_Hujan_Jam"] == 0) & (data_merge["Curah_Hujan"] == 0))
# ].copy()

# # --- Data SALAH ---
# data_salah = data_merge.loc[~data_merge.index.isin(data_akhir.index)].copy()

# # --- Pisahkan kolom timestamp menjadi tanggal & jam
# data_akhir["timestamp_data"] = pd.to_datetime(data_akhir["timestamp_data"])
# data_akhir["tanggal"] = data_akhir["timestamp_data"].dt.date.astype(str)
# data_akhir["jam"] = data_akhir["timestamp_data"].dt.strftime("%H:%M")

# # --- Simpan ke data_akhir.db ---
# db_path_akhir = "data_akhir.db"
# table_name_akhir = "data_akhir"
# conn_akhir = sqlite3.connect(db_path_akhir)
# cursor_akhir = conn_akhir.cursor()
# cursor_akhir.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_akhir} (
#     tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL,
#     sandi_gts TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
#     Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL
# )""")

# for _, row in data_akhir.iterrows():
#     cursor_akhir.execute(
#         f"DELETE FROM {table_name_akhir} WHERE tanggal = ? AND jam = ? AND station_wmo_id = ?",
#         (row["tanggal"], row["jam"], row["station_wmo_id"])
#     )

# df_to_insert_akhir = data_akhir[[
#     "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV",
#     "sandi_gts", "Curah_Hujan", "Heavy_Rain", "Curah_Hujan_Jam",
#     "Gale", "Kecepatan_angin", "Arah_angin", "Temperatur",
#     "Tekanan_Permukaan", "Tmin", "Tmax"
# ]]
# df_to_insert_akhir.to_sql(table_name_akhir, conn_akhir, if_exists="append", index=False)
# conn_akhir.commit()
# conn_akhir.close()
# tanggal_batch_akhir = data_akhir["tanggal"].unique().tolist()
# print(f"\n✅ Data AKHIR untuk tanggal {tanggal_batch_akhir} berhasil diupdate ke {db_path_akhir}")

# # --- Proses dan Simpan ke data_salah.db (perbaikan: match by station+timestamp) ---
# # pastikan timestamp di data_salah bertipe datetime
# data_salah = data_salah.copy()
# data_salah["timestamp_data"] = pd.to_datetime(data_salah["timestamp_data"])

# # buat kolom kunci untuk matching (station + timestamp)
# data_salah["key"] = data_salah["station_wmo_id"].astype(str) + "_" + data_salah["timestamp_data"].astype(str)

# # buat key yang sama di df_final
# df_final_copy = df_final.copy()
# df_final_copy["timestamp_data"] = pd.to_datetime(df_final_copy["timestamp_data"])
# df_final_copy["key"] = df_final_copy["station_wmo_id"].astype(str) + "_" + df_final_copy["timestamp_data"].astype(str)

# # pilih baris df_final yang key-nya ada di data_salah.key
# keys_salah = set(data_salah["key"].unique())
# data_salah2 = df_final_copy[df_final_copy["key"].isin(keys_salah)].copy()

# # Tambahkan kolom tanggal/jam seperti sebelumnya
# data_salah2["tanggal"] = data_salah2["timestamp_data"].dt.date.astype(str)
# data_salah2["jam"] = data_salah2["timestamp_data"].dt.strftime("%H:%M")

# # Simpan ke DB (seperti sebelumnya) — hapus baris yang sama dulu
# db_path_salah = "data_salah.db"
# table_name_salah = "data_salah"
# conn_salah = sqlite3.connect(db_path_salah)
# cursor_salah = conn_salah.cursor()
# cursor_salah.execute(f"""CREATE TABLE IF NOT EXISTS {table_name_salah} (
#     tanggal TEXT, jam TEXT, station_wmo_id TEXT, NAME TEXT, LAT REAL, LON REAL, ELEV REAL,
#     sandi_gts TEXT, Curah_Hujan REAL, Heavy_Rain REAL, Curah_Hujan_Jam REAL, Gale REAL,
#     Kecepatan_angin REAL, Arah_angin REAL, Temperatur REAL, Tekanan_Permukaan REAL, Tmin REAL, Tmax REAL, tanggal_observasi TEXT
# )""")

# for _, row in data_salah2.iterrows():
#     cursor_salah.execute(
#         f"DELETE FROM {table_name_salah} WHERE tanggal = ? AND jam = ? AND station_wmo_id = ?",
#         (row["tanggal"], row["jam"], str(row["station_wmo_id"]))
#     )

# cols = ["tanggal","jam","station_wmo_id",'NAME','LAT',"LON",'ELEV',"sandi_gts","Curah_Hujan",
#         "Heavy_Rain","Curah_Hujan_Jam","Gale","Kecepatan_angin","Arah_angin","Temperatur",
#         "Tekanan_Permukaan","Tmin","Tmax",'tanggal_observasi']
# # jika df_final tidak punya 'tanggal_observasi', isi dulu
# if "tanggal_observasi" not in data_salah2.columns:
#     data_salah2["tanggal_observasi"] = data_salah2["timestamp_data"].dt.date.astype(str)

# df_to_insert_salah = data_salah2[cols]
# df_to_insert_salah.to_sql(table_name_salah, conn_salah, if_exists="append", index=False)
# conn_salah.commit()
# conn_salah.close()
# tanggal_batch_salah = data_salah2["tanggal"].unique().tolist()
# print(f"✅ Data SALAH (baris jam yang memang suspect) untuk tanggal {tanggal_batch_salah} berhasil diupdate ke {db_path_salah}")

