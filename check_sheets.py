import gspread
from google.oauth2.service_account import Credentials
c = Credentials.from_service_account_file("credentials.json", scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
gc = gspread.authorize(c)
sh = gc.open_by_key("1PuvLTeBaHK9uOWC-iZo9T06mgeU7CETcZ-XJLX2Jdfw")
for aba in ["Competitividade Câmara Atual", "Competitividade Assembleias Atual"]:
    try:
        ws = sh.worksheet(aba)
        headers = ws.row_values(1)
        print(f"--- {aba} ---")
        print("Headers:", headers[-10:]) # print last 10 headers
    except Exception as e:
        print(e)
