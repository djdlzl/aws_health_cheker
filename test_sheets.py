import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 구글 인증
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("total-pad-452908-i9-262988efc1a1.json", scope)
client = gspread.authorize(creds)

# 시트 열기
sheet = client.open("SRE1_SMB 고객 관리_v1.0").sheet1  # 필요 시 sheet 이름 수정

# row 2 ~ 120까지 가져오기 (1은 헤더라고 가정)
data = sheet.get_values("A2:AC120")  # A~AC까지 한 번에 가져옴

result = []
latest_company_name = None

for row in data:
    company_name = row[1].strip() if len(row) > 1 and row[1] else None
    env = row[3].strip() if len(row) > 3 and row[3] else None

    # 고객사명 + 환경 모두 None이면 skip
    if not company_name and not env:
        continue

    # 고객사명 병합 처리
    if company_name:
        latest_company_name = company_name
    else:
        company_name = latest_company_name

    # 필요한 필드 추출
    entry = {
        "고객사명": company_name,
        "환경": env,
        "상세이름": row[4] if len(row) > 4 else "",
        "IAM 사용자": row[5] if len(row) > 5 else "",
        "비밀번호": row[6] if len(row) > 6 else "",
        "Account ID": row[7] if len(row) > 7 else "",
        "MFA Secret": row[28] if len(row) > 28 else "",
    }

    result.append(entry)
# 출력 확인
for item in result:
    print(item)
