import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import os

def setup_missing_logger():
    log_path = os.path.join(os.getcwd(), "missing_clients.log")
    
    logger = logging.getLogger("missing_clients")
    logger.setLevel(logging.INFO)

    # 중복 핸들러 방지
    if not logger.handlers:
        handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def load_clients_from_sheets(sheet_title):
    logger = setup_missing_logger()
    # 로그 파일 설정
    log_path = os.path.join(os.getcwd(), "missing_clients.log")
    logging.basicConfig(
        filename=log_path,
        filemode='w',  # 항상 새로 쓰기 (필요 시 'a'로 바꾸면 누적됨)
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
    )

    exclude_keywords = ["고객사", "issuereporter", "NCP"]

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("total-pad-452908-i9-262988efc1a1.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_title).sheet1
    raw_data = sheet.get_values("A2:AC6")

    parsed_clients = []
    latest_company_name = None

    for row in raw_data:
        company = row[1].strip() if len(row) > 1 and row[1] else None
        env = row[3].strip() if len(row) > 3 and row[3] else None

        #비어있는 행 제외
        if not company and not env:
            continue

        if company:
            latest_company_name = company
        else:
            company = latest_company_name

        detail = row[4].strip() if len(row) > 4 else ""
        full_name = f"{company}-{env}-{detail}"

        if any(keyword in full_name for keyword in exclude_keywords):
            print(f"제외됨: {full_name}")
            continue

        # 필수 항목 체크
        missing_fields = []
        username = row[5].strip() if len(row) > 5 and row[5] else ""
        password = row[6].strip() if len(row) > 6 and row[6] else ""
        account = row[7].strip() if len(row) > 7 and row[7] else ""
        mfa_secret = row[28].strip() if len(row) > 28 and row[28] else ""

        if not username:
            missing_fields.append("IAM 사용자")
        if not password:
            missing_fields.append("비밀번호")
        if not account:
            missing_fields.append("Account ID")
        if not mfa_secret:
            missing_fields.append("MFA Secret")

        if missing_fields:
            logger.info(f"{full_name} - 누락 항목: {', '.join(missing_fields)}")
            continue  # 정보 누락된 항목은 실행 대상에서 제외

        client_obj = {
            "name": full_name,
            "username": username,
            "password": password,
            "account": account,
            "mfaSecret": mfa_secret
        }

        parsed_clients.append(client_obj)

    return parsed_clients
