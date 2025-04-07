import json
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pyotp
import os
import logging

# 에러 로그 생성 함수
def setup_process_logger():
    log_path = os.path.join(os.getcwd(), "process_error.log")

    logger = logging.getLogger("process_error")
    logger.setLevel(logging.ERROR)

    if not logger.handlers:
        handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# 카운트와 이벤트 리스트를 가져오는 함수
def get_count_and_events(client, driver, section_name, count_xpath, button_xpath, tbody_xpath, detail_xpath):
    print(f"{section_name} 요소 대기 중...")
    link = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, count_xpath))
    )
    count_text = link.text.strip()
    count = int(re.search(r'\d+', count_text).group() if re.search(r'\d+', count_text) else '0')
    print(f"{section_name} 카운트: {count}")

    events = []
    all_affected_resources = []
    if count > 0:
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, button_xpath))
        )
        button.click()
        time.sleep(2)

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, tbody_xpath)))
        rows = driver.find_elements(By.XPATH, f"{tbody_xpath}/tr")
        for row in rows:
            event_link = row.find_element(By.XPATH, "./td[2]/div/a")
            event_title = event_link.text.strip()
            if event_title:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", event_link)
                driver.execute_script("window.scrollBy(0, -100);")
                try:
                    interfering_span = driver.find_element(By.CSS_SELECTOR, "[data-analytics-funnel-key='substep-name']")
                    driver.execute_script("arguments[0].style.display = 'none';", interfering_span)
                    event_link = row.find_element(By.XPATH, "./td[2]/div/a")
                    driver.execute_script("arguments[0].click();", event_link)
                except Exception as e:
                    print(f"클릭 실패: {event_title}, 오류: {str(e)}")
                    driver.save_screenshot(f"click_error_{event_title}.png")
                    raise
                time.sleep(2)
                event_details = get_all_sub_texts(driver, detail_xpath)
                
                event_resources = get_affected_resources(client, driver)
                all_affected_resources.extend(event_resources)
                
                events.append({
                    "title": event_title,
                    "details": event_details,
                    "affected_resources": event_resources
                })
                
                time.sleep(1)
                cancel_button_xpath = '/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[1]/div/div/button[2]'
                cancel_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, cancel_button_xpath)))
                cancel_button.click()
                time.sleep(2)

    return count, events

# 하위 모든 요소의 텍스트를 가져오는 함수 (한글/영어 처리 추가)
def get_all_sub_texts(driver, parent_xpath):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, parent_xpath))
        )
        all_elements_xpath = f"{parent_xpath}//*"
        elements = driver.find_elements(By.XPATH, all_elements_xpath)
        
        all_texts = []
        for element in elements:
            text = element.text.strip()
            if text and text != '-' and text != "이 이벤트에 대한 피드백" and text != "Feedback for this event":
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    if line:
                        all_texts.append(line)
        
        # 한글과 영어 키 매핑
        key_mapping = {
            "서비스": "Service",
            "시작 시간": "Start time",
            "상태": "Status",
            "종료 시간": "End time",
            "리전/가용 영역": "Region / Availability Zone",
            "범주": "Category",
            "계정별": "Account specific",
            "영향을 받는 리소스": "Affected resources",
            "설명": "Description"
        }
        kr_keys = list(key_mapping.keys())  # 한글 키
        en_keys = list(key_mapping.values())  # 영어 키
        
        event_details = {}
        i = 0
        while i < len(all_texts):
            text = all_texts[i]
            matched = False
            
            # 1. 한글 키로 먼저 시도
            for kr_key in kr_keys:
                if text == kr_key or text.startswith(kr_key):
                    value = text[len(kr_key):].strip() if text != kr_key else None
                    if not value and i + 1 < len(all_texts):
                        next_text = all_texts[i + 1].strip()
                        if not any(next_text.startswith(k) for k in kr_keys + en_keys):
                            value = next_text
                            i += 1
                    if kr_key == "종료 시간":
                        event_details[kr_key] = "-" if not value or value == "-" else value
                    elif kr_key == "설명":
                        description_lines = [value] if value else []
                        j = i + 1
                        while j < len(all_texts) and not any(all_texts[j].startswith(k) for k in kr_keys + en_keys):
                            description_lines.append(all_texts[j].strip().replace('\\n', '\n'))
                            j += 1
                        event_details[kr_key] = "\n".join(description_lines)
                        i = j - 1
                    else:
                        event_details[kr_key] = value
                    # print(f"한글 키 매핑: {kr_key} -> {value}")
                    matched = True
                    break
            
            # 2. 한글로 안 되면 영어 키로 시도
            if not matched:
                for en_key in en_keys:
                    if text == en_key or text.startswith(en_key):
                        kr_key = [k for k, v in key_mapping.items() if v == en_key][0]  # 영어 -> 한글 변환
                        value = text[len(en_key):].strip() if text != en_key else None
                        if not value and i + 1 < len(all_texts):
                            next_text = all_texts[i + 1].strip()
                            if not any(next_text.startswith(k) for k in kr_keys + en_keys):
                                value = next_text
                                i += 1
                        if kr_key == "종료 시간":
                            event_details[kr_key] = "-" if not value or value == "-" else value
                        elif kr_key == "설명":
                            description_lines = [value] if value else []
                            j = i + 1
                            while j < len(all_texts) and not any(all_texts[j].startswith(k) for k in kr_keys + en_keys):
                                description_lines.append(all_texts[j].strip().replace('\\n', '\n'))
                                j += 1
                            event_details[kr_key] = "\n".join(description_lines)
                            i = j - 1
                        else:
                            event_details[kr_key] = value
                        # print(f"영어 키 매핑: {en_key} -> {value} (저장 키: {kr_key})")
                        matched = True
                        break
            
            i += 1
        
        if "종료 시간" not in event_details:
            event_details["종료 시간"] = "-"
        
        return event_details
    except Exception as e:
        print(f"하위 텍스트 수집 중 오류: {str(e)}")
        return {}

def get_affected_resources(client, driver):
    affected_resources_tab_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[2]/div/div/div/div/div/div/div[1]/div/ul/li[2]/div/button"
    affected_resources_link_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[2]/div/div/div/div/div/div/div[2]/div[2]/div/section/div/div/div[2]/div/div[1]/table/tbody/tr/td[1]/div/a"
    affected_resources_text_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[2]/div/div/div/div/div/div/div[2]/div[2]/div/section/div/div/div[2]/div/div[1]/table/tbody/tr/td[1]/div/span"
    
    print(f"{client['name']} - 영향받는 리소스 탭 확인 중...")
    affected_resources_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, affected_resources_tab_xpath))
    )
    affected_resources_button.click()
    time.sleep(2)

    affected_resources = []
    link_elements = driver.find_elements(By.XPATH, affected_resources_link_xpath)
    for elem in link_elements:
        resource_text = elem.text.strip()
        resource_link = elem.get_attribute("href")
        if resource_text:
            affected_resources.append({"text": resource_text, "link": resource_link})
    
    text_elements = driver.find_elements(By.XPATH, affected_resources_text_xpath)
    for elem in text_elements:
        resource_text = elem.text.strip()
        if resource_text:
            affected_resources.append({"text": resource_text, "link": None})

    print(f"{client['name']} - 영향받는 리소스 수집 완료: {len(affected_resources)}개")
    return affected_resources

# 각 계정 처리를 위한 함수
def process_account(client, chromedriver_path):
    max_attempts = 3
    attempt = 1
    
    while attempt <= max_attempts:
        print(f"{client['name']} 계정 처리 시작... (시도 {attempt}/{max_attempts})")
        
        options = webdriver.ChromeOptions()
        options.headless = False
        options.add_experimental_option("detach", True)
        options.add_argument('--start-maximized')
        options.add_argument('--headless')
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)

        try:
            aws_login_url = f"https://{client['account']}.signin.aws.amazon.com/console"
            print(f"{client['name']} 접속 URL: {aws_login_url}")

            driver.get(aws_login_url)
            WebDriverWait(driver, 30).until(lambda d: d.execute_script('return document.readyState') == 'complete')

            username_field = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "username")))
            username_field.send_keys(client['username'])

            password_field = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "password")))
            password_field.send_keys(client['password'])

            signin_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "signin_button")))
            signin_button.click()

            mfa_field = WebDriverWait(driver, 15).until(EC.visibility_of_element_located((By.ID, "mfaCode")))
            totp = pyotp.TOTP(client['mfaSecret'])
            mfa_code = totp.now()
            mfa_field.send_keys(mfa_code)

            submit_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'], button.awsui-button")))
            submit_button.click()

            WebDriverWait(driver, 15).until(EC.any_of(EC.url_contains("console.aws.amazon.com"), EC.presence_of_element_located((By.ID, "aws-console-root"))))
            print(f"{client['name']} 로그인 완료!")

            alarm_button_selector = "#paddy-notification-widget-1 > div > div > button"
            alarm_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, alarm_button_selector)))
            alarm_button.click()

            all_events_button_xpath = "/html/body/div[2]/div[1]/div/div[3]/div/header/nav/div[1]/div[3]/div[2]/div[1]/div/div/div/div/footer/div[1]/a"
            all_events_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, all_events_button_xpath)))
            all_events_button.click()

            time.sleep(2)
            unresolved_issues_count_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[1]/div/a/span/span/span/span"
            unresolved_issues_button_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[1]/div/a"
            scheduled_changes_count_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[2]/div/a/span/span/span/span"
            scheduled_changes_button_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[2]/div/a"
            other_notifications_count_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[3]/div/a/span/span/span/span"
            other_notifications_button_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[3]/div/a"
            tbody_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[2]/div/div/section/div/div/div[2]/div/div[1]/table/tbody"
            detail_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[2]/div/div/div/div/div/div/div[2]/div[1]/div/div/div[2]/div"

            unresolved_count, unresolved_events = get_count_and_events(client, driver, "미해결 문제", unresolved_issues_count_xpath, unresolved_issues_button_xpath, tbody_xpath, detail_xpath)
            scheduled_count, scheduled_events = get_count_and_events(client, driver, "예정된 변경 사항", scheduled_changes_count_xpath, scheduled_changes_button_xpath, tbody_xpath, detail_xpath)
            other_count, other_events = get_count_and_events(client, driver, "기타 알림", other_notifications_count_xpath, other_notifications_button_xpath, tbody_xpath, detail_xpath)

            result = {
                "name": client["name"],
                "unresolved_count": unresolved_count,
                "scheduled_count": scheduled_count,
                "other_count": other_count,
                "events": {
                    "unresolved": unresolved_events,
                    "scheduled": scheduled_events,
                    "other": other_events,
                }
            }
            print(f"{client['name']} 처리 완료: unresolved={len(unresolved_events)}, scheduled={len(scheduled_events)}, other={len(other_events)}")
            driver.quit()
            return result

        except Exception as e:
            print(f"{client['name']} 시도 {attempt} 실패: {str(e)}")
            driver.save_screenshot(f"error_{client['name']}_attempt_{attempt}.png")
            driver.quit()
            attempt += 1
            if attempt > max_attempts:
                print(f"{client['name']} 최대 시도 횟수 {max_attempts} 초과, 처리 중단")
                return {"name": client["name"], "error": f"최대 시도 횟수 초과: {str(e)}"}
            time.sleep(3)

# 메인 실행 로직
def main():
    logger = setup_process_logger()
    try:
        # 기존 main 로직
        clients = load_clients_from_sheets("시트명")

        chromedriver_path = os.path.join(os.getcwd(), "chromedriver.exe")

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_client = {
                executor.submit(process_account, client, chromedriver_path): client
                for client in clients
            }

            for future in as_completed(future_to_client):
                client = future_to_client[future]
                try:
                    result = future.result()
                    if "error" not in result:
                        append_event_to_excel_by_sheet(result)
                    else:
                        print(f"{client['name']} 실패: {result['error']}")
                        log_failed_client(client['name'], result['error'])
                except Exception as e:
                    log_failed_client(client['name'], str(e))
                    logger.error(f"{client['name']} 처리 중 예외 발생", exc_info=True)

        clean_excel_file()

    except Exception as e:
        logger.error("전체 프로세스 중 예외 발생", exc_info=True)
        print("치명적인 에러가 발생했습니다. 로그를 확인해주세요.")

if __name__ == "__main__":
    main()