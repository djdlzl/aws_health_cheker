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
    all_affected_resources = []  # 모든 이벤트의 영향받는 리소스를 저장할 리스트
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
                # 스크롤 조정 및 클릭 대기 강화
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", event_link)
                print("여기까지 실행1")
                driver.execute_script("window.scrollBy(0, -100);")  # 푸터와 겹치지 않도록 위로 이동
                print("여기까지 실행2")
                try:
                    # 방해 요소 숨기기
                    interfering_span = driver.find_element(By.CSS_SELECTOR, "[data-analytics-funnel-key='substep-name']")
                    driver.execute_script("arguments[0].style.display = 'none';", interfering_span)
                except:
                    print("방해 요소 없음, 무시")
                try:
                    event_link = row.find_element(By.XPATH, "./td[2]/div/a")
                    print(f"요소 상태 - 표시: {event_link.is_displayed()}, 활성: {event_link.is_enabled()}")
                    driver.execute_script("arguments[0].click();", event_link)
                    print("클릭 완료 (JS)")
                except Exception as e:
                    print(f"클릭 실패: {event_title}, 오류: {str(e)}")
                    driver.save_screenshot(f"click_error_{event_title}.png")
                    raise
                time.sleep(1)
                event_details = get_all_sub_texts(driver, detail_xpath)
                
                # 영향받는 리소스 수집
                event_resources = get_affected_resources(client, driver)
                all_affected_resources.extend(event_resources)  # 모든 리소스를 하나의 리스트에 추가
                
                events.append({
                    "title": event_title, 
                    "details": event_details,
                    "affected_resources": event_resources  # 각 이벤트에 해당 이벤트의 영향받는 리소스 추가
                })
                
                time.sleep(1)
                cancel_button_xpath = '/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[1]/div/div/button[2]'
                cancel_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, cancel_button_xpath)))
                cancel_button.click()

                time.sleep(1)

    return count, events

# 하위 모든 요소의 텍스트를 가져오는 함수
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
            if text and text != '-' and text != "이 이벤트에 대한 피드백":
                # \n으로 분리해서 각 줄을 개별 텍스트로 추가
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    if line:
                        all_texts.append(line)
                
        # 고정된 키 목록
        keys = [
            "서비스", "시작 시간", "종료 시간", "상태",
            "리전/가용 영역", "범주", "계정별", "영향을 받는 리소스", "설명"
        ]
        
        # 키-값 매핑
        event_details = {}
        i = 0
        while i < len(all_texts):
            text = all_texts[i]
            for key in keys:
                if text == key or text.startswith(key):
                    # 키 이름 제거하고 값만 추출
                    value = text[len(key):].strip() if text != key else None
                    if not value and i + 1 < len(all_texts):
                        next_text = all_texts[i + 1].strip()
                        # 다음 텍스트가 키가 아니면 값으로 사용
                        if not any(next_text.startswith(k) for k in keys):
                            value = next_text
                            i += 1
                    # "종료 시간" 처리
                    if key == "종료 시간":
                        event_details[key] = "-" if not value or value == "-" else value
                    # "설명" 처리
                    elif key == "설명":
                        description_lines = [value] if value else []
                        j = i + 1
                        while j < len(all_texts) and not any(all_texts[j].startswith(k) for k in keys):
                            description_lines.append(all_texts[j].strip().replace('\\n', '\n'))
                            j += 1
                        event_details[key] = "\n".join(description_lines)
                        i = j - 1
                    else:
                        event_details[key] = value
                    break
            i += 1
        
        # "종료 시간" 기본값 설정
        if "종료 시간" not in event_details:
            event_details["종료 시간"] = "-"
        
        return event_details
    except Exception as e:
        print(f"하위 텍스트 수집 중 오류: {str(e)}")
        return {}

def get_affected_resources(client, driver):
        # "영향받는 리소스" 탭 클릭 및 리소스 수집
        affected_resources_tab_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[2]/div/div/div/div/div/div/div[1]/div/ul/li[2]/div/button"
        affected_resources_link_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[2]/div/div/div/div/div/div/div[2]/div[2]/div/section/div/div/div[2]/div/div[1]/table/tbody/tr/td[1]/div/a"
        affected_resources_text_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[2]/div/div/div/div/div/div/div[2]/div[2]/div/section/div/div/div[2]/div/div[1]/table/tbody/tr/td[1]/div/span"
        
        print(f"{client['name']} - 영향받는 리소스 탭 확인 중...")
        affected_resources_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, affected_resources_tab_xpath))
        )
        affected_resources_button.click()
        time.sleep(2)

        # 리소스 수집
        affected_resources = []
        
        # 링크가 있는 리소스
        link_elements = driver.find_elements(By.XPATH, affected_resources_link_xpath)
        for elem in link_elements:
            resource_text = elem.text.strip()
            resource_link = elem.get_attribute("href")
            if resource_text:
                affected_resources.append({"text": resource_text, "link": resource_link})
        
        # 텍스트만 있는 리소스
        text_elements = driver.find_elements(By.XPATH, affected_resources_text_xpath)
        for elem in text_elements:
            resource_text = elem.text.strip()
            if resource_text:
                affected_resources.append({"text": resource_text, "link": None})

        print(f"{client['name']} - 영향받는 리소스 수집 완료: {len(affected_resources)}개")
        return affected_resources


# 각 계정 처리를 위한 함수
def process_account(client, chromedriver_path):
    print(f"{client['name']} 계정 처리 시작...")
    
    options = webdriver.ChromeOptions()
    options.headless = False
    options.add_experimental_option("detach", True) # 브라우저 종료 방지
    options.add_argument('--start-maximized')
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
        # 섹션별 XPath 정의
        unresolved_issues_count_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[1]/div/a/span/span/span/span"
        unresolved_issues_button_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[1]/div/a"
        scheduled_changes_count_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[2]/div/a/span/span/span/span"
        scheduled_changes_button_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[2]/div/a"
        other_notifications_count_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[3]/div/a/span/span/span/span"
        other_notifications_button_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[1]/div[2]/div/div[1]/div/ul/li[3]/div/a"
        tbody_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[1]/div/div/div[3]/div[1]/div[2]/div/div/section/div/div/div[2]/div/div[1]/table/tbody"
        detail_xpath = "/html/body/div[2]/div[2]/div/div[1]/div/div/div/main/div[2]/section/div/div[2]/div[2]/div/div/div/div/div/div/div[2]/div[1]/div/div/div[2]/div"

        # 각 섹션 처리
        unresolved_count, unresolved_events = get_count_and_events(client, driver, "미해결 문제", unresolved_issues_count_xpath, unresolved_issues_button_xpath, tbody_xpath, detail_xpath)
        scheduled_count, scheduled_events = get_count_and_events(client, driver, "예정된 변경 사항", scheduled_changes_count_xpath, scheduled_changes_button_xpath, tbody_xpath, detail_xpath)
        other_count, other_events = get_count_and_events(client, driver, "기타 알림", other_notifications_count_xpath, other_notifications_button_xpath, tbody_xpath, detail_xpath)

        # 결과 구성
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
        return result

    except Exception as e:
        print(f"{client['name']} 오류 발생: {str(e)}")
        driver.save_screenshot(f"error_{client['name']}.png")
        return {"name": client["name"], "error": str(e)}

    finally:
        driver.quit()

# 메인 실행 로직
def main():
    with open('data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    clients = data["clients"]
    if not clients:
        print("클라이언트 정보가 없습니다.")
        return

    chromedriver_path = os.path.join(os.getcwd(), "chromedriver.exe")
    results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_client = {executor.submit(process_account, client, chromedriver_path): client for client in clients}
        for future in as_completed(future_to_client):
            client = future_to_client[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"{client['name']} 처리 중 예외 발생: {str(e)}")

    # JSON 파일로 저장
    with open('aws_events.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("결과가 'aws_events.json' 파일에 저장되었습니다.")

if __name__ == "__main__":
    main()