from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

import os, time
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
from datetime import datetime
from multiprocessing import Pool

# 크롤링할 페이지 범위 설정
start_page = 200
end_page = 475

# 전역 설정 변수
START_DATE = time.strptime("2025.11.17 00:00:00", "%Y.%m.%d %H:%M:%S")
END_DATE = time.strptime("2025.11.17 23:59:59", "%Y.%m.%d %H:%M:%S")
BASE = "https://gall.dcinside.com/mgallery/board/lists"

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def parse_article_date(date_element):
    """게시글 날짜 파싱"""
    if not date_element:
        return None
    
    date_title = date_element.get('title')
    
    if date_title:  # title 속성이 있는 경우
        date_str = date_title.replace('-', '.').replace(' ', ' ')
        article_date = time.strptime(date_str, "%Y.%m.%d %H:%M:%S")
        c_date = date_title[:19]
        return article_date, c_date
    else:  # title 속성이 없는 경우
        date_text = date_element.text.strip()
        if ':' in date_text:
            hour, minute = date_text.split(':')
            today = time.localtime()
            date_str = f"{today.tm_year}.{today.tm_mon:02d}.{today.tm_mday:02d} {hour}:{minute}"
            article_date = time.strptime(date_str, "%Y.%m.%d %H:%M")
            c_date = date_str.replace('.', '-')
            return article_date, c_date
    return None

def crawl_page(page_num):
    """
    단일 페이지 크롤링 함수 (각 프로세스가 독립적으로 실행)
    """
    print(f"프로세스 {os.getpid()}: 페이지 {page_num} 크롤링 시작")
    driver = create_driver()
    
    # 각 프로세스의 수집 데이터
    writer_list = []
    title_list = []
    contents_list = []
    contents_date_list = []
    gall_no_list = []
    reply_id = []
    reply_content = []
    reply_date = []
    
    try:
        BASE_URL = BASE + "?id=stockus&page=" + str(page_num)
        
        try:
            driver.get(BASE_URL)
            sleep(1)
        except:
            print(f"페이지 {page_num} 로드 실패")
            return None
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        tbody = soup.find('tbody')
        if not tbody:
            print(f"페이지 {page_num}에서 tbody를 찾을 수 없습니다")
            return None
        
        article_list = tbody.find_all('tr')
        
        # 수집할 게시글이 있는지 확인
        has_target_article = False
        for article in article_list:
            date_element = article.find('td', class_='gall_date')
            if not date_element:
                continue
            
            date_info = parse_article_date(date_element)
            if not date_info:
                continue
            
            article_date, _ = date_info
            if START_DATE <= article_date <= END_DATE:
                has_target_article = True
                break
        
        if not has_target_article:
            print(f"페이지 {page_num}: 수집할 게시글 없음")
            return None
        
        # 게시글 수집
        for article in article_list:
            try:
                # 날짜 확인
                date_element = article.find('td', class_='gall_date')
                if not date_element:
                    continue
                
                date_info = parse_article_date(date_element)
                if not date_info:
                    continue
                
                article_date, c_date = date_info
                
                # 날짜 범위에 포함되지 않으면 건너뛰기
                if not (START_DATE <= article_date <= END_DATE):
                    continue
                
                # 게시글 제목
                title_tag = article.find('a')
                if not title_tag:
                    continue
                title = title_tag.text
                
                # 게시글 종류
                head_tag = article.find('td', {"class": "gall_subject"})
                if not head_tag:
                    continue
                head = head_tag.text
                
                if head not in ['설문', 'AD', '공지']:
                    # 게시글 번호
                    gall_num_tag = article.find("td", {"class": "gall_num"})
                    if not gall_num_tag:
                        continue
                    gall_id = gall_num_tag.text
                    
                    # 게시글 주소
                    tag = article.find('a', href=True)
                    if not tag:
                        continue
                    content_url = "https://gall.dcinside.com" + tag['href']
                    
                    # 게시글 내용 수집
                    try:
                        driver.get(content_url)
                        sleep(1)
                        contents_soup = BeautifulSoup(driver.page_source, "html.parser")
                        
                        write_div = contents_soup.find('div', {"class": "write_div"})
                        contents = write_div.text.strip() if write_div else ""
                    except Exception as e:
                        print(f"페이지 {page_num} - 게시글 {gall_id} 로드 실패")
                        continue
                    
                    # 게시글 정보 저장
                    writer_list.append(gall_id)
                    title_list.append(title)
                    contents_list.append(contents)
                    contents_date_list.append(c_date)
                    
                    print(f"페이지 {page_num} - 수집: {gall_id} | {title[:20]}... | {c_date}")
                    
                    # 댓글 수집
                    reply_no = contents_soup.find_all("li", {"class": "ub-content"})
                    if len(reply_no) > 0:
                        for r in reply_no:
                            try:
                                user_name_tag = r.find("em")
                                user_name = user_name_tag.text if user_name_tag else ""
                                
                                user_reply_date_tag = r.find("span", {"class": "date_time"})
                                user_reply_date = user_reply_date_tag.text if user_reply_date_tag else ""
                                
                                user_reply_tag = r.find("p", {"class": "usertxt ub-word"})
                                user_reply = user_reply_tag.text if user_reply_tag else ""
                                
                                gall_no_list.append(gall_id)
                                reply_id.append(user_name)
                                reply_date.append(user_reply_date)
                                reply_content.append(user_reply)
                            except:
                                continue
            
            except Exception as e:
                print(f"페이지 {page_num} - 게시글 처리 중 오류: {str(e)}")
                continue
    
    finally:
        driver.quit()
    
    print(f"프로세스 {os.getpid()}: 페이지 {page_num} 완료 (게시글 {len(writer_list)}개, 댓글 {len(reply_content)}개)")
    
    return {
        'contents': {
            'id': writer_list,
            'title': title_list,
            'content': contents_list,
            'date': contents_date_list
        },
        'replies': {
            'id': gall_no_list,
            'reply_id': reply_id,
            'reply_content': reply_content,
            'date': reply_date
        }
    }

def main():
    """
    메인 함수: multiprocessing을 사용하여 여러 페이지를 병렬로 크롤링
    """
    # 사용할 프로세스 수)
    num_processes = 8
    print(f"\n{'='*60}")
    print(f"Multiprocessing 크롤링 시작")
    print(f"{'='*60}")
    print(f"프로세스 수: {num_processes}개")
    print(f"페이지 범위: {start_page} ~ {end_page}")
    print(f"수집 기간: {time.strftime('%Y.%m.%d %H:%M:%S', START_DATE)} ~ {time.strftime('%Y.%m.%d %H:%M:%S', END_DATE)}")
    print(f"{'='*60}\n")
    
    # 페이지 번호 리스트 생성
    page_numbers = list(range(start_page, end_page + 1))
    
    # multiprocessing Pool을 사용하여 병렬 크롤링
    with Pool(processes=num_processes) as pool:
        results = pool.map(crawl_page, page_numbers)
    
    # None이 아닌 결과만 필터링
    results = [r for r in results if r is not None]
    
    # 모든 프로세스의 결과를 병합
    all_contents = {
        'id': [],
        'title': [],
        'content': [],
        'date': []
    }
    
    all_replies = {
        'id': [],
        'reply_id': [],
        'reply_content': [],
        'date': []
    }
    
    for result in results:
        all_contents['id'].extend(result['contents']['id'])
        all_contents['title'].extend(result['contents']['title'])
        all_contents['content'].extend(result['contents']['content'])
        all_contents['date'].extend(result['contents']['date'])
        
        all_replies['id'].extend(result['replies']['id'])
        all_replies['reply_id'].extend(result['replies']['reply_id'])
        all_replies['reply_content'].extend(result['replies']['reply_content'])
        all_replies['date'].extend(result['replies']['date'])
    
    # DataFrame 생성
    contents_df = pd.DataFrame(all_contents)
    reply_df = pd.DataFrame(all_replies)
    
    # 기존 파일이 있으면 불러와서 합치기
    if os.path.exists("contents.csv"):
        existing_contents = pd.read_csv("contents.csv", encoding='utf8')
        contents_df = pd.concat([existing_contents, contents_df], ignore_index=True)
    
    if os.path.exists("reply.csv"):
        existing_reply = pd.read_csv("reply.csv", encoding='utf8')
        reply_df = pd.concat([existing_reply, reply_df], ignore_index=True)
    
    # 게시글, 댓글 중복 제거
    contents_df = contents_df.drop_duplicates(subset=['id', 'title', 'content', 'date'], keep='last')
    reply_df = reply_df.drop_duplicates(subset=['id', 'reply_id', 'reply_content', 'date'], keep='last')

    os.makedirs('./data', exist_ok=True)

    # CSV 저장
    contents_df.to_csv("./data/contents.csv", encoding='utf8', index=False)
    reply_df.to_csv("./data/reply.csv", encoding='utf8', index=False)
    
    print(f"\n{'='*60}")
    print("크롤링 완료!")
    print(f"{'='*60}")
    print(f"총 게시글 수: {len(contents_df)}개")
    print(f"총 댓글 수: {len(reply_df)}개")
    print(f"저장 파일: contents.csv, reply.csv")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    main()