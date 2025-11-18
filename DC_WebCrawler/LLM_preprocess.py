from openai import OpenAI
import pandas as pd
import os
import time
from tqdm import tqdm

# ===== OpenAI API 설정 =====
# API 키를 읽어오기
try:
    with open("api_key.txt", "r", encoding="utf-8") as f:
        OPENAI_API_KEY = f.read().strip()
except FileNotFoundError:
    print("api_key.txt 파일을 찾을 수 없습니다.")
    print("DC_WebCrawler 폴더에 api_key.txt 파일을 생성하고 OpenAI API 키를 저장하세요.")
    exit(1)

if not OPENAI_API_KEY:
    print("API 키가 비어있습니다. api_key.txt 파일에 OpenAI API 키를 입력하세요.")
    exit(1)

client = OpenAI(api_key=OPENAI_API_KEY)

# ===== 프롬프트 템플릿 =====
SENTIMENT_PROMPT = """
당신은 미국 주식 투자 전문가이자 디시인사이드 갤러리 텍스트 데이터 분석 전문가입니다.
다양한 사람이 이용하는 디시인사이드 커뮤니티에서 추출한 데이터이므로 비속어, 반어법, 직설법이 혼용됐다는 특징이 있습니다.
커뮤니티의 특성을 고려해 다음 게시글/댓글의 내용을 철저하게 분석하여 매수 의견인지 매도 의견인지 판단해주세요.

분석 규칙:
1. 긍정적/낙관적/상승 예상/매수 추천 → 1 (매수)
2. 중립적이거나 판단하기 애매한 경우 → 0 (중립)
3. 부정적/비관적/하락 예상/매도 추천 → -1 (매도)

**중요**: 반드시 정수 숫자 하나만 출력하세요 (1, 0, 또는 -1).
다른 설명이나 텍스트 없이 숫자만 반환하세요.

분석할 내용:
{text}

출력 (숫자만):"""

def analyze_sentiment(text, retry_count=3):
    """
    ChatGPT API를 사용하여 텍스트의 감성 분석
    
    Args:
        text (str): 분석할 텍스트
        retry_count (int): 재시도 횟수
    
    Returns:
        int: 1 (매수), 0 (중립), -1 (매도/오류)
    """
    # 빈 텍스트 처리
    if not text or str(text).strip() == '' or pd.isna(text):
        return 0
    
    # 너무 짧은 텍스트 처리
    if len(str(text).strip()) < 5:
        return 0
    
    # API 호출 시도
    for attempt in range(retry_count):
        try:
            prompt = SENTIMENT_PROMPT.format(text=text)
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a US stock market expert analyst."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=10
            )
            
            # 응답에서 숫자 추출
            result_text = response.choices[0].message.content.strip()
            
            # 숫자만 추출
            if '1' in result_text and '-1' not in result_text:
                return 1
            elif '-1' in result_text:
                return -1
            elif '0' in result_text:
                return 0
            else:
                # 파싱 실패 시 재시도
                if attempt < retry_count - 1:
                    time.sleep(1)
                    continue
                return 0
                
        except Exception as e:
            print(f"API 호출 오류 (시도 {attempt + 1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(2)  # 재시도 전 대기
            else:
                return 0  # 모든 시도 실패 시 중립 반환
    
    return 0

def process_contents(input_file="./data/contents.csv", output_file="./data/contents_labeled.csv", batch_size=10, start_idx=0):
    """
    게시글 데이터 처리 및 라벨링
    
    Args:
        input_file (str): 입력 CSV 파일 경로
        output_file (str): 출력 CSV 파일 경로
        batch_size (int): 중간 저장 배치 크기
        start_idx (int): 시작 인덱스 (중단된 경우 이어서 시작)
    """
    print("="*60)
    print("게시글 데이터 라벨링 시작")
    print("="*60)
    
    # 데이터 로드
    df = pd.read_csv(input_file, encoding='utf8')
    print(f"총 {len(df)}개의 게시글 로드됨")
    
    # 이미 처리된 데이터가 있는지 확인
    if os.path.exists(output_file) and start_idx == 0:
        existing_df = pd.read_csv(output_file, encoding='utf8')
        print(f"기존 라벨링 데이터 발견: {len(existing_df)}개")
        user_input = input("기존 데이터를 이어서 진행하시겠습니까? (y/n): ")
        if user_input.lower() == 'y':
            start_idx = len(existing_df)
            df = pd.concat([existing_df, df.iloc[start_idx:]], ignore_index=True)
            print(f"{start_idx}번째부터 이어서 진행합니다.")
    
    # label 컬럼이 없으면 추가
    if 'label' not in df.columns:
        df['label'] = 0
    
    # 라벨링 수행
    print(f"\n{start_idx}번째 게시글부터 라벨링 시작...")
    
    for idx in tqdm(range(start_idx, len(df)), desc="게시글 라벨링"):
        content = df.loc[idx, 'content']
        title = df.loc[idx, 'title']
        
        # 제목 + 내용 결합
        full_text = f"제목: {title}\n내용: {content}"
        
        # 감성 분석
        label = analyze_sentiment(full_text)
        df.loc[idx, 'label'] = label
        
        # 배치마다 중간 저장
        if (idx + 1) % batch_size == 0:
            df.to_csv(output_file, encoding='utf8', index=False)
            print(f"\n중간 저장 완료: {idx + 1}개 처리됨")
        
        # API 호출 제한 대응 (초당 요청 수 제한)
        time.sleep(0.01)  # 0.5초 대기 (분당 약 120개 처리)
    
    # 최종 저장
    df.to_csv(output_file, encoding='utf8', index=False)
    
    # 결과 통계
    print("\n" + "="*60)
    print("게시글 라벨링 완료!")
    print("="*60)
    print(f"총 처리: {len(df)}개")
    print(f"매수(1): {(df['label'] == 1).sum()}개")
    print(f"중립(0): {(df['label'] == 0).sum()}개")
    print(f"매도(-1): {(df['label'] == -1).sum()}개")
    print(f"저장 위치: {output_file}")
    print("="*60)

def process_replies(input_file="./data/reply.csv", output_file="./data/reply_labeled.csv", batch_size=10, start_idx=0):
    """
    댓글 데이터 처리 및 라벨링
    
    Args:
        input_file (str): 입력 CSV 파일 경로
        output_file (str): 출력 CSV 파일 경로
        batch_size (int): 중간 저장 배치 크기
        start_idx (int): 시작 인덱스
    """
    print("="*60)
    print("댓글 데이터 라벨링 시작")
    print("="*60)
    
    # 데이터 로드
    df = pd.read_csv(input_file, encoding='utf8')
    print(f"총 {len(df)}개의 댓글 로드됨")
    
    # 이미 처리된 데이터가 있는지 확인
    if os.path.exists(output_file) and start_idx == 0:
        existing_df = pd.read_csv(output_file, encoding='utf8')
        print(f"기존 라벨링 데이터 발견: {len(existing_df)}개")
        user_input = input("기존 데이터를 이어서 진행하시겠습니까? (y/n): ")
        if user_input.lower() == 'y':
            start_idx = len(existing_df)
            df = pd.concat([existing_df, df.iloc[start_idx:]], ignore_index=True)
            print(f"{start_idx}번째부터 이어서 진행합니다.")
    
    # label 컬럼이 없으면 추가
    if 'label' not in df.columns:
        df['label'] = 0
    
    # 라벨링 수행
    print(f"\n{start_idx}번째 댓글부터 라벨링 시작...")
    
    for idx in tqdm(range(start_idx, len(df)), desc="댓글 라벨링"):
        reply_content = df.loc[idx, 'reply_content']
        
        # 감성 분석
        label = analyze_sentiment(reply_content)
        df.loc[idx, 'label'] = label
        
        # 배치마다 중간 저장
        if (idx + 1) % batch_size == 0:
            df.to_csv(output_file, encoding='utf8', index=False)
            print(f"\n중간 저장 완료: {idx + 1}개 처리됨")
        
        # API 호출 제한 대응
        time.sleep(0.01)
    
    # 최종 저장
    df.to_csv(output_file, encoding='utf8', index=False)
    
    # 결과 통계
    print("\n" + "="*60)
    print("댓글 라벨링 완료!")
    print("="*60)
    print(f"총 처리: {len(df)}개")
    print(f"매수(1): {(df['label'] == 1).sum()}개")
    print(f"중립(0): {(df['label'] == 0).sum()}개")
    print(f"매도(-1): {(df['label'] == -1).sum()}개")
    print(f"저장 위치: {output_file}")
    print("="*60)

def main():
    """메인 함수"""
    print("\n" + "="*60)
    print("ChatGPT API를 사용한 감성 분석 및 라벨링")
    print("="*60)
    print("1: 게시글 라벨링")
    print("2: 댓글 라벨링")
    print("3: 게시글 + 댓글 모두 라벨링")
    print("="*60)
    
    choice = input("선택하세요 (1/2/3): ").strip()
    
    if choice == '1':
        process_contents()
    elif choice == '2':
        process_replies()
    elif choice == '3':
        print("\n게시글 라벨링을 시작합니다...\n")
        process_contents()
        print("\n\n댓글 라벨링을 시작합니다...\n")
        process_replies()
    else:
        print("잘못된 선택입니다.")

if __name__ == '__main__':
    main()