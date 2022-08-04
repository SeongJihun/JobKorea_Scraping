## Airflow 관련 모듈 ##
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from airflow.models.connection import Connection
from airflow.models import Variable

## Airflow operater 모듈
from airflow.models import DAG
from airflow.operators.python import PythonOperator

## Connection 관련 모듈
import pymongo
from pymongo import MongoClient
from boto3.session import Session, Config
import psycopg2
import hashlib


# 스크래핑 관련 모듈
import time
import requests
import random
import urllib.request
from bs4 import BeautifulSoup
import re

default_args = {
    'owner': '성지훈 인턴사원',
    'start_date': days_ago(1),
}

my_client = MongoClient("mongodb://localhost:27017/")
mydb = my_client['scraping']
mycol = mydb['jobkorea_scraping']

# 잡코리아 채용 정보 사이트

proxies = {
    "http": "173.208.150.242:15002",
    "https": "173.208.150.242:15002"
}


def clear_text(text):
    text = re.sub('  |  | \n|\t|\r', '', text)
    text = re.sub(r"[^0-9가-힣?.!,¿]+", " ", text)
    text = re.sub('\n', '', text)
    return text


def classify_wait(tot):
    if 0 <= tot <= 200:
        wait = random.random()
    elif 200 < tot <= 1000:
        wait = random.random() * 2
    elif 1000 < tot <= 4000:
        wait = random.random() * 3
    elif 4000 < tot <= 10000:
        wait = random.random() * 4
    else:
        wait = random.random() * 5
    return wait


def get_pages(total_recruits):
    if total_recruits < 20:
        pages = 1
    elif (total_recruits / 20) / int(total_recruits / 20):
        pages = int(total_recruits / 20) + 1
    else:
        pages = int(total_recruits/20)
    return pages


def job_korea():
    keyword = str(input('검색할 키워드: '))
    begin_url = 'https://www.jobkorea.co.kr/Search/?stext=' + keyword
    first_page = requests.get(begin_url).text
    page_soup = BeautifulSoup(first_page, "html.parser")
    total_recruits = int(page_soup.find(class_='dev_tot').text.replace(",", ""))  # 채용 건수
    print("채용건수: " + str(total_recruits))  # 채용 건수 출력
    count = 0

    if total_recruits >= 500:
        total_recruits = 500

    url = 'https://www.jobkorea.co.kr/Search/?stext=' + keyword + '&tabType=recruit&Page_No='
    links = []

    for p in range(1, get_pages(total_recruits) + 1):
        page = requests.get(url + str(p), proxies=urllib.request.getproxies())
        soup = BeautifulSoup(page.text, "html.parser")
        titles = soup.find_all(class_='title dev_view', href=True)
        for title in titles:
            recruit_link = 'https://www.jobkorea.co.kr' + title.get('href')  # 채용 정보 링크
            links.append(recruit_link)
    for link in links:
        count += 1
        time.sleep(classify_wait(total_recruits))
        test_page = requests.get(link, proxies=urllib.request.getproxies()).text
        test_soup = BeautifulSoup(test_page, "html.parser")

        # 채용 정보 타이틀
        recruit_title = test_soup.find('title').text
        print("------------------------------------------")
        print(recruit_title)

        co_name = ''
        # 기업명 및 채용공고 url이자 동시에 exit 할 수 있는 탈출구
        if test_soup.find(class_='sumTit') is not None:
            co_name = test_soup.find(class_='sumTit').find(class_='coName').text.strip()
        else:
            exit()
        print("기업명 " + co_name)
        print("채용 공고 Url: " + link)

        # 기업정보 ex) 산업(업종분야) 사원수 설립년도 기업형태 매출액
        co_infos = {}
        co_dt = []
        co_dd = []
        co_dts = test_soup.find(class_='artReadCoInfo divReadBx').find(class_='tbCol coInfo').find_all('dt')
        co_dds = test_soup.find(class_='artReadCoInfo divReadBx').find(class_='tbCol coInfo').find_all('dd')
        for co_a in co_dts:
            co_dt.append(clear_text(co_a.text.strip()))
        for co_b in co_dds:
            co_dd.append(clear_text(co_b.text.strip()))
        for i in range(len(co_dds)):
            co_infos[co_dt[i]] = co_dd[i]
        print("기업 정보")

        indust_form = ''
        employee_cnt = ''
        fond_year = ''
        entrprs_form = ''
        samt = ''
        certification = ''
        if "산업" not in list(co_infos.keys()):
            pass
        else:
            indust_form = co_infos.get("산업")

        if "사원수" not in list(co_infos.keys()):
            pass
        else:
            employee_cnt = co_infos.get("사원수")[:-1]

        if "설립" not in list(co_infos.keys()):
            pass
        else:
            fond_year = co_infos.get("설립")[:4]

        if "기업형태" not in list(co_infos.keys()):
            pass
        else:
            entrprs_form = co_infos.get("기업형태").split(' ')[0]

        if "인증" not in list(co_infos.keys()):
            pass
        else:
            certification = co_infos.get("인증")

        if "매출액" not in list(co_infos.keys()):
            pass
        else:
            samt = co_infos.get("매출액")

        print("산업 : " + indust_form)  # 산업
        print("사원수 : " + employee_cnt)  # 사원수
        print("설립년도 : " + fond_year)  # 설립일
        print("기업형태 : " + entrprs_form)  # 기업형태
        print("인증 : " + certification)  # 인증
        print("매출액 : " + samt)  # 매출액

        # 접수 기간 정보
        # 상시 모집은 시작일과 마감일이 없다
        start_date = ''
        end_date = ''
        if test_soup.find(class_='date') is not None:
            start_date = test_soup.find(class_='date').find('dd')
            end_date = start_date.find_next('dd')
            start_date = start_date.text
            end_date = end_date.text
            print("시작일 : " + start_date)
            print("마감일 : " + end_date)

        # 근무환경 정보
        address = ''
        if test_soup.find(class_='address') is not None:
            address = test_soup.find(class_='address').find('strong').text
        print("주소 : " + address)

        keywords = []
        # 관련 키워드 정보
        if test_soup.find(class_='list extendBx').find_all('li') is not None:
            related_keyword = test_soup.find(class_='list extendBx').find_all('li')
            for keyword in related_keyword:
                keywords.append(keyword.text.strip())
            print("키워드 : ")
            print(keywords)
            time.sleep(classify_wait(total_recruits))

        print("")

        # 지원자 정보
        # 지원자 정보 예상 통계에 사용되는 url과 채용 정보 url 간의 상관관계 찾을 수 없음 -> Selenium 사용
        appli_stats = test_soup.find(class_='secReadStatistic')
        by_age_list = []
        by_age_dict = {}
        applicant_cnt = ''
        gathering_cnt = ''
        if appli_stats is not None:
            print(appli_stats.find(class_='hd_2').text)
            applicant_cnt = appli_stats.find(class_='metricsCount').find(class_='value').text
            gathering_cnt = appli_stats.find(class_='metricsRate').find(class_='value').text
            print("지원자수: " + applicant_cnt)
            print("모집인원: " + gathering_cnt)

            # 지원자 연령 정보
            ages = appli_stats.find(class_='chart chartCol_1 chartVertical')
            print("연령")

            for age in ages.select('li'):
                by_age_list.append(clear_text(age.text))
            by_age_dict["25세 이하"] = by_age_list[0].split(' ')[-2]
            by_age_dict["26세 이상 30세 이하"] = by_age_list[1].split(' ')[-2]
            by_age_dict["31세 이상 35세 이하 "] = by_age_list[2].split(' ')[-2]
            by_age_dict["36세 이상 40세 이하"] = by_age_list[3].split(' ')[-2]
            by_age_dict["41세 이상 45세 이하"] = by_age_list[4].split(' ')[-2]
            by_age_dict["46세 이상"] = by_age_list[5].split(' ')[-2]
            print(by_age_dict)

            male = ''
            female = ''
            # 지원자 성별 정보
            gender = {}
            gender_find = appli_stats.find(class_='chart chartCol_2 chartSex')
            male = gender_find.find(class_='item itemMan').find(class_='value').text
            female = gender_find.find(class_='item itemWoman').find(class_='value').text
            gender["남성"] = male
            gender["여성"] = female
            print("성별")
            print(gender)

            # 지원자 학력 정보
            print("학력: ")
            academic = appli_stats.find(class_='chart chartCol_3 chartVertical').find_all(class_='value')
            academic_list = []
            academic_list = []
            academic_dict = {}
            for aca in academic:
                academic_list.append(aca.text)
            academic_dict["고졸미만"] = academic_list[0]
            academic_dict["고졸(예정)"] = academic_list[1]
            academic_dict["초대졸(예정)"] = academic_list[2]
            academic_dict["대졸(예정)"] = academic_list[3]
            academic_dict["석박사(예졍)"] = academic_list[4]
            print(academic_dict)

            # 50개 링크 돌고 나서 sleep 걸어주기
            if (count % 50) == 0:
                time.sleep(30)

            temp_id = str(co_name + recruit_title)
            temp_id = temp_id.encode('utf-8')
            h = hashlib.new('md5')
            h.update(temp_id)
            _id = h.hexdigest()

            jobkorea_dict = {
                "_id": _id,
                "CMPNY_NM:": co_name,
                "TITLE": recruit_title,
                "SIT_URL": link,
                "INDUST_FORM": indust_form,
                "EMPLOYEE_CNT": employee_cnt,
                "FOND_YEAR": fond_year,
                "ENTRPRS_FORM": entrprs_form,
                "CRTFC": certification,
                "SAMT": samt,
                "VLNTR_CNT": applicant_cnt,
                "RCRIT_NMPR_CNT": gathering_cnt,
                "RK": keywords,
                "CFAGE_CNT": by_age_dict,
                "MALE_CNT": male,
                "FEMALE_CNT": female,
                "RCRIT_ACDMCRER_CNT": academic_dict,
                "ACPT_BGN_DE": start_date,
                "ACPT_END_DE": end_date,
                "CMPNY_ADRS": address
            }

            try:
                # scrap.insert_one(jobkorea_dict)
                mycol.insert_one(jobkorea_dict)
                # print('날짜: ', time.time())
            except pymongo.errors.DuplicateKeyError:
                print('중복된키 삽입')
                breaker = True
                break
            except Exception as e:
                print("Mongo insert ERROR :", e)
                continue

start = time.time()
job_korea()

print("걸린 시간 : ", time.time() - start)