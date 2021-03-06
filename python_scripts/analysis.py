import pymongo
from pymongo import MongoClient
import redis
import pandas as pd
from datetime import datetime
import json
import time
import logging
import threading
import time, sys

## subject_count multi-threading class
class subject_count(threading.Thread):
    def __init__(self, name, subject_li, df):
        threading.Thread.__init__(self)
        self.name = name
        self.subject_li = subject_li
        self.df = df

    def run(self):
        total_cnt = 0
        star_sum = 0
        index_li = []
        subject_name_cnt = self.name + "_COUNT"

        for word in self.subject_li:
            df_dropped = self.df.drop(index_li)
            df_filter = df_dropped[df_dropped["COMMENT"].str.contains(word, regex=False) == True]
            index_li += df_filter.index.tolist()
            total_cnt += df_filter["STAR"].count()
            star_sum += df_filter["STAR"].sum()

        if total_cnt == 0:
            avg_star = float(0)
        else:
            avg_star = float(round(star_sum / total_cnt, 2))

        whole_result[self.name] = avg_star
        whole_result[subject_name_cnt] = int(total_cnt)


## mongo 연결

collection_name = sys.argv[1]
user_id = sys.argv[2]
start_date = sys.argv[3]
end_date = sys.argv[4]

with open('/analy2/config/mongo.json') as f:
    mongo_config = json.load(f)
    f.close()

client = MongoClient(mongo_config["host"])
db = client["review"]
db_col = db[collection_name]

# 기간 필터 걸어서 원하는 document 불러온 후 df 생성
df_review = pd.DataFrame(list(db_col.find({'DATE': {'$gte': start_date, '$lte': end_date}}).sort('DATE', -1)))

client.close()

# 커맨트 개수, 기간안의 평균 평점, 1~5점 개수

whole_result = {}

whole_name = ["APP_NAME", "START_DATE", "END_DATE", "COUNT", "AVG_STAR",
              "ONE_STAR", "TWO_STAR", "THREE_STAR", "FOUR_STAR", "FIVE_STAR", "user_id"]

whole_cnt = int(df_review["STAR"].count())
whole_avg = round(df_review["STAR"].mean(), 2)
whole_one = int(df_review[df_review["STAR"] == 1]["STAR"].count())
whole_two = int(df_review[df_review["STAR"] == 2]["STAR"].count())
whole_three = int(df_review[df_review["STAR"] == 3]["STAR"].count())
whole_four = int(df_review[df_review["STAR"] == 4]["STAR"].count())
whole_five = int(df_review[df_review["STAR"] == 5]["STAR"].count())

whole_list = [collection_name, start_date, end_date, whole_cnt, whole_avg, whole_one, whole_two , whole_three, whole_four, whole_five, user_id]

for i in range(len(whole_name)):
    whole_result[whole_name[i]] = whole_list[i]

design_li = ["그래픽"]
profile_li = ["로그인", "로그아웃", "가입", "탈퇴"]
resource_li = ["배터리", "메모리", "데이터"]
speed_li = ["지연", "느림", "빠름"]
safety_li = ["다운", "버그", "정지"]
remove_li = ["제거", "제거함"]
update_li = ["버전", "업데이트"]

format = "%(asctime)s: %(message)s"  # Logging format 설정
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")


a = subject_count('DESIGN', design_li, df_review)
b = subject_count('PROFILE', profile_li, df_review)
c = subject_count('RESOURCE', resource_li, df_review)
d = subject_count('SPEED', speed_li, df_review)
e = subject_count('SAFETY', safety_li, df_review)
f = subject_count('REMOVE', remove_li, df_review)
g = subject_count('UPDATE', update_li, df_review)

# logging.info("[Main-Thread] 쓰레드 시작 전")

a.start()  # 서브 스레드 시작
b.start()
c.start()
d.start()
e.start()
f.start()
g.start()

a.join()  # 서브 조인 시작
b.join()
c.join()
d.join()
e.join()
f.join()
g.join()

# logging.info("[Main-Thread] 프로그램을 종료합니다.")

## REDIS

with open('/analy2/config/redis.json') as f:
    redis_config = json.load(f)
    f.close()

r = redis.Redis(
    host = redis_config["host"],
    port = redis_config["port"],
    password = redis_config["password"]
    )

json_whole_result = json.dumps(whole_result, ensure_ascii=False).encode('utf-8')
r.set(user_id, json_whole_result)
r.expire(user_id, 1200)

print("Insert REDIS Done")
