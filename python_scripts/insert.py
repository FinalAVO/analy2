import redis, sys
import json
import mysql.connector


user_id = sys.argv[1]

with open('/analy2/config/redis.json') as f:
    redis_config = json.load(f)
    f.close()

r = redis.Redis(
    host = redis_config["host"],
    port = redis_config["port"],
    password = redis_config["password"]
    )

json_whole_result = r.get(user_id).decode('utf-8')
whole_result = dict(json.loads(json_whole_result))

# RDS 연결 - 주제별 단어들 가져오기 위함

with open('/analy2/config/rds.json') as f:
    rds_config = json.load(f)
    f.close()

config = {
  'user': rds_config["user"],
  'password': rds_config["password"],
  'host': rds_config["host"],
  'database': rds_config["database"],
  'raise_on_warnings': rds_config["raise_on_warnings"]
}
cnx = mysql.connector.connect(**config)

# insert into star_share_report
cursor = cnx.cursor()

query_1 = ("INSERT INTO star_share_report "
        "(app_name, one_star, two_star, three_star, four_star, five_star, count, user_id, start_date, end_date) "
        "VALUES (%(APP_NAME)s, %(ONE_STAR)s, %(TWO_STAR)s, %(THREE_STAR)s, %(FOUR_STAR)s, %(FIVE_STAR)s, %(COUNT)s, %(user_id)s, %(start_date)s, %(end_date)s)")

cursor.execute(query_1, whole_result)
cnx.commit()

# insert into star_report

subject_name = ["DESIGN", "PROFILE", "RESOURCE", "SPEED", "SAFETY", "REMOVE", "UPDATE"]
subject_name_cnt = ["DESIGN_COUNT", "PROFILE_COUNT", "RESOURCE_COUNT", "SPEED_COUNT",
                    "SAFETY_COUNT", "REMOVE_COUNT", "UPDATE_COUNT"]

cursor = cnx.cursor()

for idx in range(len(subject_name)):
    query_2 = ("INSERT INTO star_report "
        "(app_name, subject, avg_star, count, user_id, start_date, end_date) "
        "VALUES (%(APP_NAME)s, " + '"' + subject_name[idx] + '"' + ", %(" + subject_name[idx] + ")s, %(" + subject_name_cnt[idx] + ")s, %(user_id)s, %(start_date)s, %(end_date)s)")

    cursor.execute(query_2, whole_result)
    cnx.commit()

cnx.close()
print("Insert RDS Done")
