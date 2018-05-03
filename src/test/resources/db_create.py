# Requires Python 3.5+, psycopg2
import psycopg2
import urllib.request
import os

database_env_name = "DATABASE_ENV_NAME"
database_env_host = "DATABASE_ENV_HOST"
database_env_port = "DATABASE_ENV_PORT"
database_env_user = "DATABASE_ENV_USER"
database_env_pass = "DATABASE_ENV_PASS"
database_env_table = "DATABASE_ENV_TABLE"


database_name = os.getenv(database_env_name)
database_host = os.getenv(database_env_host)
database_port = os.getenv(database_env_port)
database_user = os.getenv(database_env_user)
database_pass = os.getenv(database_env_pass)
database_table = os.getenv(database_env_table)


connection_string = "dbname={} user={} password={} host={} port={}" \
    .format(database_env_name, database_user, database_pass, database_host, database_port)

conn = psycopg2.connect(connection_string)

skin_txt = "https://archive.ics.uci.edu/ml/machine-learning-databases/00229/Skin_NonSkin.txt"
txt = urllib.request.urlopen(skin_txt).read().decode("UTF-8")
file = txt.split("\n")

cur = conn.cursor()
create_sql = "CREATE TABLE {} (id serial PRIMARY KEY, cb integer, cg integer, cr integer, " \
             "label varchar);".format(database_table)
cur.execute(create_sql)

insertSQl = "INSERT INTO {} (cb, cg, cr, label) VALUES (%s, %s, %s, %s)".format(database_table)

for line in file:
    xplit = line[:-1].split("\t")
    print(xplit)
    if len(xplit) != 4:
        continue
    cb = int(xplit[0])
    cg = int(xplit[1])
    cr = int(xplit[2])
    label = xplit[3]
    cur.execute(insertSQl, (cb, cg, cr, label))


conn.commit()
cur.close()
conn.close()

