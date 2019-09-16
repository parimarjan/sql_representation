import glob
from sql_rep.query import *
import argparse

def read_flags():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_name", type=str, required=False,
            default="imdb")
    parser.add_argument("--db_host", type=str, required=False,
            default="localhost")
    parser.add_argument("--user", type=str, required=False,
            default="imdb")
    parser.add_argument("--pwd", type=str, required=False,
            default="")
    parser.add_argument("--port", type=str, required=False,
            default=5432)
    return parser.parse_args()


args = read_flags()
# simple testing script
fns = list(glob.glob("./test_sqls/*"))
print(fns)
sqls = []
for fn in fns:
    if ".sql" in fn:
        with open(fn, "r") as f:
            sql = f.read()
            sqls.append(sql)

sql_json = parse_sql(sqls[1], args.user, args.db_name,
        args.db_host, args.port, args.pwd)
