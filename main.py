import glob
import json
from sql_rep.query import *
import argparse
import re

def read_flags():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_name", type=str, required=False,
            default="imdb")
    parser.add_argument("--db_host", type=str, required=False,
            default="localhost")
    parser.add_argument("--user", type=str, required=False,
            default="ubuntu")
    parser.add_argument("--pwd", type=str, required=False,
            default="")
    parser.add_argument("--port", type=str, required=False,
            default=5432)
    return parser.parse_args()



q_num = re.compile(".*/([0-9]+[a-z])\\.sql.*")

args = read_flags()
# simple testing script
fns = list(glob.glob("./test_sqls/*"))
for fn in fns:
    if ".sql" in fn:
        sql_id = q_num.match(fn).group(1)
        with open(fn, "r") as f:
            sql = f.read()
        print("Processing", sql_id)
        sql_json = parse_sql(sql, args.user, args.db_name,
                             args.db_host, args.port, args.pwd,
                             compute_ground_truth=False)
        print(sql_json.keys())
        break
        with open(f"parsed/{sql_id}.json", "w") as f:
            json.dump(sql_json, f)
