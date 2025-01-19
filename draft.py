import pandas as pd
import ibis
import letsql as ls

from letsql.expr.datatypes import LargeString
from ibis.expr.datatypes import Int64

df = pd.DataFrame(
    data=[("A", 1), ("B", 2), ("A", 2), ("A", 4), ("C", 1), ("A", 1)],
    columns=["name", "age"],
)

con = ls.connect()
t = con.create_table(
    "names", df, schema=ibis.schema([("name", LargeString), ("age", Int64)])
)
print(t.schema())
print(ls.execute(t))

# data = """
# create table utf8_data(str string, val bigint) as values
#   ('A', 1),
#   ('B', 2),
#   ('A', 2),
#   ('A', 4),
#   ('C', 1),
#   ('A', 1);
# """
#
# con.con.sql(data)
#
# large_data = """
# create table largeutf8_data as
# select arrow_cast(str, 'LargeUtf8') as str, val
# from utf8_data;
# """
#
# con.con.sql(large_data)
#
# print(con.list_tables())
# print(con.con.table('largeutf8_data').schema())
# print(con.table('largeutf8_data'))
