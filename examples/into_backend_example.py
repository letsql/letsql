import itertools

import ibis

from letsql.common.caching import SourceStorage
from letsql.expr.relations import into_backend, RemoteTable

from ibis.expr import operations as ops

import letsql as ls

con = ls.connect()
ddb = ibis.duckdb.connect()
pg = ls.postgres.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="ibis_testing",
)

t = into_backend(pg.table("batting"), con, "ls_batting")

expr = (
    t.join(t, "playerID")
    .limit(15)
    .select(player_id="playerID", year_id="yearID_right")
    .cache(SourceStorage(source=con))
)


class Replacer:
    def __init__(self):
        self.tables = {}
        self.count = itertools.count()

    def __call__(self, node, _, **kwargs):
        for k, v in list(kwargs.items()):
            try:
                if v in self.tables:
                    name = f"{v.name}_{next(self.count)}"
                    kwargs[k] = ops.DatabaseTable(
                        name, schema=v.schema, source=v.source, namespace=v.namespace
                    )
                    remote: RemoteTable = self.tables[v]
                    batches = remote.remote_expr.to_pyarrow_batches()
                    remote.source.register(batches, table_name=name)

            except TypeError:  # v may not be hashable
                continue

        node = node.__recreate__(kwargs)
        if isinstance(node, RemoteTable):
            result = ops.DatabaseTable(
                node.name,
                schema=node.schema,
                source=node.source,
                namespace=node.namespace,
            )
            self.tables[result] = node
            return result
        return node


# print(expr)
# op = expr.op()
# out = op.replace(Replacer())
# expr = out.to_expr()
res = expr.compile(pretty=True)
print(res)
print(expr.execute())
print(con.list_tables())
