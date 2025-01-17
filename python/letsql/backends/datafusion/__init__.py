import ibis.expr.operations as ops
from ibis.backends.datafusion import Backend as IbisDatafusionBackend


class Backend(IbisDatafusionBackend):
    def _register_in_memory_table(self, op: ops.InMemoryTable) -> None:
        self.con.from_arrow(op.data.to_pyarrow(op.schema), op.name)
