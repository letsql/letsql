import itertools
import pyarrow as pa

from letsql.executor.utils import SafeTee
from ibis.expr.operations import DatabaseTable


class Collected:
    def __init__(self, node):
        self.node = node

    def value(self):
        return self.node


class RemoteTableCollected(Collected):
    def __init__(self, node):
        from letsql import to_pyarrow_batches

        self.counter = itertools.count()
        self.keep = to_pyarrow_batches(node.remote_expr)
        super().__init__(node)

    def _get_batches(self, batches):
        schema = self.node.remote_expr.as_table().schema()
        return pa.RecordBatchReader.from_batches(schema.to_pyarrow(), batches)

    def value(self):
        from letsql.backends.postgres import Backend as PGBackend

        batches, keep = SafeTee.tee(self.keep, 2)
        batches = self._get_batches(batches)
        name = f"clone_{next(self.counter)}_{self.node.name}"
        result = DatabaseTable(
            name,
            schema=self.node.schema,
            source=self.node.source,
            namespace=self.node.namespace,
        )
        if isinstance(self.node.source, PGBackend):
            self.node.source.read_record_batches(batches, table_name=name)
        else:
            self.node.source.register(batches, table_name=name)
        self.keep = keep
        return result
