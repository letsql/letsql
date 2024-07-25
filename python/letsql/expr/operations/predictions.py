from typing import Optional

import ibis.expr.datashape as ds
import ibis.expr.datatypes as dt
from ibis.common.annotations import attribute
from ibis.expr.operations import Relation
from ibis.expr.operations.core import Value


class PredictXGB(Value):
    arg: Relation
    model_name: Value[dt.String]
    where: Optional[Value[dt.Boolean]] = None
    dtype = dt.float64
    shape = ds.columnar

    @attribute
    def relations(self):
        return frozenset({self.arg})
