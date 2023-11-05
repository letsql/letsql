use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::utils::wait_for_future;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::{PyArrowType, ToPyArrow};

#[pyclass(name = "DataFrame", module = "datafusion", subclass)]
#[derive(Clone)]
pub(crate) struct PyDataFrame {
    df: Arc<DataFrame>,
}

impl PyDataFrame {
    /// creates a new PyDataFrame
    pub fn new(df: DataFrame) -> Self {
        Self { df: Arc::new(df) }
    }
}

#[pymethods]
impl PyDataFrame {
    // Executes this DataFrame to get the total number of rows.
    fn count(&self, py: Python) -> PyResult<usize> {
        Ok(wait_for_future(py, self.df.as_ref().clone().count())?)
    }

    /// Returns the schema from the logical plan
    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.df.schema().into())
    }

    /// Executes the plan, returning a list of `RecordBatch`es.
    /// Unless some order is specified in the plan, there is no
    /// guarantee of the order of the result.
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches = wait_for_future(py, self.df.as_ref().clone().collect())?;
        // cannot use PyResult<Vec<RecordBatch>> return type due to
        // https://github.com/PyO3/pyo3/issues/1813
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    fn to_arrow_table(&self, py: Python) -> PyResult<PyObject> {
        let batches = self.collect(py)?.to_object(py);
        let schema: PyObject = self.schema().into_py(py);

        Python::with_gil(|py| {
            // Instantiate pyarrow Table object and use its from_batches method
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();
            Ok(table)
        })
    }

    /// Convert to pandas dataframe with pyarrow
    /// Collect the batches, pass to Arrow Table & then convert to Pandas DataFrame
    fn to_pandas(&self, py: Python) -> PyResult<PyObject> {
        let table = self.to_arrow_table(py)?;

        Python::with_gil(|py| {
            // See also: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas
            let result = table.call_method0(py, "to_pandas")?;
            Ok(result)
        })
    }
}
