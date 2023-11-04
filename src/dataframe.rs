use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use pyo3::prelude::*;

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
}
