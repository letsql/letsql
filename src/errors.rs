use core::fmt;
use std::error::Error;
use std::fmt::Debug;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError as InnerDataFusionError;
use pyo3::{exceptions::PyException, PyErr};

use prost::EncodeError;

#[derive(Debug)]
pub enum DataFusionError {
    ExecutionError(InnerDataFusionError),
    ArrowError(ArrowError),
    Common(String),
    PythonError(PyErr),
    EncodeError(EncodeError),
}

impl fmt::Display for DataFusionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataFusionError::ExecutionError(e) => write!(f, "DataFusion error: {e:?}"),
            DataFusionError::ArrowError(e) => write!(f, "Arrow error: {e:?}"),
            DataFusionError::PythonError(e) => write!(f, "Python error {e:?}"),
            DataFusionError::Common(e) => write!(f, "{e}"),
            DataFusionError::EncodeError(e) => write!(f, "Failed to encode substrait plan: {e}"),
        }
    }
}

impl From<ArrowError> for DataFusionError {
    fn from(err: ArrowError) -> DataFusionError {
        DataFusionError::ArrowError(err)
    }
}

impl From<InnerDataFusionError> for DataFusionError {
    fn from(err: InnerDataFusionError) -> DataFusionError {
        DataFusionError::ExecutionError(err)
    }
}

impl From<PyErr> for DataFusionError {
    fn from(err: PyErr) -> DataFusionError {
        DataFusionError::PythonError(err)
    }
}

impl From<DataFusionError> for PyErr {
    fn from(err: DataFusionError) -> PyErr {
        match err {
            DataFusionError::PythonError(py_err) => py_err,
            _ => PyException::new_err(err.to_string()),
        }
    }
}

impl Error for DataFusionError {}
