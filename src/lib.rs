use pyo3::prelude::*;
#[allow(clippy::borrow_deref_ref)]
pub mod catalog;
pub mod common;
#[allow(clippy::borrow_deref_ref)]
mod context;
mod dataframe;
mod dataset;
mod dataset_exec;
mod errors;
pub mod expr;
mod functions;
mod ibis_filter_expression;
mod ibis_table;
mod ibis_table_exec;
pub mod model;
mod optimizer;
pub mod physical_plan;
pub mod predict_udf;
mod provider;
mod py_record_batch_provider;
mod pyarrow_filter_expression;
mod record_batch;
pub mod sql;
#[allow(clippy::borrow_deref_ref)]
mod udaf;
#[allow(clippy::borrow_deref_ref)]
mod udf;
mod udwf;
pub mod utils;

mod object_storage;

// Used to define Tokio Runtime as a Python module attribute
#[pyclass]
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

/// Low-level LetSQL internal package.
#[pymodule]
fn _internal(_py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    // Register the Tokio Runtime as a module attribute, so we can reuse it
    m.add(
        "runtime",
        TokioRuntime(tokio::runtime::Runtime::new().unwrap()),
    )?;

   Ok(())
}
