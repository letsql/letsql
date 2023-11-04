use pyo3::prelude::*;

#[allow(clippy::borrow_deref_ref)]
pub mod catalog;

#[allow(clippy::borrow_deref_ref)]
mod context;

mod dataframe;
mod errors;
pub mod utils;

// Used to define Tokio Runtime as a Python module attribute
#[pyclass]
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

/// Low-level LetSQL internal package.
#[pymodule]
fn _internal(_py: Python, m: &PyModule) -> PyResult<()> {
    // Register the Tokio Runtime as a module attribute so we can reuse it
    m.add(
        "runtime",
        TokioRuntime(tokio::runtime::Runtime::new().unwrap()),
    )?;

    m.add_class::<context::PySessionConfig>()?;
    m.add_class::<context::PySessionContext>()?;
    Ok(())
}
