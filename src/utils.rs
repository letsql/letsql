use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{ExecutionMode, PlanProperties};
use datafusion_common::DataFusionError::Execution;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::Volatility;
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};
use image::{DynamicImage, ImageFormat, ImageReader};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::errors::DataFusionError;
use crate::TokioRuntime;

/// Utility to get the Tokio Runtime from Python
pub(crate) fn get_tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    let datafusion = py.import("letsql._internal").unwrap();
    datafusion.getattr("runtime").unwrap().extract().unwrap()
}

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F>(py: Python, f: F) -> F::Output
where
    F: Send + Future,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime(py).0;
    py.allow_threads(|| runtime.block_on(f))
}
#[allow(clippy::redundant_async_block)]
pub fn wait_for_completion<F>(py: Python, fut: F) -> F::Output
where
    F: Send + Future,
    F::Output: Send,
{
    py.allow_threads(|| futures::executor::block_on(async move { fut.await }))
}

pub(crate) fn parse_volatility(value: &str) -> Result<Volatility, DataFusionError> {
    Ok(match value {
        "immutable" => Volatility::Immutable,
        "stable" => Volatility::Stable,
        "volatile" => Volatility::Volatile,
        value => {
            return Err(DataFusionError::Common(format!(
                "Unsupported volatility type: `{value}`, supported \
                 values are: immutable, stable and volatile."
            )))
        }
    })
}

pub fn compute_properties(schema: SchemaRef) -> PlanProperties {
    let eq_properties = EquivalenceProperties::new(schema);

    PlanProperties::new(
        eq_properties,
        Partitioning::UnknownPartitioning(1),
        ExecutionMode::Bounded,
    )
}

pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .map(|arg| arg.clone().into_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = (inner)(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

pub(crate) fn binary_to_img(data: &[u8]) -> Result<(ImageFormat, DynamicImage)> {
    let data = Cursor::new(data);

    let reader = ImageReader::new(data)
        .with_guessed_format()
        .map_err(|e| Execution(e.to_string()))?;

    let format = match reader.format() {
        Some(format) => format,
        None => return exec_err!("Format"),
    };

    let image = reader.decode().map_err(|e| Execution(e.to_string()))?;

    Ok((format, image))
}
