use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::{DataFrame, ParquetReadOptions};
use datafusion_common::ScalarValue;
use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::errors::DataFusionError;
use crate::utils::wait_for_future;

/// Configuration options for a SessionContext
#[pyclass(name = "SessionConfig", module = "datafusion", subclass)]
#[derive(Clone, Default)]
pub(crate) struct PySessionConfig {
    pub(crate) config: SessionConfig,
}

impl From<SessionConfig> for PySessionConfig {
    fn from(config: SessionConfig) -> Self {
        Self { config }
    }
}

#[pymethods]
impl PySessionConfig {
    #[pyo3(signature = (config_options=None))]
    #[new]
    fn new(config_options: Option<HashMap<String, String>>) -> Self {
        let mut config = SessionConfig::new();
        if let Some(hash_map) = config_options {
            for (k, v) in &hash_map {
                config = config.set(k, ScalarValue::Utf8(Some(v.clone())));
            }
        }

        Self { config }
    }
}

/// `PySessionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(name = "SessionContext", module = "datafusion", subclass)]
#[derive(Clone)]
pub(crate) struct PySessionContext {
    pub(crate) ctx: SessionContext,
}

#[pymethods]
impl PySessionContext {
    #[pyo3(signature = (config=None))]
    #[new]
    fn new(config: Option<PySessionConfig>) -> PyResult<Self> {
        let config = if let Some(c) = config {
            c.config
        } else {
            SessionConfig::default().with_information_schema(true)
        };
        let runtime_config = RuntimeConfig::default();
        let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
        let session_state = SessionState::new_with_config_rt(config, runtime);
        Ok(PySessionContext {
            ctx: SessionContext::new_with_state(session_state),
        })
    }

    /// Returns a PyDataFrame whose plan corresponds to the SQL statement.
    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(PyDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, path, table_partition_cols=vec![],
                        parquet_pruning=true,
                        file_extension=".parquet",
                        skip_metadata=true,
                        schema=None))]
    fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        table_partition_cols: Vec<(String, String)>,
        parquet_pruning: bool,
        file_extension: &str,
        skip_metadata: bool,
        schema: Option<PyArrowType<Schema>>,
        py: Python,
    ) -> PyResult<()> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?)
            .parquet_pruning(parquet_pruning)
            .skip_metadata(skip_metadata);
        options.file_extension = file_extension;
        options.schema = schema.as_ref().map(|x| &x.0);

        let result = self.ctx.register_parquet(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(())
    }

    fn tables(&self) -> HashSet<String> {
        #[allow(deprecated)]
        self.ctx.tables().unwrap()
    }

    fn table(&self, name: &str, py: Python) -> PyResult<PyDataFrame> {
        let x = wait_for_future(py, self.ctx.table(name)).map_err(DataFusionError::from)?;
        Ok(PyDataFrame::new(x))
    }

    fn table_exist(&self, name: &str) -> PyResult<bool> {
        Ok(self.ctx.table_exist(name)?)
    }

    fn empty_table(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::new(self.ctx.read_empty()?))
    }

    fn session_id(&self) -> String {
        self.ctx.session_id()
    }

    fn __repr__(&self) -> PyResult<String> {
        let config = self.ctx.copied_config();
        let mut config_entries = config
            .options()
            .entries()
            .iter()
            .filter(|e| e.value.is_some())
            .map(|e| format!("{} = {}", e.key, e.value.as_ref().unwrap()))
            .collect::<Vec<_>>();
        config_entries.sort();
        Ok(format!(
            "SessionContext: id={}; configs=[\n\t{}]",
            self.session_id(),
            config_entries.join("\n\t")
        ))
    }
}

impl PySessionContext {
    async fn _table(&self, name: &str) -> datafusion_common::Result<DataFrame> {
        self.ctx.table(name).await
    }
}

impl From<PySessionContext> for SessionContext {
    fn from(ctx: PySessionContext) -> SessionContext {
        ctx.ctx
    }
}

impl From<SessionContext> for PySessionContext {
    fn from(ctx: SessionContext) -> PySessionContext {
        PySessionContext { ctx }
    }
}

fn convert_table_partition_cols(
    table_partition_cols: Vec<(String, String)>,
) -> Result<Vec<(String, DataType)>, DataFusionError> {
    table_partition_cols
        .into_iter()
        .map(|(name, ty)| match ty.as_str() {
            "string" => Ok((name, DataType::Utf8)),
            _ => Err(DataFusionError::Common(format!(
                "Unsupported data type '{ty}' for partition column"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()
}
