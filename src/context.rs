use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{CsvReadOptions, DataFrame, ParquetReadOptions};
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;
use gbdt::gradient_boost::GBDT;
use parking_lot::RwLock;
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;

use crate::catalog::{PyCatalog, PyTable};
use crate::dataframe::PyDataFrame;
use crate::dataset::Dataset;
use crate::errors::DataFusionError;
use crate::functions::greatest::GreatestFunc;
use crate::functions::least::LeastFunc;
use crate::ibis_table::IbisTable;
use crate::model::{ModelRegistry, SessionModelRegistry};
use crate::optimizer::{PredictXGBoostAnalyzerRule, PyOptimizerRule};
use crate::predict_udf::PredictUdf;
use crate::provider::PyTableProvider;
use crate::py_record_batch_provider::PyRecordBatchProvider;
use crate::tensor_functions::mean_all::TensorMeanAllUDF;
use crate::tensor_functions::rotate::Rotate90UDF;
use crate::tensor_functions::segment_anything::SegmentAnythingUDF;
use crate::udaf::PyAggregateUDF;
use crate::udf::PyScalarUDF;
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

    fn with_information_schema(&self, enabled: bool) -> Self {
        Self::from(self.config.clone().with_information_schema(enabled))
    }
}

#[pyclass(name = "SessionState", module = "datafusion", subclass)]
#[derive(Clone)]
pub(crate) struct PySessionState {
    pub(crate) session_state: SessionState,
}

impl From<SessionState> for PySessionState {
    fn from(session_state: SessionState) -> Self {
        Self { session_state }
    }
}

#[pymethods]
impl PySessionState {
    #[pyo3(signature = (config=None))]
    #[new]
    fn new(config: Option<PySessionConfig>) -> Self {
        let config = if let Some(c) = config {
            c.config
        } else {
            SessionConfig::default().with_information_schema(true)
        };
        let runtime_config = RuntimeConfig::default();
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

        let session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();

        Self { session_state }
    }

    fn add_optimizer_rule(&mut self, rule: PyOptimizerRule) -> Self {
        Self::from(
            SessionStateBuilder::new_from_existing(self.session_state.clone())
                .with_optimizer_rule(Arc::new(rule))
                .build(),
        )
    }
}

/// `PySessionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multithreaded execution engine to perform the execution.
#[pyclass(name = "SessionContext", module = "datafusion", subclass)]
#[derive(Clone)]
pub(crate) struct PySessionContext {
    pub(crate) ctx: SessionContext,
    /// Shared session state for the session
    model_registry: Arc<RwLock<SessionModelRegistry>>,
}

impl ModelRegistry for PySessionContext {
    fn register_model(&self, name: &str, path: &str, objective: &str) {
        let mut registry = self.model_registry.write();
        let model = GBDT::from_xgboost_dump(path, objective).expect("failed to load model");
        registry.models.insert(name.to_string(), Arc::new(model));
    }

    fn register_json_model(&self, name: &str, path: &str) {
        let mut registry = self.model_registry.write();
        let model = GBDT::from_xgboost_json_used_feature(path).expect("failed to load model");
        registry.models.insert(name.to_string(), Arc::new(model));
    }
}

#[pymethods]
impl PySessionContext {
    #[pyo3(signature = (session_state=None, config=None))]
    #[new]
    fn new(
        session_state: Option<PySessionState>,
        config: Option<PySessionConfig>,
    ) -> PyResult<Self> {
        let model_registry = SessionModelRegistry::new();
        let registry = Arc::new(RwLock::new(model_registry));
        let predict_xgb = ScalarUDF::from(PredictUdf::new_with_model_registry(registry.clone()));
        let rule = PredictXGBoostAnalyzerRule::new(registry.clone());

        let runtime_config = RuntimeConfig::default();
        let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
        let mut session_state = match (session_state, config) {
            (Some(s), _) => s.session_state,
            (None, Some(c)) => SessionStateBuilder::new()
                .with_config(c.config)
                .with_runtime_env(runtime)
                .with_default_features()
                .build(),
            (None, _) => {
                let session_config = SessionConfig::default().with_information_schema(true);
                SessionStateBuilder::new()
                    .with_config(session_config)
                    .with_runtime_env(runtime)
                    .with_default_features()
                    .build()
            }
        };

        let session_state = session_state.add_analyzer_rule(Arc::new(rule));
        let ctx = SessionContext::new_with_state(session_state.clone());
        // register the UDF with the context, so it can be invoked by name and from SQL
        ctx.register_udf(predict_xgb.clone());
        ctx.register_udf(ScalarUDF::from(GreatestFunc::new()));
        ctx.register_udf(ScalarUDF::from(LeastFunc::new()));
        ctx.register_udf(ScalarUDF::from(TensorMeanAllUDF::new()));
        ctx.register_udf(ScalarUDF::from(SegmentAnythingUDF::new()));
        ctx.register_udf(ScalarUDF::from(Rotate90UDF::new()));

        Ok(PySessionContext {
            ctx,
            model_registry: registry,
        })
    }

    /// Returns a PyDataFrame whose plan corresponds to the SQL statement.
    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).unwrap();
        Ok(PyDataFrame::new(df))
    }

    fn deregister_table(&mut self, name: &str) -> PyResult<()> {
        self.ctx
            .deregister_table(name)
            .map_err(DataFusionError::from)?;
        Ok(())
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

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name,
                        path,
                        schema=None,
                        has_header=true,
                        delimiter=",",
                        schema_infer_max_records=1000,
                        file_extension=".csv",
                        file_compression_type=None))]
    fn register_csv(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        file_compression_type: Option<String>,
        py: Python,
    ) -> PyResult<()> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;
        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(PyValueError::new_err(
                "Delimiter must be a single character",
            ));
        }

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension)
            .file_compression_type(parse_file_compression_type(file_compression_type)?);
        options.schema = schema.as_ref().map(|x| &x.0);

        let result = self.ctx.register_csv(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;

        Ok(())
    }

    fn register_record_batches(
        &mut self,
        name: &str,
        partitions: PyArrowType<Vec<Vec<RecordBatch>>>,
    ) -> PyResult<()> {
        let schema = partitions.0[0][0].schema();
        let table = MemTable::try_new(schema, partitions.0)?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    pub fn register_record_batch_reader(
        &mut self,
        name: &str,
        reader: PyArrowType<ArrowArrayStreamReader>,
    ) -> PyResult<()> {
        let reader = reader.0;
        let table = PyRecordBatchProvider::new(reader);
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(DataFusionError::from)?;

        Ok(())
    }

    pub fn register_ibis_table(&mut self, name: &str, reader: &PyAny, py: Python) -> PyResult<()> {
        let table: Arc<dyn TableProvider> = Arc::new(IbisTable::new(reader, py)?);

        self.ctx
            .register_table(name, table)
            .map_err(DataFusionError::from)?;

        Ok(())
    }

    #[pyo3(name = "register_table_provider")]
    pub fn register_py_table_provider(
        &mut self,
        name: &str,
        provider: PyTableProvider,
    ) -> PyResult<()> {
        let table: Arc<dyn TableProvider> = Arc::new(provider);

        self.ctx
            .register_table(name, table)
            .map_err(DataFusionError::from)?;

        Ok(())
    }

    pub fn register_dataframe(&mut self, name: &str, dataframe: PyDataFrame) -> PyResult<()> {
        let table: Arc<dyn TableProvider> = dataframe.df.as_ref().clone().into_view();

        self.ctx
            .register_table(name, table)
            .map_err(DataFusionError::from)?;

        Ok(())
    }

    pub fn register_table(&mut self, name: &str, table: &PyTable) -> PyResult<()> {
        self.ctx
            .register_table(name, table.table())
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    fn register_dataset(&self, name: &str, dataset: &PyAny, py: Python) -> PyResult<()> {
        let table: Arc<dyn TableProvider> = Arc::new(Dataset::new(dataset, py)?);

        self.ctx
            .register_table(name, table)
            .map_err(DataFusionError::from)?;

        Ok(())
    }

    fn tables(&self) -> HashSet<String> {
        self.ctx
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table_names()
            .iter()
            .cloned()
            .collect()
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

    fn register_udf(&mut self, udf: PyScalarUDF) -> PyResult<()> {
        self.ctx.register_udf(udf.function);
        Ok(())
    }

    fn register_udaf(&mut self, udaf: PyAggregateUDF) -> PyResult<()> {
        self.ctx.register_udaf(udaf.function);
        Ok(())
    }

    #[pyo3(signature = (name, path, objective))]
    fn register_xgb_model(&mut self, name: &str, path: &str, objective: &str) -> PyResult<()> {
        self.register_model(name, path, objective);
        Ok(())
    }

    #[pyo3(signature = (name, path))]
    fn register_xgb_json_model(&mut self, name: &str, path: &str) -> PyResult<()> {
        self.register_json_model(name, path);
        Ok(())
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

    #[pyo3(signature = (name="datafusion"))]
    fn catalog(&self, name: &str) -> PyResult<PyCatalog> {
        match self.ctx.catalog(name) {
            Some(catalog) => Ok(PyCatalog::new(catalog)),
            None => Err(PyKeyError::new_err(format!(
                "Catalog with name {} doesn't exist.",
                &name,
            ))),
        }
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

fn parse_file_compression_type(
    file_compression_type: Option<String>,
) -> Result<FileCompressionType, PyErr> {
    FileCompressionType::from_str(&*file_compression_type.unwrap_or("".to_string()).as_str())
        .map_err(|_| {
            PyValueError::new_err("file_compression_type must one of: gzip, bz2, xz, zstd")
        })
}
