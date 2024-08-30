// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use datafusion::common::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, TableOptions, Visit,
};
use datafusion::common::{config_err, exec_datafusion_err, exec_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;

use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::SessionContext;
use datafusion_common::config::ConfigFileType;
use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use object_store::{CredentialProvider, ObjectStore};
use url::Url;

pub async fn get_s3_object_store_builder(
    url: &Url,
    aws_options: &AwsOptions,
) -> Result<AmazonS3Builder> {
    let AwsOptions {
        access_key_id,
        secret_access_key,
        session_token,
        region,
        endpoint,
        allow_http,
    } = aws_options;

    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);

    if let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) {
        builder = builder
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);

        if let Some(session_token) = session_token {
            builder = builder.with_token(session_token);
        }
    } else {
        let config = aws_config::from_env().load().await;
        if let Some(region) = config.region() {
            builder = builder.with_region(region.to_string());
        }

        let credentials = config
            .credentials_provider()
            .ok_or_else(|| {
                DataFusionError::ObjectStore(object_store::Error::Generic {
                    store: "S3",
                    source: "Failed to get S3 credentials from the environment".into(),
                })
            })?
            .clone();

        let credentials = Arc::new(S3CredentialProvider { credentials });
        builder = builder.with_credentials(credentials);
    }

    if let Some(region) = region {
        builder = builder.with_region(region);
    }

    if let Some(endpoint) = endpoint {
        // Make a nicer error if the user hasn't allowed http and the endpoint
        // is http as the default message is "URL scheme is not allowed"
        if let Ok(endpoint_url) = Url::try_from(endpoint.as_str()) {
            if !matches!(allow_http, Some(true)) && endpoint_url.scheme() == "http" {
                return config_err!(
                    "Invalid endpoint: {endpoint}. \
                HTTP is not allowed for S3 endpoints. \
                To allow HTTP, set 'aws.allow_http' to true"
                );
            }
        }

        builder = builder.with_endpoint(endpoint);
    }

    if let Some(allow_http) = allow_http {
        builder = builder.with_allow_http(*allow_http);
    }

    Ok(builder)
}

#[derive(Debug)]
struct S3CredentialProvider {
    credentials: aws_credential_types::provider::SharedCredentialsProvider,
}

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self.credentials.provide_credentials().await.map_err(|e| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(ToString::to_string),
        }))
    }
}

pub fn get_oss_object_store_builder(
    url: &Url,
    aws_options: &AwsOptions,
) -> Result<AmazonS3Builder> {
    get_object_store_builder(url, aws_options, true)
}

pub fn get_cos_object_store_builder(
    url: &Url,
    aws_options: &AwsOptions,
) -> Result<AmazonS3Builder> {
    get_object_store_builder(url, aws_options, false)
}

fn get_object_store_builder(
    url: &Url,
    aws_options: &AwsOptions,
    virtual_hosted_style_request: bool,
) -> Result<AmazonS3Builder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env()
        .with_virtual_hosted_style_request(virtual_hosted_style_request)
        .with_bucket_name(bucket_name)
        // oss/cos don't care about the "region" field
        .with_region("do_not_care");

    if let (Some(access_key_id), Some(secret_access_key)) =
        (&aws_options.access_key_id, &aws_options.secret_access_key)
    {
        builder = builder
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);
    }

    if let Some(endpoint) = &aws_options.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    Ok(builder)
}

pub fn get_gcs_object_store_builder(
    url: &Url,
    gs_options: &GcpOptions,
) -> Result<GoogleCloudStorageBuilder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket_name);

    if let Some(service_account_path) = &gs_options.service_account_path {
        builder = builder.with_service_account_path(service_account_path);
    }

    if let Some(service_account_key) = &gs_options.service_account_key {
        builder = builder.with_service_account_key(service_account_key);
    }

    if let Some(application_credentials_path) = &gs_options.application_credentials_path {
        builder = builder.with_application_credentials(application_credentials_path);
    }

    Ok(builder)
}

fn get_bucket_name(url: &Url) -> Result<&str> {
    url.host_str().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Not able to parse bucket name from url: {}",
            url.as_str()
        ))
    })
}

/// This struct encapsulates AWS options one uses when setting up object storage.
#[derive(Default, Debug, Clone)]
pub struct AwsOptions {
    /// Access Key ID
    pub access_key_id: Option<String>,
    /// Secret Access Key
    pub secret_access_key: Option<String>,
    /// Session token
    pub session_token: Option<String>,
    /// AWS Region
    pub region: Option<String>,
    /// OSS or COS Endpoint
    pub endpoint: Option<String>,
    /// Allow HTTP (otherwise will always use https)
    pub allow_http: Option<bool>,
}

impl ExtensionOptions for AwsOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let (_key, aws_key) = key.split_once('.').unwrap_or((key, ""));
        let (key, rem) = aws_key.split_once('.').unwrap_or((aws_key, ""));
        match key {
            "access_key_id" => {
                self.access_key_id.set(rem, value)?;
            }
            "secret_access_key" => {
                self.secret_access_key.set(rem, value)?;
            }
            "session_token" => {
                self.session_token.set(rem, value)?;
            }
            "region" => {
                self.region.set(rem, value)?;
            }
            "oss" | "cos" | "endpoint" => {
                self.endpoint.set(rem, value)?;
            }
            "allow_http" => {
                self.allow_http.set(rem, value)?;
            }
            _ => {
                return config_err!("Config value \"{}\" not found on AwsOptions", rem);
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.access_key_id.visit(&mut v, "access_key_id", "");
        self.secret_access_key
            .visit(&mut v, "secret_access_key", "");
        self.session_token.visit(&mut v, "session_token", "");
        self.region.visit(&mut v, "region", "");
        self.endpoint.visit(&mut v, "endpoint", "");
        self.allow_http.visit(&mut v, "allow_http", "");
        v.0
    }
}

impl ConfigExtension for AwsOptions {
    const PREFIX: &'static str = "aws";
}

/// This struct encapsulates GCP options one uses when setting up object storage.
#[derive(Debug, Clone, Default)]
pub struct GcpOptions {
    /// Service account path
    pub service_account_path: Option<String>,
    /// Service account key
    pub service_account_key: Option<String>,
    /// Application credentials path
    pub application_credentials_path: Option<String>,
}

impl ExtensionOptions for GcpOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let (_key, rem) = key.split_once('.').unwrap_or((key, ""));
        match rem {
            "service_account_path" => {
                self.service_account_path.set(rem, value)?;
            }
            "service_account_key" => {
                self.service_account_key.set(rem, value)?;
            }
            "application_credentials_path" => {
                self.application_credentials_path.set(rem, value)?;
            }
            _ => {
                return config_err!("Config value \"{}\" not found on GcpOptions", rem);
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.service_account_path
            .visit(&mut v, "service_account_path", "");
        self.service_account_key
            .visit(&mut v, "service_account_key", "");
        self.application_credentials_path
            .visit(&mut v, "application_credentials_path", "");
        v.0
    }
}

impl ConfigExtension for GcpOptions {
    const PREFIX: &'static str = "gcp";
}

pub(crate) async fn get_object_store(
    state: &SessionState,
    scheme: &str,
    url: &Url,
    table_options: &TableOptions,
) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
    let store: Arc<dyn ObjectStore> = match scheme {
        "s3" => {
            let Some(options) = table_options.extensions.get::<AwsOptions>() else {
                return exec_err!("Given table options incompatible with the 's3' scheme");
            };
            let builder = get_s3_object_store_builder(url, options).await?;
            Arc::new(builder.build()?)
        }
        "oss" => {
            let Some(options) = table_options.extensions.get::<AwsOptions>() else {
                return exec_err!("Given table options incompatible with the 'oss' scheme");
            };
            let builder = get_oss_object_store_builder(url, options)?;
            Arc::new(builder.build()?)
        }
        "cos" => {
            let Some(options) = table_options.extensions.get::<AwsOptions>() else {
                return exec_err!("Given table options incompatible with the 'cos' scheme");
            };
            let builder = get_cos_object_store_builder(url, options)?;
            Arc::new(builder.build()?)
        }
        "gs" | "gcs" => {
            let Some(options) = table_options.extensions.get::<GcpOptions>() else {
                return exec_err!("Given table options incompatible with the 'gs'/'gcs' scheme");
            };
            let builder = get_gcs_object_store_builder(url, options)?;
            Arc::new(builder.build()?)
        }
        "http" | "https" => Arc::new(
            HttpBuilder::new()
                .with_url(url.origin().ascii_serialization())
                .build()?,
        ),
        _ => {
            // For other types, try to get from `object_store_registry`:
            state
                .runtime_env()
                .object_store_registry
                .get_store(url)
                .map_err(|_| exec_datafusion_err!("Unsupported object store scheme: {}", scheme))?
        }
    };
    Ok(store)
}

pub(crate) async fn register_object_store_and_config_extensions(
    ctx: &SessionContext,
    location: &String,
    format: Option<ConfigFileType>,
    options: &HashMap<String, String>,
) -> Result<()> {
    // Parse the location URL to extract the scheme and other components
    let table_path = ListingTableUrl::parse(location)?;

    // Extract the scheme (e.g., "s3", "gcs") from the parsed URL
    let scheme = table_path.scheme();

    // Obtain a reference to the URL
    let url = table_path.as_ref();

    // Register the options based on the scheme extracted from the location
    match scheme {
        // For Amazon S3 or Alibaba Cloud OSS
        "s3" | "oss" | "cos" => {
            // Register AWS specific table options in the session context:
            ctx.register_table_options_extension(AwsOptions::default())
        }
        // For Google Cloud Storage
        "gs" | "gcs" => {
            // Register GCP specific table options in the session context:
            ctx.register_table_options_extension(GcpOptions::default())
        }
        // For unsupported schemes, do nothing:
        _ => {}
    }

    // Clone and modify the default table options based on the provided options
    let mut table_options = ctx.state().default_table_options().clone();
    if let Some(format) = format {
        table_options.set_config_format(format);
    }

    if let "s3" | "oss" | "cos" | "gs" | "gcs" = scheme {
        table_options.alter_with_string_hash_map(options)?;
    }

    // Retrieve the appropriate object store based on the scheme, URL, and modified table options
    let store = get_object_store(&ctx.state(), scheme, url, &table_options).await?;

    // Register the retrieved object store in the session context's runtime environment
    ctx.register_object_store(url, store);

    Ok(())
}
