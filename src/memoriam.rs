use std::sync::Arc;

use domain_engine_arrow::{arrow_http, ArrowConfig, ArrowReqMessage, ArrowRespMessage};
use domain_engine_core::{DomainError, DomainResult, Session};
use domain_engine_datafusion::{DomainEngineAPI, Ontology, OntologyCatalogProvider};
use futures::{stream::BoxStream, StreamExt, TryFutureExt, TryStreamExt};
use pyo3::PyResult;
use reqwest::{
    header::{HeaderMap, AUTHORIZATION},
    Client,
};
use serde_json::json;
use url::Url;

use crate::{context::PySessionContext, errors::py_runtime_err};

impl PySessionContext {
    pub async fn register_memoriam_ontology_internal(
        &self,
        url: &str,
        username: Option<&str>,
        password: Option<&str>,
        service_name: Option<&str>,
        service_secret: Option<&str>,
    ) -> PyResult<()> {
        let url = Url::parse(url).map_err(py_runtime_err)?;
        let http_client = Client::builder()
            .danger_accept_invalid_certs(true)
            .http2_prior_knowledge()
            .build()
            .unwrap();

        let auth_url = url
            .join("/authly/api/auth/authenticate")
            .map_err(py_runtime_err)?;
        let ontology_url = url.join("/o/arrow/ontology").map_err(py_runtime_err)?;
        let transact_url = url.join("/o/arrow/transact").map_err(py_runtime_err)?;

        let token = maybe_authenticate_with_authly(
            &http_client,
            auth_url,
            username,
            password,
            service_name,
            service_secret,
        )
        .await?;

        let mut headers = HeaderMap::new();
        if let Some(token) = token {
            headers.insert(AUTHORIZATION, format!("Bearer {token}").try_into().unwrap());
        }

        let ontology_postcard = http_client
            .get(ontology_url)
            .headers(headers.clone())
            .send()
            .await
            .map_err(py_runtime_err)?
            .error_for_status()
            .map_err(py_runtime_err)?
            .bytes()
            .await
            .map_err(py_runtime_err)?;
        let ontology = Arc::new(
            Ontology::try_from_postcard(&ontology_postcard)
                .map_err(|err| py_runtime_err(format!("{err:?}")))?,
        );

        let domain_engine_api: Arc<dyn DomainEngineAPI + Send + Sync> =
            Arc::new(DomainEngineClient {
                http_client,
                ontology,
                transact_url,
                headers,
            });

        self.ctx.register_catalog(
            "ontology",
            Arc::new(OntologyCatalogProvider::from(domain_engine_api)),
        );

        Ok(())
    }
}

struct DomainEngineClient {
    ontology: Arc<Ontology>,
    http_client: Client,
    transact_url: Url,
    headers: HeaderMap,
}

impl domain_engine_datafusion::DomainEngineAPI for DomainEngineClient {
    fn ontology_defs(&self) -> &ontol_runtime::ontology::aspects::DefsAspect {
        self.ontology.as_ref().as_ref()
    }
}

impl domain_engine_datafusion::ArrowTransactAPI for DomainEngineClient {
    fn arrow_transact(
        &self,
        req: ArrowReqMessage,
        config: ArrowConfig,
        _session: Session,
    ) -> BoxStream<'static, DomainResult<ArrowRespMessage>> {
        let http_client = self.http_client.clone();
        let mut url = self.transact_url.clone();
        let headers = self.headers.clone();

        url.set_query(Some(&format!("batch_size={}", config.batch_size)));

        async_stream::try_stream! {
            let response = http_client
                .post(url)
                .headers(headers)
                .body(postcard::to_allocvec(&req).unwrap())
                .send()
                .map_err(|err| DomainError::protocol(format!("request error: {err}")))
                .await?;

            let http_stream = response
                .bytes_stream()
                .map_err(|err| DomainError::protocol(format!("stream error: {err}")));

            for await msg_result in arrow_http::http_stream_to_resp_msg_stream(http_stream) {
                yield msg_result?;
            }
        }
        .boxed()
    }
}

async fn maybe_authenticate_with_authly(
    http_client: &Client,
    url: Url,
    username: Option<&str>,
    password: Option<&str>,
    service_name: Option<&str>,
    service_secret: Option<&str>,
) -> PyResult<Option<String>> {
    let body = if let Some(username) = username {
        json!({
            "username": username,
            "password": password.unwrap_or(""),
        })
    } else if let Some(service_name) = service_name {
        json!({
            "serviceName": service_name,
            "serviceSecret": service_secret.unwrap_or(""),
        })
    } else {
        return Ok(None);
    };

    let token_response: serde_json::Value = http_client
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(py_runtime_err)?
        .json()
        .await
        .map_err(py_runtime_err)?;

    let token = token_response
        .as_object()
        .ok_or_else(|| py_runtime_err("invalid Authly token response"))?
        .get("token")
        .ok_or_else(|| py_runtime_err("no Authly token"))?
        .as_str()
        .ok_or_else(|| py_runtime_err("Authly token must be a string"))?
        .to_string();

    Ok(Some(token))
}
