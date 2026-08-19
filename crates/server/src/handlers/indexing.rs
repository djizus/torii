use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use http::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper::body::HttpBody;
use hyper::{Body, Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use starknet::providers::Provider;
use starknet_crypto::Felt;
use tokio::time::timeout;
use torii_indexer::control::{
    ContractManagementError, ContractRegistrationOutcome, ContractRegistrationResult,
    EngineControlClient,
};
use torii_storage::proto::{Contract, ContractDefinition, ContractQuery, ContractType};
use torii_storage::ReadOnlyStorage;
use tracing::{error, info};

use super::Handler;

const ADMIN_CONTRACTS_PATH: &str = "/admin/indexing/contracts";
const MAX_REQUEST_BODY_BYTES: usize = 4 * 1024;
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(30);
const LOG_TARGET: &str = "torii::server::handlers::indexing";

#[derive(Clone)]
pub struct ContractManagementConfig {
    token: String,
    control: EngineControlClient,
}

impl ContractManagementConfig {
    pub fn new(token: String, control: EngineControlClient) -> Self {
        Self { token, control }
    }
}

impl fmt::Debug for ContractManagementConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContractManagementConfig")
            .field("token", &"[redacted]")
            .field("control", &self.control)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct RegisterContractRequest {
    address: String,
    contract_type: String,
    starting_block: Option<u64>,
}

impl TryFrom<RegisterContractRequest> for ContractDefinition {
    type Error = String;

    fn try_from(request: RegisterContractRequest) -> Result<Self, Self::Error> {
        let address = Felt::from_str(&request.address)
            .map_err(|error| format!("invalid contract address: {error}"))?;
        let contract_type = ContractType::from_str(&request.contract_type)
            .map_err(|error| format!("invalid contract type: {error}"))?;

        Ok(Self {
            address,
            r#type: contract_type,
            starting_block: request.starting_block,
        })
    }
}

#[derive(Debug, Serialize)]
struct ContractStatus {
    address: String,
    contract_type: String,
    head: Option<u64>,
    chain_head: u64,
    ready: bool,
}

impl ContractStatus {
    fn from_contract(contract: &Contract, chain_head: u64) -> Self {
        Self {
            address: format!("{:#x}", contract.contract_address),
            contract_type: contract.contract_type.to_string(),
            head: contract.head,
            chain_head,
            ready: contract.head.is_some_and(|head| head >= chain_head),
        }
    }
}

#[derive(Debug)]
pub struct ReadinessHandler<P, S> {
    provider: P,
    storage: Arc<S>,
    startup_contracts: Vec<Felt>,
}

impl<P, S> ReadinessHandler<P, S> {
    pub fn new(provider: P, storage: Arc<S>, startup_contracts: Vec<Felt>) -> Self {
        Self {
            provider,
            storage,
            startup_contracts,
        }
    }
}

#[async_trait::async_trait]
impl<P, S> Handler for ReadinessHandler<P, S>
where
    P: Provider + Sync + Send + fmt::Debug,
    S: ReadOnlyStorage + 'static,
{
    fn should_handle(&self, request: &Request<Body>) -> bool {
        request.uri().path() == "/ready"
    }

    async fn handle(&self, request: Request<Body>, _client_addr: IpAddr) -> Response<Body> {
        if request.method() != Method::GET {
            return error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "readiness only supports GET",
            );
        }

        match resolve_contract_statuses(
            &self.provider,
            self.storage.as_ref(),
            &self.startup_contracts,
        )
        .await
        {
            Ok((chain_head, statuses)) => {
                let ready = all_contracts_are_ready(self.startup_contracts.len(), &statuses);
                json_response(
                    if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    json!({
                        "success": ready,
                        "status": if ready { "ready" } else { "indexing" },
                        "chain_head": chain_head,
                        "contracts": statuses,
                    }),
                )
            }
            Err(error) => {
                error!(target: LOG_TARGET, error = %error, "Failed to resolve readiness.");
                error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "readiness_unavailable",
                    "unable to resolve indexer readiness",
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct ContractManagementHandler<P, S> {
    config: Option<ContractManagementConfig>,
    provider: P,
    storage: Arc<S>,
}

impl<P, S> ContractManagementHandler<P, S> {
    pub fn new(config: Option<ContractManagementConfig>, provider: P, storage: Arc<S>) -> Self {
        Self {
            config,
            provider,
            storage,
        }
    }

    async fn register_contract(
        &self,
        request: Request<Body>,
        config: &ContractManagementConfig,
    ) -> Response<Body> {
        let definition = match parse_contract_definition(request.into_body()).await {
            Ok(definition) => definition,
            Err(response) => return response,
        };

        match timeout(
            REGISTRATION_TIMEOUT,
            config.control.register_contract(definition),
        )
        .await
        {
            Ok(Ok(result)) => registration_response(result),
            Ok(Err(error)) => contract_management_error_response(error),
            Err(_) => error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "engine_timeout",
                "indexing engine did not accept the command in time",
            ),
        }
    }

    async fn contract_status(&self, address: &str) -> Response<Body>
    where
        P: Provider + Sync + Send + fmt::Debug,
        S: ReadOnlyStorage,
    {
        let address = match Felt::from_str(address) {
            Ok(address) => address,
            Err(error) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_address",
                    &format!("invalid contract address: {error}"),
                )
            }
        };

        match resolve_contract_statuses(&self.provider, self.storage.as_ref(), &[address]).await {
            Ok((_, mut statuses)) => match statuses.pop() {
                Some(status) => json_response(
                    StatusCode::OK,
                    json!({ "success": true, "contract": status }),
                ),
                None => error_response(
                    StatusCode::NOT_FOUND,
                    "contract_not_found",
                    "contract is not registered",
                ),
            },
            Err(error) => {
                error!(target: LOG_TARGET, error = %error, "Failed to resolve contract status.");
                error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "status_unavailable",
                    "unable to resolve contract indexing status",
                )
            }
        }
    }
}

#[async_trait::async_trait]
impl<P, S> Handler for ContractManagementHandler<P, S>
where
    P: Provider + Sync + Send + fmt::Debug,
    S: ReadOnlyStorage + 'static,
{
    fn should_handle(&self, request: &Request<Body>) -> bool {
        let path = request.uri().path();
        path == ADMIN_CONTRACTS_PATH
            || path
                .strip_prefix(ADMIN_CONTRACTS_PATH)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }

    async fn handle(&self, request: Request<Body>, _client_addr: IpAddr) -> Response<Body> {
        let Some(config) = &self.config else {
            return disabled_response();
        };
        if !is_authorized(&request, &config.token) {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "valid bearer authentication is required",
            );
        }

        let method = request.method().clone();
        let path = request.uri().path().to_string();
        match (method, path.strip_prefix(ADMIN_CONTRACTS_PATH)) {
            (Method::POST, Some("")) => self.register_contract(request, config).await,
            (Method::GET, Some(address)) if address.starts_with('/') => {
                self.contract_status(&address[1..]).await
            }
            _ => error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "use POST on the collection or GET on a contract address",
            ),
        }
    }
}

fn all_contracts_are_ready(expected_count: usize, statuses: &[ContractStatus]) -> bool {
    statuses.len() == expected_count && statuses.iter().all(|status| status.ready)
}

async fn resolve_contract_statuses<P, S>(
    provider: &P,
    storage: &S,
    addresses: &[Felt],
) -> Result<(u64, Vec<ContractStatus>), ContractManagementError>
where
    P: Provider + Sync + Send,
    S: ReadOnlyStorage,
{
    let chain_head = provider
        .block_hash_and_number()
        .await
        .map_err(|error| ContractManagementError::Provider(error.to_string()))?
        .block_number;
    let contracts = storage
        .contracts(&ContractQuery {
            contract_addresses: addresses.to_vec(),
            contract_types: vec![],
        })
        .await
        .map_err(|error| ContractManagementError::Storage(error.to_string()))?;
    let contracts_by_address: HashMap<_, _> = contracts
        .into_iter()
        .map(|contract| (contract.contract_address, contract))
        .collect();
    let statuses = addresses
        .iter()
        .filter_map(|address| contracts_by_address.get(address))
        .map(|contract| ContractStatus::from_contract(contract, chain_head))
        .collect();

    Ok((chain_head, statuses))
}

async fn parse_contract_definition(body: Body) -> Result<ContractDefinition, Response<Body>> {
    let bytes = read_limited_body(body).await?;
    let request = serde_json::from_slice::<RegisterContractRequest>(&bytes).map_err(|error| {
        error_response(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            &format!("invalid registration request: {error}"),
        )
    })?;
    ContractDefinition::try_from(request)
        .map_err(|error| error_response(StatusCode::BAD_REQUEST, "invalid_contract", &error))
}

async fn read_limited_body(mut body: Body) -> Result<Vec<u8>, Response<Body>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|_| {
            error_response(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                "failed to read request body",
            )
        })?;
        if bytes.len() + chunk.len() > MAX_REQUEST_BODY_BYTES {
            return Err(error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds 4 KiB",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn is_authorized(request: &Request<Body>, expected_token: &str) -> bool {
    let provided_token = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let Some(provided_token) = provided_token else {
        return false;
    };

    let expected = Sha256::digest(expected_token.as_bytes());
    let provided = Sha256::digest(provided_token.as_bytes());
    expected
        .iter()
        .zip(provided.iter())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn registration_response(result: ContractRegistrationResult) -> Response<Body> {
    let outcome = match result.outcome {
        ContractRegistrationOutcome::Registered => "registered",
        ContractRegistrationOutcome::AlreadyRegistered => "already_registered",
    };
    info!(
        target: LOG_TARGET,
        contract_address = %format!("{:#x}", result.contract.contract_address),
        outcome,
        "Handled dynamic contract registration."
    );

    json_response(
        StatusCode::OK,
        json!({
            "success": true,
            "outcome": outcome,
            "target_head": result.target_head,
            "contract": {
                "address": format!("{:#x}", result.contract.contract_address),
                "contract_type": result.contract.contract_type.to_string(),
                "head": result.contract.head,
            },
        }),
    )
}

fn contract_management_error_response(error: ContractManagementError) -> Response<Body> {
    let (status, code) = match error {
        ContractManagementError::TypeConflict { .. } => {
            (StatusCode::CONFLICT, "contract_type_conflict")
        }
        ContractManagementError::NotFound(_) => (StatusCode::NOT_FOUND, "contract_not_found"),
        ContractManagementError::EngineUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "engine_unavailable")
        }
        ContractManagementError::Provider(_) => (StatusCode::BAD_GATEWAY, "provider_error"),
        ContractManagementError::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    };
    error!(target: LOG_TARGET, error = %error, "Dynamic contract registration failed.");
    error_response(status, code, &error.to_string())
}

fn disabled_response() -> Response<Body> {
    error_response(
        StatusCode::NOT_FOUND,
        "not_found",
        "contract management is not enabled",
    )
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response<Body> {
    json_response(
        status,
        json!({
            "success": false,
            "error": { "code": code, "message": message },
        }),
    )
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("JSON response should be valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_world_registration() {
        let definition = ContractDefinition::try_from(RegisterContractRequest {
            address: "0x123".to_string(),
            contract_type: "WORLD".to_string(),
            starting_block: Some(42),
        })
        .unwrap();

        assert_eq!(definition.address, Felt::from_hex("0x123").unwrap());
        assert_eq!(definition.r#type, ContractType::WORLD);
        assert_eq!(definition.starting_block, Some(42));
    }

    #[test]
    fn bearer_token_comparison_accepts_only_the_configured_token() {
        let authorized = Request::builder()
            .header(AUTHORIZATION, "Bearer correct-token")
            .body(Body::empty())
            .unwrap();
        let unauthorized = Request::builder()
            .header(AUTHORIZATION, "Bearer wrong-token")
            .body(Body::empty())
            .unwrap();

        assert!(is_authorized(&authorized, "correct-token"));
        assert!(!is_authorized(&unauthorized, "correct-token"));
    }

    #[test]
    fn startup_readiness_requires_every_startup_contract() {
        let ready = ContractStatus {
            address: "0x1".to_string(),
            contract_type: "WORLD".to_string(),
            head: Some(10),
            chain_head: 10,
            ready: true,
        };

        assert!(all_contracts_are_ready(1, &[ready]));
        assert!(!all_contracts_are_ready(1, &[]));
    }
}
