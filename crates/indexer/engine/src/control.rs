use starknet_crypto::Felt;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use torii_storage::proto::{Contract, ContractDefinition, ContractType};

const CONTROL_CHANNEL_CAPACITY: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractRegistrationOutcome {
    Registered,
    AlreadyRegistered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractRegistrationResult {
    pub contract: Contract,
    pub outcome: ContractRegistrationOutcome,
    pub target_head: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ContractManagementError {
    #[error(
        "contract {address:#x} is already registered as {registered_type}, not {requested_type}"
    )]
    TypeConflict {
        address: Felt,
        registered_type: ContractType,
        requested_type: ContractType,
    },
    #[error("contract {0:#x} is not registered")]
    NotFound(Felt),
    #[error("provider request failed: {0}")]
    Provider(String),
    #[error("storage operation failed: {0}")]
    Storage(String),
    #[error("indexing engine is unavailable")]
    EngineUnavailable,
}

#[derive(Debug)]
pub(crate) enum EngineControlCommand {
    RegisterContract {
        definition: ContractDefinition,
        response: oneshot::Sender<Result<ContractRegistrationResult, ContractManagementError>>,
    },
}

#[derive(Debug, Clone)]
pub struct EngineControlClient {
    sender: mpsc::Sender<EngineControlCommand>,
}

impl EngineControlClient {
    pub async fn register_contract(
        &self,
        definition: ContractDefinition,
    ) -> Result<ContractRegistrationResult, ContractManagementError> {
        let (response, result) = oneshot::channel();
        self.sender
            .send(EngineControlCommand::RegisterContract {
                definition,
                response,
            })
            .await
            .map_err(|_| ContractManagementError::EngineUnavailable)?;

        result
            .await
            .map_err(|_| ContractManagementError::EngineUnavailable)?
    }
}

#[derive(Debug)]
pub struct EngineControlReceiver {
    receiver: mpsc::Receiver<EngineControlCommand>,
}

impl EngineControlReceiver {
    pub(crate) fn try_recv(&mut self) -> Result<EngineControlCommand, TryRecvError> {
        self.receiver.try_recv()
    }
}

pub fn engine_control_channel() -> (EngineControlClient, EngineControlReceiver) {
    let (sender, receiver) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
    (
        EngineControlClient { sender },
        EngineControlReceiver { receiver },
    )
}
