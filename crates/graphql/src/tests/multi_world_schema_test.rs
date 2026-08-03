#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use dojo_types::naming::get_tag;
    use dojo_types::primitive::Primitive;
    use dojo_types::schema::{Member, Struct, Ty};
    use dojo_world::contracts::abigen::model::Layout;
    use dojo_world::contracts::naming::compute_selector_from_tag;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use starknet::core::types::Felt;
    use starknet::providers::jsonrpc::HttpTransport;
    use starknet::providers::JsonRpcClient;
    use tempfile::NamedTempFile;
    use tokio::sync::broadcast;
    use torii_messaging::Messaging;
    use torii_sqlite::executor::Executor;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use torii_storage::Storage;

    use crate::schema::build_schema;

    /// When torii indexes multiple worlds, the same (namespace, name) model is
    /// registered once per world. GraphQL field names carry no world scope, so
    /// building the schema used to panic on the duplicate field
    /// ("Field `...` already exists") and take the process down.
    #[tokio::test(flavor = "multi_thread")]
    async fn multi_world_duplicate_models_schema_builds() {
        let tempfile = NamedTempFile::new().unwrap();
        let path = tempfile.path().to_string_lossy();
        let options = SqliteConnectOptions::from_str(&path)
            .unwrap()
            .create_if_missing(true)
            .with_regexp();
        let pool = SqlitePoolOptions::new().connect_with(options).await.unwrap();
        sqlx::migrate!("../migrations").run(&pool).await.unwrap();

        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
            starknet::providers::Url::parse("http://localhost:1").unwrap(),
        )));

        let (shutdown_tx, _) = broadcast::channel(1);
        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let world_a = Felt::ONE;
        let world_b = Felt::TWO;
        let contracts = &[
            ContractDefinition {
                address: world_a,
                r#type: ContractType::WORLD,
                starting_block: None,
            },
            ContractDefinition {
                address: world_b,
                r#type: ContractType::WORLD,
                starting_block: None,
            },
        ];
        let db = Sql::new(pool.clone(), sender, contracts).await.unwrap();

        // the same model, once per world — e.g. two instances of the same game
        let tag = get_tag("multiworld_test", "Duplicate");
        for world_address in [world_a, world_b] {
            db.register_model(
                world_address,
                compute_selector_from_tag(&tag),
                &Ty::Struct(Struct {
                    name: tag.clone(),
                    children: vec![Member {
                        name: "id".to_string(),
                        key: true,
                        ty: Ty::Primitive(Primitive::U32(None)),
                    }],
                }),
                &Layout::Fixed(vec![]),
                Felt::ONE,
                Felt::TWO,
                0,
                0,
                1710754478_u64,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        }
        db.execute().await.unwrap();

        let messaging = Arc::new(Messaging::new(
            Default::default(),
            Arc::new(db.clone()),
            provider,
        ));
        let schema = build_schema(&pool, messaging, Arc::new(db.clone()))
            .await
            .expect("schema must build with the same model registered by two worlds");

        let sdl = schema.sdl();
        let field = "multiworldTestDuplicateModels";
        assert_eq!(
            sdl.matches(field).count() > 0,
            true,
            "model field missing from schema"
        );
    }
}
