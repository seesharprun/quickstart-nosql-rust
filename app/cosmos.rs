use serde_json;
use azure_data_cosmos::{CosmosClient, CosmosAccountEndpoint, CosmosAccountReference, PartitionKey, Query, RoutingStrategy};
use azure_data_cosmos::regions::Region;
use azure_core::credentials::Secret;
use futures::stream::StreamExt;
use crate::item::Item;

pub async fn run<F>(
    _endpoint: String,
    database_name: String,
    container_name: String,
    callback: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(String),
{
    callback("Current Status:\tStarting...".to_string());

    let cosmos_endpoint: CosmosAccountEndpoint = "<azure-cosmos-db-nosql-endpoint>".parse()?;
    let account = CosmosAccountReference::with_master_key(cosmos_endpoint, Secret::from("<azure-cosmos-db-nosql-read-write-key>"));
    let client = CosmosClient::builder().build(account, RoutingStrategy::ProximityTo(Region::EAST_US)).await?;

    callback("Client created".to_string());

    let database = client.database_client(&database_name);
    callback(format!("Get database:\t {}", database_name));

    let container = database.container_client(&container_name).await?;
    callback(format!("Get container:\t {}", container_name));

    {
        let item = Item {
            id: "aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb".to_string(),
            category: "gear-surf-surfboards".to_string(),
            name: "Yamba Surfboard".to_string(),
            quantity: 12,
            price: 850.00,
            clearance: false,
        };
        
        let partition_key = PartitionKey::from(item.category.clone());
        
        container.upsert_item(partition_key, item.clone(), None).await?;

        callback(format!("Upserted item:\t{}", item.id));
    }
    
    {
        let item = Item {
            id: "bbbbbbbb-1111-2222-3333-cccccccccccc".to_string(),
            category: "gear-surf-surfboards".to_string(),
            name: "Kiama Classic Surfboard".to_string(),
            quantity: 25,
            price: 790.00,
            clearance: true,
        };

        let partition_key = PartitionKey::from(item.category.clone());

        container.upsert_item(partition_key, item.clone(), None).await?;

        callback(format!("Upserted item:\t{}", item.id));
    }

    {
        let item_id = "aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb";
        let item_partition_key = "gear-surf-surfboards";

        let response = container.read_item::<Item>(item_partition_key, item_id, None).await?;

        let item = response.into_model()?;

        callback(format!("Read item:\t{}\t{}", item.id, item.category));
    }

    {
        let item_partition_key = "gear-surf-surfboards";

        let query = Query::from("SELECT * FROM c WHERE c.category = @category")
            .with_parameter("@category", item_partition_key)?;

        let mut pager = container.query_items::<Item>(query, item_partition_key, None)?;
        
        callback("Run query:".to_string());

        while let Some(item_result) = pager.next().await {
            let item = item_result?;
            callback(serde_json::to_string_pretty(&item).unwrap());
        }
    }
    
    callback("Current Status:\tStopping...".to_string());

    Ok(())
}
