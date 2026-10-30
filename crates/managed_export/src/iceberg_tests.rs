use anyhow::Result;
use serde_json::{
    json,
    Value as JsonValue,
};
use value::{
    ConvexObject,
    ConvexValue,
};

pub(crate) use super::testing::memory_catalog;
use super::*;

/// `payload` is any JSON value; the document holds it in field `v`.
pub(crate) fn change(id: &str, ts: u64, payload: Option<&str>) -> Change {
    let document = payload.map(|payload| {
        let payload: JsonValue = serde_json::from_str(payload).expect("payload is JSON");
        document(json!({ "v": payload })).expect("payload is a Convex value")
    });
    Change {
        id: id.to_owned(),
        ts,
        document,
    }
}

fn document(json: JsonValue) -> Result<ConvexObject> {
    ConvexObject::try_from(ConvexValue::try_from(json)?)
}

pub(crate) async fn read(table: &Table) -> Result<Vec<Change>> {
    let mut changes = super::testing::read_typed(table).await?;
    for change in &mut changes {
        if let Some(document) = &change.document {
            change.document = Some(ConvexObject::try_from(
                document
                    .iter()
                    .filter(|(name, _)| {
                        let name: &str = name;
                        !["_id", "_creationTime"].contains(&name)
                    })
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect::<std::collections::BTreeMap<_, _>>(),
            )?);
        }
    }
    Ok(changes)
}
