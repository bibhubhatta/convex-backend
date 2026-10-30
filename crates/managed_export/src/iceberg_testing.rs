//! Utilities for observing committed exports in downstream tests.
//!
//! Enable `managed_export/testing`. Scans preserve the exported representation:
//! typed doubles cannot reveal whether the source was an exactly representable
//! integer, and nulls in established columns cannot distinguish missing fields
//! from explicit null. Nulls retained in JSON or `_rescued` remain explicit.
//! Typed creation times are rounded to microseconds and returned as
//! milliseconds. Use an independently constructed expectation with these narrow
//! adjustments; nested JSON and rescued values retain their Convex types.
//!
//! ```ignore
//! use managed_export::iceberg::testing::{memory_catalog, scan, table_info, materialize};
//! let catalog = memory_catalog().await?;
//! // Pass catalog.clone() to IcebergChangeWriter::staged_with_catalog, then apply pages.
//! let table = catalog.load_table(&ident).await?;
//! let info = table_info(&table)?;
//! let history = scan(&table).await?;
//! let documents = materialize(&history)?;
//! // Keep info.source alongside documents; info.retired selects active tables.
//! ```

use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    sync::Arc,
};

use anyhow::{
    ensure,
    Context,
    Result,
};
use arrow_iceberg::{
    Array,
    BooleanArray,
    Float64Array,
    Int64Array,
    LargeBinaryArray,
    RecordBatch,
    StringArray,
    TimestampMicrosecondArray,
};
use futures::TryStreamExt;
use iceberg::{
    memory::{
        MemoryCatalogBuilder,
        MEMORY_CATALOG_WAREHOUSE,
    },
    spec::{
        PrimitiveType,
        Type,
    },
    table::{
        StaticTable,
        Table,
    },
    Catalog,
    CatalogBuilder,
    NamespaceIdent,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use value::{
    ConvexObject,
    ConvexValue,
    FieldName,
};

use super::{
    columns,
    json,
    Change,
    Format,
    SourceTable,
};

/// Create an isolated catalog and backing memory store for each call.
pub async fn memory_catalog() -> Result<Arc<dyn Catalog>> {
    Ok(Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::Memory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    "memory://warehouse".to_owned(),
                )]),
            )
            .await?,
    ))
}

/// Clear registrations before reopening a staged writer from committed
/// locations. Keep the catalog alive across the restart: its memory store owns
/// all files. `drop_table` retains files; `purge_table` would destroy the
/// restart state.
pub async fn clear_registrations(catalog: &dyn Catalog, namespace: &NamespaceIdent) -> Result<()> {
    for ident in catalog.list_tables(namespace).await? {
        catalog.drop_table(&ident).await?;
    }
    Ok(())
}

/// Source identity and lifecycle of one physical Iceberg table, including empty
/// tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableInfo {
    pub source: SourceTable,
    pub format: Format,
    pub retired: bool,
}

/// Validate the export properties and required schema on loaded metadata.
pub fn table_info(table: &Table) -> Result<TableInfo> {
    let properties = table.metadata().properties();
    let property = |key| {
        properties
            .get(key)
            .with_context(|| format!("Missing table property {key}"))
    };
    let format: Format = property("convex.porter.encoding")?.parse()?;
    let expected = match format {
        Format::Typed => columns::metadata_schema()?,
        Format::Json => json::change_log_schema()?,
    };
    let fields = table.metadata().current_schema().as_struct().fields();
    ensure!(
        fields.starts_with(expected.as_struct().fields()),
        "Invalid export metadata schema"
    );
    if format == Format::Json {
        ensure!(
            fields.len() == expected.as_struct().fields().len(),
            "Unexpected JSON columns"
        );
    }
    Ok(TableInfo {
        source: SourceTable {
            component: property("convex.porter.component")?.clone(),
            table: property("convex.porter.table")?.clone(),
        },
        format,
        retired: properties
            .get("convex.porter.retired")
            .map(|v| v.parse::<bool>())
            .transpose()
            .context("Invalid retirement property")?
            .unwrap_or(false),
    })
}

/// Reload the metadata location held by `table` and scan its committed
/// snapshot. Load a fresh table from the catalog first to observe newer
/// commits. The result includes `_id` and `_creationTime` in live documents and
/// retains all revisions. Retirement does not filter historical data.
pub async fn scan(table: &Table) -> Result<Vec<Change>> {
    let table = StaticTable::from_metadata_file(
        table.metadata_location_result()?,
        table.identifier().clone(),
        table.file_io().clone(),
    )
    .await
    .context("Reloading committed Iceberg metadata")?
    .into_table();
    let info = table_info(&table)?;
    let changes = match info.format {
        Format::Typed => read_typed(&table).await,
        Format::Json => read_json(&table).await,
    }
    .with_context(|| format!("Scanning {}", table.identifier()))?;
    for change in &changes {
        ensure!(!change.id.is_empty(), "Empty document ID");
        if let Some(document) = &change.document {
            ensure!(
                document.get("_id") == Some(&ConvexValue::try_from(change.id.clone())?),
                "Document ID disagrees with revision {}",
                change.id
            );
            ensure!(
                matches!(document.get("_creationTime"), Some(ConvexValue::Float64(ms)) if ms.is_finite()),
                "Missing or invalid creation time for {}",
                change.id
            );
        }
    }
    Ok(changes)
}

/// Materialize history from a single source table, rejecting conflicts at every
/// timestamp (even if a later revision hides them). Call separately per table;
/// identical document IDs in different source tables are independent.
pub fn materialize(changes: &[Change]) -> Result<BTreeMap<String, ConvexObject>> {
    let mut revisions = BTreeMap::new();
    for change in changes {
        let key = (&change.id, change.ts);
        if let Some(previous) = revisions.insert(key, &change.document) {
            ensure!(
                previous == &change.document,
                "Conflicting revision for {} at {}",
                change.id,
                change.ts
            );
        }
    }
    let mut live = BTreeMap::new();
    for ((id, _), document) in revisions {
        if let Some(document) = document {
            live.insert(id.clone(), document.clone());
        } else {
            live.remove(id);
        }
    }
    Ok(live)
}

/// Normalize only creation-time precision for an independently built
/// expectation. Other numeric fields should be compared according to their
/// observed schema.
pub fn creation_time_ms(ms: f64) -> Result<f64> {
    let micros = (ms * 1000.0).round();
    ensure!(
        ms.is_finite() && micros >= i64::MIN as f64 && micros < -(i64::MIN as f64),
        "Creation time out of range"
    );
    Ok(micros as i64 as f64 / 1000.0)
}

fn array<'a, A: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a A> {
    batch
        .column_by_name(name)
        .with_context(|| format!("Missing column {name}"))?
        .as_any()
        .downcast_ref::<A>()
        .with_context(|| format!("Unexpected Arrow type for {name}"))
}

async fn read_json(table: &Table) -> Result<Vec<Change>> {
    let batches: Vec<_> = table
        .scan()
        .build()?
        .to_arrow()
        .await?
        .try_collect()
        .await?;
    let mut changes = vec![];
    for batch in batches {
        let ids = array::<StringArray>(&batch, "_id")?;
        let timestamps = array::<Int64Array>(&batch, "ts")?;
        let deleted = array::<BooleanArray>(&batch, "deleted")?;
        let payloads = array::<StringArray>(&batch, "payload")?;
        for index in 0..batch.num_rows() {
            ensure!(
                !ids.is_null(index) && !timestamps.is_null(index) && !deleted.is_null(index),
                "Null revision metadata at document {index}"
            );
            ensure!(
                deleted.value(index) == payloads.is_null(index),
                "Deletion flag disagrees with payload for {}",
                ids.value(index)
            );
            let document = if deleted.value(index) {
                None
            } else {
                Some(ConvexObject::try_from(ConvexValue::try_from(
                    serde_json::from_str::<serde_json::Value>(payloads.value(index))
                        .context("Invalid JSON payload")?,
                )?)?)
            };
            changes.push(Change {
                id: ids.value(index).to_owned(),
                ts: timestamps.value(index).try_into()?,
                document,
            });
        }
    }
    changes.sort_by_key(|c| (c.id.clone(), c.ts));
    Ok(changes)
}

pub(crate) async fn read_typed(table: &Table) -> Result<Vec<Change>> {
    let batches: Vec<_> = table
        .scan()
        .build()?
        .to_arrow()
        .await?
        .try_collect()
        .await?;
    let schema = table.metadata().current_schema();
    let mut changes = vec![];
    for batch in batches {
        let ids = array::<StringArray>(&batch, columns::ID_COLUMN)?;
        let ts = array::<Int64Array>(&batch, columns::TS_COLUMN)?;
        let deleted = array::<BooleanArray>(&batch, columns::DELETED_COLUMN)?;
        let rescued = array::<StringArray>(&batch, columns::RESCUED_COLUMN)?;
        let created = array::<TimestampMicrosecondArray>(&batch, columns::CREATION_TIME_COLUMN)?;
        for index in 0..batch.num_rows() {
            ensure!(
                !ids.is_null(index) && !ts.is_null(index) && !deleted.is_null(index),
                "Null revision metadata at document {index}"
            );
            let document = if deleted.value(index) {
                for name in batch.schema().fields().iter().map(|f| f.name()) {
                    if ![
                        columns::ID_COLUMN,
                        columns::TS_COLUMN,
                        columns::DELETED_COLUMN,
                    ]
                    .contains(&name.as_str())
                    {
                        ensure!(
                            batch
                                .column_by_name(name)
                                .context("Missing tombstone column")?
                                .is_null(index),
                            "Tombstone {} contains field {name}",
                            ids.value(index)
                        );
                    }
                }
                None
            } else {
                let mut fields: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
                if !rescued.is_null(index) {
                    let ConvexValue::Object(misfits) = ConvexValue::from_clean_lossless(
                        serde_json::from_str(rescued.value(index)).with_context(|| {
                            format!(
                                "Invalid _rescued JSON for {} at {}",
                                ids.value(index),
                                ts.value(index)
                            )
                        })?,
                    )?
                    else {
                        anyhow::bail!("_rescued is not an object");
                    };
                    fields.extend(misfits);
                }
                let metadata = [
                    columns::ID_COLUMN,
                    columns::TS_COLUMN,
                    columns::DELETED_COLUMN,
                    columns::CREATION_TIME_COLUMN,
                    columns::RESCUED_COLUMN,
                ];
                for field in schema.as_struct().fields() {
                    let name = field.name.as_str();
                    let array = batch
                        .column_by_name(name)
                        .with_context(|| format!("Missing column {name}"))?;
                    if metadata.contains(&name) || array.is_null(index) {
                        continue;
                    }
                    let value = match &*field.field_type {
                        Type::Primitive(PrimitiveType::String)
                            if field.doc.as_deref() == Some(columns::JSON_DOC) =>
                        {
                            let text = array
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .context("Unexpected Arrow column type")?;
                            ConvexValue::from_clean_lossless(
                                serde_json::from_str(text.value(index)).with_context(|| {
                                    format!(
                                        "Invalid JSON in column {name} for {} at {}",
                                        ids.value(index),
                                        ts.value(index)
                                    )
                                })?,
                            )?
                        },
                        Type::Primitive(PrimitiveType::String) => {
                            let text = array
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .context("Unexpected Arrow column type")?;
                            ConvexValue::try_from(text.value(index).to_owned())?
                        },
                        Type::Primitive(PrimitiveType::Double) => {
                            let doubles = array
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .context("Unexpected Arrow column type")?;
                            ConvexValue::Float64(doubles.value(index))
                        },
                        Type::Primitive(PrimitiveType::Long) => {
                            let longs = array
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .context("Unexpected Arrow column type")?;
                            ConvexValue::Int64(longs.value(index))
                        },
                        Type::Primitive(PrimitiveType::Boolean) => {
                            let bools = array
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .context("Unexpected Arrow column type")?;
                            ConvexValue::Boolean(bools.value(index))
                        },
                        Type::Primitive(PrimitiveType::Binary) => {
                            let bytes = array
                                .as_any()
                                .downcast_ref::<LargeBinaryArray>()
                                .context("Unexpected Arrow column type")?;
                            ConvexValue::try_from(bytes.value(index).to_vec())?
                        },
                        other => anyhow::bail!("Unsupported type {other} in column {name}"),
                    };
                    ensure!(
                        fields.insert(name.parse()?, value).is_none(),
                        "Field {name} occurs in both column and rescue"
                    );
                }
                ensure!(
                    !fields.contains_key(&"_id".parse::<FieldName>()?)
                        && !fields.contains_key(&"_creationTime".parse::<FieldName>()?),
                    "Rescued document metadata"
                );
                fields.insert(
                    "_id".parse()?,
                    ConvexValue::try_from(ids.value(index).to_owned())?,
                );
                if !created.is_null(index) {
                    fields.insert(
                        "_creationTime".parse()?,
                        ConvexValue::Float64(created.value(index) as f64 / 1000.0),
                    );
                }
                Some(ConvexObject::try_from(fields)?)
            };
            changes.push(Change {
                id: ids.value(index).to_owned(),
                ts: ts.value(index).try_into()?,
                document,
            });
        }
    }
    changes.sort_by_key(|c| (c.id.clone(), c.ts));
    Ok(changes)
}
