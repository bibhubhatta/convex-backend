use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};

use anyhow::Result;
use iceberg::{
    transaction::{
        ApplyTransactionAction,
        Transaction,
    },
    NamespaceIdent,
    TableIdent,
};
use value::{
    obj,
    ConvexObject,
};

use super::*;
use crate::iceberg::IcebergChangeWriter;

fn source(table: &str) -> SourceTable {
    SourceTable {
        component: String::new(),
        table: table.to_owned(),
    }
}

fn revision(id: &str, ts: u64, document: Option<ConvexObject>) -> Change {
    Change {
        id: id.to_owned(),
        ts,
        document,
    }
}

#[tokio::test]
async fn committed_scans_restart_and_retirement() -> Result<()> {
    for format in [Format::Typed, Format::Json] {
        let catalog = memory_catalog().await?;
        let namespace = NamespaceIdent::new("test_export".to_owned());
        let selected = vec![source("docs"), source("empty"), source("deleted")];
        let mut writer = IcebergChangeWriter::staged_with_catalog(
            catalog.clone(),
            "test_export",
            "memory://warehouse",
            selected.clone(),
            &BTreeMap::new(),
            &BTreeSet::new(),
            HashMap::new(),
            format,
        )
        .await?;
        let first = obj!(
            "_id" => "a", "_creationTime" => 1.2345,
            "scalar" => 1.5, "nested" => obj!("explicit" => null)?,
            "only_null" => null, "_ts" => "rescued", "bytes" => vec![1u8, 2u8]
        )?;
        let later = obj!("_id" => "a", "_creationTime" => 1.2345, "scalar" => 9_007_199_254_740_992i64, "later" => true)?;
        let rescued =
            obj!("_id" => "b", "_creationTime" => 2.0, "scalar" => 9_007_199_254_740_993i64)?;
        writer
            .apply(
                &selected,
                vec![
                    (source("docs"), revision("a", 1, Some(first.clone()))),
                    (
                        source("deleted"),
                        revision(
                            "gone",
                            1,
                            Some(obj!("_id" => "gone", "_creationTime" => 0.0)?),
                        ),
                    ),
                ],
                BTreeMap::new(),
            )
            .await?;
        writer
            .apply(
                &[],
                vec![
                    (source("docs"), revision("a", 2, Some(later.clone()))),
                    (source("docs"), revision("b", 3, Some(rescued.clone()))),
                    (source("deleted"), revision("gone", 2, None)),
                ],
                BTreeMap::new(),
            )
            .await?;
        let ident = TableIdent::new(namespace.clone(), "app__docs".to_owned());
        let table = catalog.load_table(&ident).await?;
        let observed = scan(&table).await?;
        let expected = match format {
            Format::Json => vec![
                revision("a", 1, Some(first)),
                revision("a", 2, Some(later)),
                revision("b", 3, Some(rescued)),
            ],
            Format::Typed => vec![
                revision(
                    "a",
                    1,
                    Some(obj!(
                        "_id" => "a", "_creationTime" => 1.235,
                        "scalar" => 1.5, "nested" => obj!("explicit" => null)?,
                        "only_null" => null, "_ts" => "rescued", "bytes" => vec![1u8, 2u8]
                    )?),
                ),
                revision(
                    "a",
                    2,
                    Some(
                        obj!("_id" => "a", "_creationTime" => 1.235, "scalar" => 9_007_199_254_740_992f64, "later" => true)?,
                    ),
                ),
                revision("b", 3, Some(rescued)),
            ],
        };
        assert_eq!(observed, expected);
        assert_eq!(materialize(&observed)?.len(), 2);
        for name in ["empty", "deleted"] {
            let table = catalog
                .load_table(&TableIdent::new(namespace.clone(), format!("app__{name}")))
                .await?;
            assert_eq!(table_info(&table)?.source, source(name));
            let history = scan(&table).await?;
            assert_eq!(history.len(), if name == "empty" { 0 } else { 2 });
            assert!(materialize(&history)?.is_empty());
        }
        writer.retire_unselected(&[]).await?;
        let table = catalog.load_table(&ident).await?;
        assert!(table_info(&table)?.retired);
        assert_eq!(scan(&table).await?, observed);
        let locations = writer.locations().clone();
        let retired = writer.retired().unwrap().clone();
        drop(writer);
        clear_registrations(&*catalog, &namespace).await?;
        let mut writer = IcebergChangeWriter::staged_with_catalog(
            catalog.clone(),
            "test_export",
            "memory://warehouse",
            selected,
            &locations,
            &retired,
            HashMap::new(),
            format,
        )
        .await?;
        // Reconciliation lazily registers the committed pointers without adding
        // revisions.
        writer.retire_unselected(&[source("docs")]).await?;
        assert_eq!(scan(&catalog.load_table(&ident).await?).await?, observed);
        assert!(!table_info(&catalog.load_table(&ident).await?)?.retired);
    }
    Ok(())
}

#[tokio::test]
async fn invalid_documents_and_properties_are_errors() -> Result<()> {
    for format in [Format::Typed, Format::Json] {
        let catalog = memory_catalog().await?;
        let mut writer = IcebergChangeWriter::staged_with_catalog(
            catalog.clone(),
            "bad_export",
            "memory://warehouse",
            vec![source("docs")],
            &BTreeMap::new(),
            &BTreeSet::new(),
            HashMap::new(),
            format,
        )
        .await?;
        writer
            .apply(
                &[],
                vec![(source("docs"), revision("a", 1, Some(obj!("_id" => "a")?)))],
                BTreeMap::new(),
            )
            .await?;
        let ident = TableIdent::new(
            NamespaceIdent::new("bad_export".to_owned()),
            "app__docs".to_owned(),
        );
        let table = catalog.load_table(&ident).await?;
        assert!(scan(&table)
            .await
            .unwrap_err()
            .to_string()
            .contains("creation time"));
        let tx = Transaction::new(&table);
        let table = tx
            .update_table_properties()
            .set("convex.porter.retired".to_owned(), "invalid".to_owned())
            .apply(tx)?
            .commit(&*catalog)
            .await?;
        assert!(table_info(&table).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn established_column_nulls_and_json_document_ids() -> Result<()> {
    for format in [Format::Typed, Format::Json] {
        let catalog = memory_catalog().await?;
        let mut writer = IcebergChangeWriter::staged_with_catalog(
            catalog.clone(),
            "null_export",
            "memory://warehouse",
            vec![],
            &BTreeMap::new(),
            &BTreeSet::new(),
            HashMap::new(),
            format,
        )
        .await?;
        writer.apply(&[], vec![(source("docs"), revision("a", 1, Some(obj!("_id" => "a", "_creationTime" => 0.0, "s" => "text", "n" => 5i64, "b" => true)?)))], BTreeMap::new()).await?;
        writer
            .apply(
                &[],
                vec![
                    (
                        source("docs"),
                        revision(
                            "a",
                            2,
                            Some(obj!("_id" => "a", "_creationTime" => 0.0, "s" => null)?),
                        ),
                    ),
                    (
                        source("docs"),
                        revision("a", 3, Some(obj!("_id" => "a", "_creationTime" => 0.0)?)),
                    ),
                ],
                BTreeMap::new(),
            )
            .await?;
        let ident = TableIdent::new(
            NamespaceIdent::new("null_export".to_owned()),
            "app__docs".to_owned(),
        );
        let history = scan(&catalog.load_table(&ident).await?).await?;
        assert_eq!(
            history[0].document,
            Some(
                obj!("_id" => "a", "_creationTime" => 0.0, "s" => "text", "n" => 5i64, "b" => true)?
            )
        );
        let null_document = match format {
            Format::Typed => obj!("_id" => "a", "_creationTime" => 0.0)?,
            Format::Json => obj!("_id" => "a", "_creationTime" => 0.0, "s" => null)?,
        };
        assert_eq!(history[1].document, Some(null_document));
        assert_eq!(
            history[2].document,
            Some(obj!("_id" => "a", "_creationTime" => 0.0)?)
        );
        if format == Format::Json {
            writer
                .apply(
                    &[],
                    vec![(
                        source("docs"),
                        revision(
                            "a",
                            4,
                            Some(obj!("_id" => "wrong", "_creationTime" => 0.0)?),
                        ),
                    )],
                    BTreeMap::new(),
                )
                .await?;
            assert!(scan(&catalog.load_table(&ident).await?)
                .await
                .unwrap_err()
                .to_string()
                .contains("ID disagrees"));
        }
    }
    Ok(())
}
