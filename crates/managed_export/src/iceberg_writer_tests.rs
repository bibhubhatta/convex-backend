use std::collections::{
    BTreeMap,
    HashMap,
};

use common::shapes::reduced::{
    ReducedField,
    ReducedShape,
};

use super::{
    super::tests::{
        change,
        memory_catalog,
        read,
    },
    *,
};

fn source(table: &str) -> SourceTable {
    component_source("", table)
}

fn component_source(component: &str, table: &str) -> SourceTable {
    SourceTable {
        component: component.to_owned(),
        table: table.to_owned(),
    }
}

async fn open(catalog: Arc<dyn Catalog>) -> Result<IcebergChangeWriter> {
    open_selecting(catalog, vec![]).await
}

async fn open_selecting(
    catalog: Arc<dyn Catalog>,
    selected: Vec<SourceTable>,
) -> Result<IcebergChangeWriter> {
    IcebergChangeWriter::open(
        catalog,
        "convex_chess_wandering_fish_513",
        "memory://warehouse",
        selected,
        Format::Typed,
    )
    .await
}

async fn staged(
    catalog: Arc<dyn Catalog>,
    selected: Vec<SourceTable>,
    locations: &BTreeMap<String, String>,
    retired: &BTreeSet<String>,
    snapshot_properties: HashMap<String, String>,
) -> Result<IcebergChangeWriter> {
    IcebergChangeWriter::staged_with_catalog(
        catalog,
        "convex_chess_wandering_fish_513",
        "memory://warehouse",
        selected,
        locations,
        retired,
        snapshot_properties,
        Format::Typed,
    )
    .await
}

use super::super::testing::clear_registrations;

async fn changes(writer: &IcebergChangeWriter, table: &str) -> Result<Vec<Change>> {
    let ident = TableIdent::new(
        writer.namespace.clone(),
        IcebergChangeWriter::table_name(&source(table))?,
    );
    read(&writer.catalog.load_table(&ident).await?).await
}

#[tokio::test]
async fn truncates_recreate_tables_and_changes_append() -> Result<()> {
    let catalog = memory_catalog().await?;
    let mut writer = open(catalog.clone()).await?;
    writer
        .apply(
            &[source("menu"), source("empty")],
            vec![(source("menu"), change("burger", 10, Some("10")))],
            BTreeMap::new(),
        )
        .await?;
    writer
        .apply(
            &[],
            vec![(source("menu"), change("burger", 12, None))],
            BTreeMap::new(),
        )
        .await?;
    assert_eq!(
        changes(&writer, "menu").await?,
        vec![change("burger", 10, Some("10")), change("burger", 12, None)]
    );
    assert!(changes(&writer, "empty").await?.is_empty());
    let properties = writer.tables[&source("menu")].metadata().properties();
    assert_eq!(properties[COMPONENT_PROPERTY], "");
    assert_eq!(properties[TABLE_PROPERTY], "menu");

    writer
        .apply(
            &[source("menu")],
            vec![(source("menu"), change("soup", 5, Some("6")))],
            BTreeMap::new(),
        )
        .await?;
    assert_eq!(
        changes(&writer, "menu").await?,
        vec![change("soup", 5, Some("6"))]
    );

    let mut reopened = open(catalog.clone()).await?;
    reopened
        .apply(
            &[],
            vec![(source("menu"), change("burger", 20, Some("12")))],
            BTreeMap::new(),
        )
        .await?;
    assert_eq!(
        changes(&reopened, "menu").await?,
        vec![
            change("burger", 20, Some("12")),
            change("soup", 5, Some("6"))
        ]
    );
    Ok(())
}

#[tokio::test]
async fn locations_track_every_commit_and_summaries_name_the_page() -> Result<()> {
    let catalog = memory_catalog().await?;
    let mut writer = staged(
        catalog.clone(),
        vec![source("menu"), source("empty")],
        &BTreeMap::new(),
        &BTreeSet::new(),
        HashMap::from([("convex.page".to_owned(), "7".to_owned())]),
    )
    .await?;
    writer
        .apply(
            &[source("menu"), source("empty")],
            vec![(source("menu"), change("burger", 10, Some("10")))],
            BTreeMap::new(),
        )
        .await?;
    for name in ["app__menu", "app__empty"] {
        let table = catalog
            .load_table(&TableIdent::new(writer.namespace.clone(), name.to_owned()))
            .await?;
        assert_eq!(
            writer.locations()[name],
            table.metadata_location_result()?,
            "{name}"
        );
    }
    let menu = &writer.tables[&source("menu")];
    let snapshot = menu.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.summary().additional_properties["convex.page"], "7");
    let after_one_document = writer.bytes_written();
    assert!(after_one_document > 0, "one Parquet file was written");

    writer
        .apply(&[source("menu")], vec![], BTreeMap::new())
        .await?;
    assert_eq!(
        writer.bytes_written(),
        after_one_document,
        "recreating a table writes metadata only"
    );
    let menu = catalog
        .load_table(&TableIdent::new(
            writer.namespace.clone(),
            "app__menu".to_owned(),
        ))
        .await?;
    assert_eq!(
        writer.locations()["app__menu"],
        menu.metadata_location_result()?
    );
    Ok(())
}

#[tokio::test]
async fn staged_writers_restore_committed_history_and_reset_byte_accounting() -> Result<()> {
    let catalog = memory_catalog().await?;
    let selected = vec![source("menu"), source("empty")];
    let mut writer = staged(
        catalog.clone(),
        selected.clone(),
        &BTreeMap::new(),
        &BTreeSet::new(),
        HashMap::new(),
    )
    .await?;
    assert_eq!(writer.bytes_written(), 0);
    writer
        .apply(
            &selected,
            vec![(source("menu"), change("burger", 10, Some("10")))],
            BTreeMap::new(),
        )
        .await?;
    let after_create = writer.bytes_written();
    writer
        .apply(
            &[],
            vec![
                (source("menu"), change("burger", 11, Some("11"))),
                (source("menu"), change("burger", 12, None)),
            ],
            BTreeMap::new(),
        )
        .await?;
    assert!(writer.bytes_written() > after_create);
    let history = vec![
        change("burger", 10, Some("10")),
        change("burger", 11, Some("11")),
        change("burger", 12, None),
    ];
    assert_eq!(changes(&writer, "menu").await?, history);
    assert!(changes(&writer, "empty").await?.is_empty());
    writer.retire_unselected(&[source("menu")]).await?;
    let locations = writer.locations().clone();
    let retired = writer.retired().unwrap().clone();
    assert_eq!(retired, BTreeSet::from(["app__empty".to_owned()]));
    let namespace = writer.namespace.clone();
    drop(writer);
    clear_registrations(&*catalog, &namespace).await?;

    let mut writer = staged(
        catalog.clone(),
        selected.clone(),
        &locations,
        &retired,
        HashMap::from([("convex.page".to_owned(), "8".to_owned())]),
    )
    .await?;
    assert_eq!(writer.bytes_written(), 0);
    assert_eq!(writer.locations(), &locations);
    assert_eq!(writer.retired(), Some(&retired));
    assert!(catalog.list_tables(&namespace).await?.is_empty());
    writer
        .apply(
            &[],
            vec![(source("menu"), change("burger", 13, Some("13")))],
            BTreeMap::new(),
        )
        .await?;
    assert!(writer.bytes_written() > 0);
    let mut expected = history;
    expected.push(change("burger", 13, Some("13")));
    assert_eq!(changes(&writer, "menu").await?, expected);
    assert_ne!(writer.locations()["app__menu"], locations["app__menu"]);
    assert_eq!(writer.locations()["app__empty"], locations["app__empty"]);
    let menu = catalog
        .load_table(&TableIdent::new(namespace.clone(), "app__menu".to_owned()))
        .await?;
    assert_eq!(
        menu.metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties["convex.page"],
        "8"
    );

    writer.retire_unselected(&selected).await?;
    assert_eq!(writer.retired(), Some(&BTreeSet::new()));
    assert!(changes(&writer, "empty").await?.is_empty());
    assert_ne!(writer.locations()["app__empty"], locations["app__empty"]);
    let after_append = writer.bytes_written();
    writer
        .apply(&[source("menu")], vec![], BTreeMap::new())
        .await?;
    assert!(changes(&writer, "menu").await?.is_empty());
    assert_eq!(writer.bytes_written(), after_append);
    Ok(())
}

#[tokio::test]
async fn staged_catalog_requires_an_empty_namespace() -> Result<()> {
    let catalog = memory_catalog().await?;
    let mut writer = open(catalog.clone()).await?;
    writer
        .apply(&[source("menu")], vec![], BTreeMap::new())
        .await?;
    let locations = writer.locations().clone();
    let namespace = writer.namespace.clone();
    drop(writer);
    let error = staged(
        catalog.clone(),
        vec![source("menu")],
        &locations,
        &BTreeSet::new(),
        HashMap::new(),
    )
    .await
    .err()
    .expect("cached catalog pointers must be rejected");
    assert!(
        error.to_string().contains("no table registrations"),
        "{error:#}"
    );
    assert_eq!(catalog.list_tables(&namespace).await?.len(), 1);

    let isolated = memory_catalog().await?;
    let mut writer = staged(
        isolated.clone(),
        vec![source("menu")],
        &BTreeMap::new(),
        &BTreeSet::new(),
        HashMap::new(),
    )
    .await?;
    let ident = TableIdent::new(namespace, "app__menu".to_owned());
    assert!(!isolated.table_exists(&ident).await?);
    writer
        .apply(&[source("menu")], vec![], BTreeMap::new())
        .await?;
    let table = isolated.load_table(&ident).await?;
    assert!(!table.file_io().exists(&locations["app__menu"]).await?);
    assert!(
        catalog
            .load_table(&ident)
            .await?
            .file_io()
            .exists(&locations["app__menu"])
            .await?
    );
    assert_eq!(writer.bytes_written(), 0);
    Ok(())
}

#[tokio::test]
async fn names_follow_components_and_keep_case_in_directories() -> Result<()> {
    let mut writer = open(memory_catalog().await?).await?;
    let node = component_source("aggregate/inner", "btreeNode");
    writer
        .apply(
            &[source("authRefreshTokens"), node.clone()],
            vec![],
            BTreeMap::new(),
        )
        .await?;
    assert_eq!(writer.tables.len(), 2);
    for (source, name, location) in [
        (
            &node,
            "aggregate__inner__btreenode",
            "memory://warehouse/aggregate__inner/btreeNode",
        ),
        (
            &source("authRefreshTokens"),
            "app__authrefreshtokens",
            "memory://warehouse/app/authRefreshTokens",
        ),
    ] {
        let table = &writer.tables[source];
        assert_eq!(table.identifier().name(), name);
        assert_eq!(table.metadata().location(), location);
    }
    Ok(())
}

#[tokio::test]
async fn colliding_sources_fail_before_any_write() -> Result<()> {
    let mut writer = open(memory_catalog().await?).await?;
    let error = writer
        .apply(&[source("Users"), source("users")], vec![], BTreeMap::new())
        .await
        .expect_err("a colliding page must be rejected");
    assert!(error.to_string().contains("both map to"), "{error:#}");
    assert!(writer.tables.is_empty());
    writer
        .apply(&[source("menu")], vec![], BTreeMap::new())
        .await?;
    let error = open_selecting(
        writer.catalog.clone(),
        vec![source("Users"), source("users")],
    )
    .await
    .err()
    .expect("a colliding selection must be rejected");
    assert!(error.to_string().contains("both map to"), "{error:#}");
    assert_eq!(
        writer.catalog.list_tables(&writer.namespace).await?.len(),
        1
    );
    let deep = "s3://b/".to_owned() + &"p/".repeat(450);
    let error = IcebergChangeWriter::check_names(&deep, &[source("users")])
        .expect_err("an overlong location must be rejected");
    assert!(error.to_string().contains("longer than"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn a_new_table_replaces_a_leftover_with_the_same_name() -> Result<()> {
    let mut writer = open(memory_catalog().await?).await?;
    writer
        .apply(
            &[source("Users")],
            vec![(source("Users"), change("ada", 1, Some("{}")))],
            BTreeMap::new(),
        )
        .await?;
    for replacement in [source("users"), component_source("app", "users")] {
        writer = open(writer.catalog.clone()).await?;
        let error = writer
            .apply(
                &[],
                vec![(replacement.clone(), change("grace", 2, Some("{}")))],
                BTreeMap::new(),
            )
            .await
            .expect_err("a different source must truncate before reusing a Glue name");
        assert!(
            error.to_string().contains("belongs to another source"),
            "{error:#}"
        );
        writer
            .apply(
                std::slice::from_ref(&replacement),
                vec![(replacement.clone(), change("grace", 2, Some("{}")))],
                BTreeMap::new(),
            )
            .await?;
        assert_eq!(writer.tables.keys().collect::<Vec<_>>(), [&replacement]);
        assert_eq!(
            read(&writer.tables[&replacement]).await?,
            vec![change("grace", 2, Some("{}"))]
        );
    }
    Ok(())
}

#[tokio::test]
async fn retirement_follows_the_selection() -> Result<()> {
    let catalog = memory_catalog().await?;
    let mut writer = open(catalog).await?;
    writer
        .apply(&[source("menu"), source("orders")], vec![], BTreeMap::new())
        .await?;
    let retired = |writer: &IcebergChangeWriter, table: &str| {
        writer.tables[&source(table)]
            .metadata()
            .properties()
            .get(RETIRED_PROPERTY)
            .cloned()
    };
    writer = open(writer.catalog.clone()).await?;
    writer.retire_unselected(&[source("menu")]).await?;
    assert_eq!(retired(&writer, "menu"), None);
    assert_eq!(retired(&writer, "orders").as_deref(), Some("true"));
    writer
        .retire_unselected(&[source("menu"), source("orders")])
        .await?;
    assert_eq!(retired(&writer, "orders").as_deref(), Some("false"));
    Ok(())
}

#[tokio::test]
async fn foreign_tables_are_checked_when_used_or_reconciled() -> Result<()> {
    let catalog = memory_catalog().await?;
    open(catalog.clone()).await?;
    catalog
        .create_table(
            &NamespaceIdent::new("convex_chess_wandering_fish_513".to_owned()),
            TableCreation::builder()
                .name("app__menu".to_owned())
                .schema(metadata_schema()?)
                .build(),
        )
        .await?;
    let mut writer = open(catalog).await?;
    writer
        .apply(
            &[],
            vec![(source("orders"), change("order", 1, Some("{}")))],
            BTreeMap::new(),
        )
        .await?;
    assert_eq!(changes(&writer, "orders").await?.len(), 1);

    let error = writer
        .apply(
            &[],
            vec![(source("menu"), change("burger", 1, Some("{}")))],
            BTreeMap::new(),
        )
        .await
        .expect_err("appending to a foreign table must fail");
    assert!(error.to_string().contains("not written by"), "{error:#}");
    let error = writer
        .apply(&[source("menu")], vec![], BTreeMap::new())
        .await
        .expect_err("truncating a foreign table must fail");
    assert!(error.to_string().contains("not written by"), "{error:#}");
    let error = writer
        .retire_unselected(&[])
        .await
        .expect_err("retirement must validate the full catalog");
    assert!(error.to_string().contains("not written by"), "{error:#}");
    Ok(())
}

#[tokio::test]
async fn a_known_retired_set_loads_only_the_tables_a_page_uses() -> Result<()> {
    let catalog = memory_catalog().await?;
    let mut writer = open(catalog.clone()).await?;
    writer
        .apply(&[source("menu"), source("orders")], vec![], BTreeMap::new())
        .await?;
    writer
        .retire_unselected(&[source("menu"), source("orders")])
        .await?;
    let locations = writer.locations().clone();
    assert_eq!(writer.retired(), Some(&BTreeSet::new()));

    let retired = writer.retired().unwrap().clone();
    let namespace = writer.namespace.clone();
    drop(writer);
    clear_registrations(&*catalog, &namespace).await?;
    let mut writer = staged(
        catalog.clone(),
        vec![source("menu"), source("orders")],
        &locations,
        &retired,
        HashMap::new(),
    )
    .await?;
    let registered = async |writer: &IcebergChangeWriter| -> Result<Vec<String>> {
        Ok(catalog
            .list_tables(&writer.namespace)
            .await?
            .into_iter()
            .map(|ident| ident.name().to_owned())
            .collect())
    };
    assert!(registered(&writer).await?.is_empty());

    writer
        .apply(
            &[],
            vec![(source("menu"), change("burger", 1, Some("{}")))],
            BTreeMap::new(),
        )
        .await?;
    writer
        .retire_unselected(&[source("menu"), source("orders")])
        .await?;
    assert_eq!(registered(&writer).await?, ["app__menu"]);

    writer.retire_unselected(&[source("menu")]).await?;
    assert_eq!(registered(&writer).await?.len(), 2);
    assert_eq!(
        writer.retired(),
        Some(&BTreeSet::from(["app__orders".to_owned()]))
    );
    assert_eq!(
        writer.tables[&source("orders")].metadata().properties()[RETIRED_PROPERTY],
        "true"
    );
    Ok(())
}

#[tokio::test]
async fn an_export_in_the_json_format_keeps_its_payload_column() -> Result<()> {
    let catalog = memory_catalog().await?;
    let mut writer = IcebergChangeWriter::open(
        catalog.clone(),
        "convex_chess_wandering_fish_513",
        "memory://warehouse",
        vec![],
        Format::Json,
    )
    .await?;
    writer
        .apply(
            &[],
            vec![(source("menu"), change("burger", 10, Some("10")))],
            BTreeMap::new(),
        )
        .await?;
    let ident = TableIdent::new(
        writer.namespace.clone(),
        IcebergChangeWriter::table_name(&source("menu"))?,
    );
    let table = catalog.load_table(&ident).await?;
    assert_eq!(
        table.metadata().current_schema().as_struct(),
        json::change_log_schema()?.as_struct()
    );
    assert_eq!(
        table.metadata().properties()[FORMAT_PROPERTY],
        "convex-change-log-json-v1"
    );
    // A typed writer stops at a JSON table instead of appending to it.
    let mut typed = open(catalog).await?;
    let error = typed
        .apply(
            &[],
            vec![(source("menu"), change("soup", 11, None))],
            BTreeMap::new(),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("convex-change-log-json-v1"),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_new_table_starts_with_its_seeds() -> Result<()> {
    let catalog = memory_catalog().await?;
    let mut writer = open(catalog.clone()).await?;
    let page = vec![(source("menu"), change("burger", 10, Some("10")))];
    assert_eq!(
        writer.new_tables(&[source("empty")], &page)?,
        BTreeSet::from([source("empty"), source("menu")])
    );
    let shape = ReducedShape::Object(BTreeMap::from([(
        "v".parse()?,
        ReducedField {
            optional: false,
            shape: ReducedShape::Union(BTreeSet::from([ReducedShape::Int64, ReducedShape::String])),
        },
    )]));
    let seeds = BTreeMap::from([
        (source("menu"), columns::seeds(&shape, |_| None)?),
        (source("empty"), columns::seeds(&shape, |_| None)?),
    ]);
    writer.apply(&[source("empty")], page, seeds).await?;
    // A truncated table is recreated with its seeds too.
    let empty = TableIdent::new(
        writer.namespace.clone(),
        IcebergChangeWriter::table_name(&source("empty"))?,
    );
    let empty = catalog.load_table(&empty).await?;
    assert!(empty
        .metadata()
        .current_schema()
        .field_by_name("v")
        .is_some());
    // The seed typed `v` as JSON before the page's int64 could make it a long.
    let ident = TableIdent::new(
        writer.namespace.clone(),
        IcebergChangeWriter::table_name(&source("menu"))?,
    );
    let table = catalog.load_table(&ident).await?;
    let v = table
        .metadata()
        .current_schema()
        .field_by_name("v")
        .context("seeded column")?;
    assert_eq!(v.doc.as_deref(), Some(columns::JSON_DOC));
    assert_eq!(changes(&writer, "menu").await?.len(), 1);
    // Known tables are not new again; truncated ones are.
    let next = vec![(source("menu"), change("soup", 11, Some("\"x\"")))];
    assert!(writer.new_tables(&[], &next)?.is_empty());
    assert_eq!(
        writer.new_tables(&[source("menu")], &[])?,
        BTreeSet::from([source("menu")])
    );
    Ok(())
}
