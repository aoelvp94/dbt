//! Athena-specific adapter helpers, ported from dbt-athena's Python
//! `AthenaAdapter`.
//!
//! Typed only: `adapter/mod.rs` converts the Jinja arguments and dispatches
//! here on `AdapterType::Athena`. Nothing in this module touches
//! `minijinja::Value`.
//!
//! dbt-athena reaches Glue, Lake Formation and S3 through boto3. Fusion has no
//! AWS client, so each helper either answers from SQL (`SHOW CREATE TABLE`,
//! `DROP`), from the profile, or reports that the operation is not available.

use crate::AdapterResult;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::errors::{AdapterError, AdapterErrorKind};
use crate::metadata::athena::athena_string_literal;
use crate::query_ctx::node_id_from_state;
use crate::relation::athena_s3_path_table_part;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::BaseRelation;
use minijinja::State;

/// How a Glue / Lake Formation housekeeping method behaves without an AWS
/// client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlueHousekeeping {
    /// Only governance metadata would change (table versions, LF tags and
    /// grants, column docs on the Glue table). Skipped with a warning: data
    /// and catalog entries are untouched either way.
    MetadataOnly,
    /// Data or catalog entries would be removed. Refused: a drop or full
    /// refresh that skipped it would report success with the table in place.
    RemovesData,
}

/// Classify a dbt-athena adapter method that has no SQL equivalent.
///
/// AthenaAdapter: `expire_glue_table_versions` (#L947), `add_lf_tags`
/// (#L189), `add_lf_tags_to_database` (#L171), `apply_lf_grants` (#L207),
/// `persist_docs_to_glue` (#L990), `delete_from_s3` (#L503),
/// `clean_up_partitions` (#L404), `swap_table` (#L816), `drop_glue_database`
/// (#L1256), all relative to
/// https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py.
///
/// `delete_from_s3` clears a table location before CTAS. With a `*_unique`
/// `s3_data_naming` (dbt-athena's default) the location is a fresh UUID
/// prefix and the delete is a no-op; for a reused location Athena's CTAS
/// itself refuses a non-empty directory. Skipping it can fail loudly but never
/// corrupt, so it is metadata-only here.
pub fn glue_housekeeping(method: &str) -> Option<GlueHousekeeping> {
    match method {
        "expire_glue_table_versions"
        | "add_lf_tags"
        | "add_lf_tags_to_database"
        | "apply_lf_grants"
        | "persist_docs_to_glue"
        | "delete_from_s3" => Some(GlueHousekeeping::MetadataOnly),
        "clean_up_partitions" | "swap_table" | "drop_glue_database" => {
            Some(GlueHousekeeping::RemovesData)
        }
        _ => None,
    }
}

/// What dbt-athena's `get_glue_table_type` reports for an existing relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlueTableType {
    IcebergTable,
    /// A Hive (external) table.
    Table,
    View,
}

impl GlueTableType {
    /// The `TableType` value the dbt-athena macros compare against.
    pub fn as_str(self) -> &'static str {
        match self {
            GlueTableType::IcebergTable => "iceberg_table",
            GlueTableType::Table => "table",
            GlueTableType::View => "view",
        }
    }
}

/// Profile values `generate_s3_location` falls back to, read from `target`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct S3Target {
    pub s3_staging_dir: Option<String>,
    pub s3_data_dir: Option<String>,
    pub s3_data_naming: Option<String>,
    pub s3_tmp_table_dir: Option<String>,
}

/// The macro-supplied arguments of `generate_s3_location`, each overriding the
/// profile value of the same name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct S3LocationArgs {
    pub s3_data_dir: Option<String>,
    pub s3_data_naming: Option<String>,
    pub s3_tmp_table_dir: Option<String>,
    pub external_location: Option<String>,
    pub is_temporary_table: bool,
}

/// AthenaAdapter `generate_s3_location` (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L303): the S3
/// prefix a table is created under.
///
/// `external_location` wins for non-temporary tables. Otherwise the root is
/// `s3_tmp_table_dir` (temporary tables only), else `s3_data_dir`, else
/// `<s3_staging_dir>/tables`, and `s3_data_naming` decides how schema, table
/// and a UUID are appended. `unique` supplies the UUID so the layout is
/// testable.
pub fn generate_s3_location(
    relation: &dyn BaseRelation,
    target: &S3Target,
    args: &S3LocationArgs,
    unique: impl Fn() -> String,
) -> AdapterResult<String> {
    if let Some(external) = args.external_location.as_deref()
        && !args.is_temporary_table
    {
        return Ok(external.trim_end_matches('/').to_string());
    }

    let tmp_dir = args
        .s3_tmp_table_dir
        .as_deref()
        .or(target.s3_tmp_table_dir.as_deref());
    let table_prefix = match (tmp_dir, args.is_temporary_table) {
        (Some(tmp), true) => tmp.to_string(),
        _ => match args
            .s3_data_dir
            .as_deref()
            .or(target.s3_data_dir.as_deref())
        {
            Some(dir) => dir.to_string(),
            None => {
                let staging = target.s3_staging_dir.as_deref().ok_or_else(|| {
                    AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "generate_s3_location: neither s3_data_dir nor s3_staging_dir is set",
                    )
                })?;
                format!("{}/tables", staging.trim_end_matches('/'))
            }
        },
    };
    let table_prefix = table_prefix.trim_end_matches('/');

    let identifier = resolved(relation.identifier_as_resolved_str())?;
    let table_part = athena_s3_path_table_part(relation).unwrap_or(identifier);
    let schema = resolved(relation.schema_as_resolved_str())?;
    // dbt-athena defaults `s3_data_naming` to schema_table_unique.
    let naming = args
        .s3_data_naming
        .as_deref()
        .or(target.s3_data_naming.as_deref())
        .unwrap_or("schema_table_unique");

    Ok(match naming {
        "unique" => format!("{table_prefix}/{}", unique()),
        "table" => format!("{table_prefix}/{table_part}"),
        "table_unique" => format!("{table_prefix}/{table_part}/{}", unique()),
        "schema_table" => format!("{table_prefix}/{schema}/{table_part}"),
        "schema_table_unique" => format!("{table_prefix}/{schema}/{table_part}/{}", unique()),
        other => {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "generate_s3_location: unknown s3_data_naming '{other}' (expected unique, \
                     table, table_unique, schema_table or schema_table_unique)"
                ),
            ));
        }
    })
}

/// AthenaAdapter `is_work_group_output_location_enforced`
/// (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L245). Answering truthfully needs the Athena
/// `GetWorkGroup` API, which Fusion cannot call; `false` is what dbt-athena
/// returns with `skip_workgroup_check`, and it only makes the macros emit an
/// explicit table location, which is always valid.
pub fn is_work_group_output_location_enforced() -> bool {
    false
}

/// Result of [`run_query_with_partitions_limit_catching`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionsLimitOutcome {
    /// Athena refused the statement with `TOO_MANY_OPEN_PARTITIONS`; the
    /// macros fall back to batched inserts.
    TooManyOpenPartitions,
    Executed {
        rowcount: i64,
        bytes_scanned: i64,
    },
}

/// AthenaAdapter `run_query_with_partitions_limit_catching`
/// (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L1393).
pub fn run_query_with_partitions_limit_catching(
    adapter: &AdapterImpl,
    state: &State,
    sql: &str,
    token: CancellationToken,
) -> AdapterResult<PartitionsLimitOutcome> {
    match execute(adapter, state, sql, false, token) {
        Ok((rowcount, bytes_scanned)) => Ok(PartitionsLimitOutcome::Executed {
            rowcount,
            bytes_scanned,
        }),
        Err(e) if e.message().contains("TOO_MANY_OPEN_PARTITIONS") => {
            Ok(PartitionsLimitOutcome::TooManyOpenPartitions)
        }
        Err(e) => Err(e),
    }
}

/// AthenaAdapter `get_glue_table_type` (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L369), `None`
/// when the relation does not exist.
///
/// dbt-athena reads the Glue table's `table_type` parameter; without a Glue
/// client the same fact comes from `SHOW CREATE TABLE`, see
/// [`classify_show_create_table`].
pub fn get_glue_table_type(
    adapter: &AdapterImpl,
    state: &State,
    relation: &dyn BaseRelation,
    token: CancellationToken,
) -> AdapterResult<Option<GlueTableType>> {
    if relation.relation_type() == Some(RelationType::View) {
        return Ok(Some(GlueTableType::View));
    }
    let schema = resolved(relation.schema_as_resolved_str())?;
    let identifier = resolved(relation.identifier_as_resolved_str())?;

    // Existence first: `SHOW CREATE TABLE` on a missing table does not say
    // "not found". Athena falls back to its Hive parser, which rejects the
    // leading query comment with a ParseException. The macros call
    // `drop_relation` on relation objects that may not exist yet.
    let exists_sql = format!(
        "select table_type from information_schema.tables \
         where lower(table_schema) = '{}' and lower(table_name) = '{}' limit 1",
        athena_string_literal(&schema),
        athena_string_literal(&identifier)
    );
    match fetch_first_column(adapter, state, &exists_sql, true, token.clone())?.first() {
        None => return Ok(None),
        Some(table_type) if table_type.trim().eq_ignore_ascii_case("VIEW") => {
            return Ok(Some(GlueTableType::View));
        }
        Some(_) => {}
    }

    let sql = format!("show create table `{schema}`.`{identifier}`");
    let lines = fetch_first_column(adapter, state, &sql, false, token)?;
    Ok(classify_show_create_table(lines.iter().map(String::as_str)))
}

/// Classify `SHOW CREATE TABLE` output as Iceberg, Hive or view.
///
/// Iceberg DDL carries `'table_type'='ICEBERG'` in `TBLPROPERTIES` and no
/// `ROW FORMAT` / `STORED AS` clauses; Hive DDL is `CREATE EXTERNAL TABLE ...
/// ROW FORMAT ... STORED AS ...`. No single line is relied upon: the Athena
/// ADBC driver drops the first output row (it treats row 0 as a column header,
/// which is right for `SELECT` results and wrong for `SHOW`).
pub fn classify_show_create_table<'a>(
    lines: impl IntoIterator<Item = &'a str>,
) -> Option<GlueTableType> {
    let lines: Vec<String> = lines
        .into_iter()
        .map(|line| line.trim().to_ascii_uppercase())
        .filter(|line| !line.is_empty())
        .collect();
    if lines.is_empty() {
        return None;
    }
    let any = |pred: fn(&str) -> bool| lines.iter().any(|l| pred(l));
    if any(|l| l.starts_with("CREATE VIEW")) {
        Some(GlueTableType::View)
    } else if any(|l| l.contains("'TABLE_TYPE'='ICEBERG'")) {
        Some(GlueTableType::IcebergTable)
    } else if any(|l| {
        l.starts_with("CREATE EXTERNAL TABLE")
            || l.starts_with("ROW FORMAT")
            || l.starts_with("STORED AS")
            || l.starts_with("INPUTFORMAT")
    }) {
        Some(GlueTableType::Table)
    } else {
        // Iceberg DDL without the property line (older engine output).
        Some(GlueTableType::IcebergTable)
    }
}

/// AthenaAdapter `clean_up_table` (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L438), which deletes a
/// table's S3 data. For Iceberg tables and views the `DROP` that follows
/// (see [`delete_from_glue_catalog`]) removes the data too, so there is nothing
/// to do. Hive tables are refused: `DROP TABLE` on an external Hive table
/// leaves the data behind, which dbt-athena would have deleted.
pub fn clean_up_table(
    adapter: &AdapterImpl,
    state: &State,
    relation: &dyn BaseRelation,
    token: CancellationToken,
) -> AdapterResult<()> {
    match get_glue_table_type(adapter, state, relation, token)? {
        Some(GlueTableType::Table) => Err(hive_drop_unsupported("clean_up_table", relation)),
        _ => Ok(()),
    }
}

/// AthenaAdapter `delete_from_glue_catalog` (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L1223),
/// which removes the Glue entry. A SQL `DROP` reaches the same end state for
/// views and Iceberg tables (Athena's `DROP TABLE` on Iceberg deletes the data
/// files as well). Hive tables are refused, as in [`clean_up_table`].
pub fn delete_from_glue_catalog(
    adapter: &AdapterImpl,
    state: &State,
    relation: &dyn BaseRelation,
    token: CancellationToken,
) -> AdapterResult<()> {
    let schema = resolved(relation.schema_as_resolved_str())?;
    let identifier = resolved(relation.identifier_as_resolved_str())?;
    let sql = match get_glue_table_type(adapter, state, relation, token.clone())? {
        None => return Ok(()),
        Some(GlueTableType::View) => format!("drop view if exists \"{schema}\".\"{identifier}\""),
        Some(GlueTableType::IcebergTable) => {
            format!("drop table if exists `{schema}`.`{identifier}`")
        }
        Some(GlueTableType::Table) => {
            return Err(hive_drop_unsupported("delete_from_glue_catalog", relation));
        }
    };
    execute(adapter, state, &sql, false, token)?;
    Ok(())
}

/// AthenaAdapter `format_partition_keys`
/// (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L1403): the partition expressions as a comma-separated select list
/// for the distinct-partitions probe.
pub fn format_partition_keys<'a>(partition_keys: impl IntoIterator<Item = &'a str>) -> String {
    partition_keys
        .into_iter()
        .map(format_one_partition_key)
        .collect::<Vec<_>>()
        .join(", ")
}

/// AthenaAdapter `format_one_partition_key`
/// (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L1407): Iceberg hidden partitioning (`day(ts)` becomes
/// `date_trunc('day', ts)`), bucket partitioning (`bucket(col, 16)` becomes
/// `col`), else the lowercased key.
pub fn format_one_partition_key(partition_key: &str) -> String {
    let lower = partition_key.trim().to_ascii_lowercase();
    for unit in ["hour", "day", "month", "year"] {
        if let Some(rest) = lower.strip_prefix(&format!("{unit}("))
            && let Some(inner) = rest.strip_suffix(')')
        {
            return format!("date_trunc('{unit}', {inner})");
        }
    }
    if let Some(rest) = lower.strip_prefix("bucket(")
        && let Some((col, _)) = rest.split_once(',')
    {
        return col.trim().to_string();
    }
    lower
}

/// AthenaAdapter `format_value_for_partition`
/// (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L1442): the SQL literal and comparison operator for a partition
/// value, e.g. `DATE'2026-09-01'` with `=`, or `null` with ` is `.
/// `column_type` comes from `adapter.convert_type`, so Fusion's Athena
/// spellings (`varchar`, `bigint`, ...) are accepted alongside dbt-athena's.
pub fn format_value_for_partition(
    value: Option<&str>,
    column_type: &str,
) -> AdapterResult<(String, &'static str)> {
    let Some(text) = value else {
        return Ok(("null".to_string(), " is "));
    };
    let literal = match column_type.trim().to_ascii_lowercase().as_str() {
        "integer" | "bigint" | "smallint" | "tinyint" | "int" => text.to_string(),
        "string" | "varchar" | "text" => format!("'{}'", text.replace('\'', "''")),
        "date" => format!("DATE'{text}'"),
        "timestamp" => format!("TIMESTAMP'{text}'"),
        other => {
            return Err(AdapterError::new(
                AdapterErrorKind::UnsupportedType,
                format!("format_value_for_partition: unsupported column type: {other}"),
            ));
        }
    };
    Ok((literal, "="))
}

/// AthenaAdapter `murmur3_hash` (https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L1419) hashes a value for Iceberg
/// bucket partitions with MurmurHash3 over Iceberg's byte encoding (dbt-athena
/// uses `mmh3`). No crate in the workspace provides it, and adding one needs
/// permission, so bucket-partitioned batching is refused.
pub fn murmur3_hash_unsupported() -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::NotSupported,
        "adapter.murmur3_hash is not yet supported on the Athena adapter: Iceberg bucket \
         partitions need MurmurHash3, which Fusion does not ship",
    )
}

fn hive_drop_unsupported(method: &str, relation: &dyn BaseRelation) -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::NotSupported,
        format!(
            "adapter.{method}: {} is a Hive table; dropping it means deleting its S3 data \
             through the S3 API, which Fusion cannot call yet. Iceberg tables and views are \
             dropped through SQL.",
            relation.render_self_as_str()
        ),
    )
}

fn resolved(component: Result<String, minijinja::Error>) -> AdapterResult<String> {
    component.map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, e.to_string()))
}

/// Run `sql` on the node's connection; returns `(rows_affected, bytes_scanned)`.
fn execute(
    adapter: &AdapterImpl,
    state: &State,
    sql: &str,
    fetch: bool,
    token: CancellationToken,
) -> AdapterResult<(i64, i64)> {
    let mut conn = adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
    let (response, _) = adapter.execute(
        Some(state),
        conn.as_mut(),
        None,
        sql,
        false,
        fetch,
        None,
        None,
        token,
    )?;
    Ok((
        response.rows_affected_i64(),
        response.bytes_processed().unwrap_or(0),
    ))
}

/// Run `sql` and return the first result column as strings, row by row.
///
/// `query_comment: false` runs the statement without the `/* ... */` query
/// comment prefix, which the engine only adds when handed a Jinja state. For
/// Hive tables Athena executes `SHOW CREATE TABLE` through its Hive DDL
/// engine, which rejects the comment with a ParseException; Iceberg tables go
/// through Trino and accept it.
fn fetch_first_column(
    adapter: &AdapterImpl,
    state: &State,
    sql: &str,
    query_comment: bool,
    token: CancellationToken,
) -> AdapterResult<Vec<String>> {
    let mut conn = adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
    let (_, table) = adapter.execute(
        query_comment.then_some(state),
        conn.as_mut(),
        None,
        sql,
        false,
        true,
        None,
        None,
        token,
    )?;
    let batch = table.original_record_batch();
    Ok(batch
        .columns()
        .first()
        .map(|col| {
            (0..batch.num_rows())
                .filter_map(|i| arrow::util::display::array_value_to_string(col, i).ok())
                .collect()
        })
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::{ATHENA_S3_PATH_TABLE_PART, Relation};
    use dbt_adapter_core::AdapterType;
    use std::collections::BTreeMap;

    fn relation(part: Option<&str>) -> Relation {
        Relation::new(
            AdapterType::Athena,
            Some("awsdatacatalog".to_string()),
            Some("analytics".to_string()),
            Some("orders".to_string()),
        )
        .with_metadata(
            part.map(|p| BTreeMap::from([(ATHENA_S3_PATH_TABLE_PART.to_string(), p.to_string())])),
        )
    }

    fn target() -> S3Target {
        S3Target {
            s3_staging_dir: Some("s3://staging/results/".to_string()),
            s3_data_dir: Some("s3://data/".to_string()),
            s3_data_naming: None,
            s3_tmp_table_dir: Some("s3://tmp/".to_string()),
        }
    }

    fn uuid() -> String {
        "UUID".to_string()
    }

    fn location(target: &S3Target, args: S3LocationArgs) -> String {
        generate_s3_location(&relation(None), target, &args, uuid).unwrap()
    }

    fn naming(name: &str) -> S3LocationArgs {
        S3LocationArgs {
            s3_data_naming: Some(name.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn s3_location_five_naming_layouts() {
        let t = target();
        assert_eq!(location(&t, naming("unique")), "s3://data/UUID");
        assert_eq!(location(&t, naming("table")), "s3://data/orders");
        assert_eq!(
            location(&t, naming("table_unique")),
            "s3://data/orders/UUID"
        );
        assert_eq!(
            location(&t, naming("schema_table")),
            "s3://data/analytics/orders"
        );
        assert_eq!(
            location(&t, naming("schema_table_unique")),
            "s3://data/analytics/orders/UUID"
        );
        // dbt-athena's default.
        assert_eq!(
            location(&t, S3LocationArgs::default()),
            "s3://data/analytics/orders/UUID"
        );
    }

    #[test]
    fn s3_location_unknown_naming_is_an_error() {
        let err =
            generate_s3_location(&relation(None), &target(), &naming("nope"), uuid).unwrap_err();
        assert_eq!(err.kind(), AdapterErrorKind::Configuration);
        assert!(err.message().contains("nope"), "{}", err.message());
    }

    #[test]
    fn s3_location_prefix_precedence() {
        // external_location wins for non-temporary tables, trailing slash trimmed.
        let external = S3LocationArgs {
            external_location: Some("s3://ext/orders/".to_string()),
            ..naming("table")
        };
        assert_eq!(location(&target(), external.clone()), "s3://ext/orders");

        // ... but not for temporary tables, which go under s3_tmp_table_dir.
        let temporary = S3LocationArgs {
            is_temporary_table: true,
            ..external
        };
        assert_eq!(location(&target(), temporary), "s3://tmp/orders");

        // Argument overrides the profile value.
        let arg_dir = S3LocationArgs {
            s3_data_dir: Some("s3://arg/".to_string()),
            ..naming("table")
        };
        assert_eq!(location(&target(), arg_dir), "s3://arg/orders");

        // No data dir anywhere: <staging>/tables.
        let staging_only = S3Target {
            s3_data_dir: None,
            s3_tmp_table_dir: None,
            ..target()
        };
        assert_eq!(
            location(&staging_only, naming("table")),
            "s3://staging/results/tables/orders"
        );

        // Nothing to derive a prefix from.
        let empty = S3Target::default();
        let err =
            generate_s3_location(&relation(None), &empty, &naming("table"), uuid).unwrap_err();
        assert!(
            err.message().contains("s3_staging_dir"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn s3_location_uses_the_relation_s3_path_table_part() {
        let rel = relation(Some("orders__ha"));
        let got = generate_s3_location(&rel, &target(), &naming("schema_table"), uuid).unwrap();
        assert_eq!(got, "s3://data/analytics/orders__ha");
    }

    #[test]
    fn classify_show_create_table_output() {
        let iceberg = [
            "CREATE TABLE analytics.orders (",
            "  id bigint)",
            "LOCATION 's3://data/analytics/orders/UUID'",
            "TBLPROPERTIES (",
            "  'table_type'='ICEBERG',",
            "  'format'='PARQUET')",
        ];
        assert_eq!(
            classify_show_create_table(iceberg),
            Some(GlueTableType::IcebergTable)
        );

        // The driver drops row 0, so the CREATE line may be missing.
        assert_eq!(
            classify_show_create_table(iceberg[1..].iter().copied()),
            Some(GlueTableType::IcebergTable)
        );

        let hive = [
            "CREATE EXTERNAL TABLE `analytics.orders`(",
            "  `id` bigint)",
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'",
            "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'",
            "LOCATION 's3://data/analytics/orders'",
        ];
        assert_eq!(classify_show_create_table(hive), Some(GlueTableType::Table));
        assert_eq!(
            classify_show_create_table(hive[1..].iter().copied()),
            Some(GlueTableType::Table)
        );

        let view = [
            "CREATE VIEW analytics.orders_v AS",
            "SELECT id FROM analytics.orders",
        ];
        assert_eq!(classify_show_create_table(view), Some(GlueTableType::View));

        assert_eq!(classify_show_create_table(["", "  "]), None);
        assert_eq!(classify_show_create_table([]), None);
    }

    #[test]
    fn glue_housekeeping_classification() {
        for name in [
            "expire_glue_table_versions",
            "add_lf_tags",
            "add_lf_tags_to_database",
            "apply_lf_grants",
            "persist_docs_to_glue",
            "delete_from_s3",
        ] {
            assert_eq!(
                glue_housekeeping(name),
                Some(GlueHousekeeping::MetadataOnly),
                "{name}"
            );
        }
        for name in ["clean_up_partitions", "swap_table", "drop_glue_database"] {
            assert_eq!(
                glue_housekeeping(name),
                Some(GlueHousekeeping::RemovesData),
                "{name}"
            );
        }
        assert_eq!(glue_housekeeping("execute"), None);
        assert_eq!(glue_housekeeping("clean_up_table"), None);
    }
    #[test]
    fn partition_key_formatting() {
        assert_eq!(format_one_partition_key("day(ts)"), "date_trunc('day', ts)");
        assert_eq!(
            format_one_partition_key(" MONTH(Created_At) "),
            "date_trunc('month', created_at)"
        );
        assert_eq!(format_one_partition_key("bucket(user_id, 16)"), "user_id");
        assert_eq!(format_one_partition_key("Region"), "region");
        assert_eq!(
            format_partition_keys(["day(ts)", "region"]),
            "date_trunc('day', ts), region"
        );
        assert_eq!(format_partition_keys([]), "");
    }

    #[test]
    fn partition_value_literals() {
        assert_eq!(
            format_value_for_partition(None, "varchar").unwrap(),
            ("null".to_string(), " is ")
        );
        assert_eq!(
            format_value_for_partition(Some("42"), "BIGINT").unwrap(),
            ("42".to_string(), "=")
        );
        assert_eq!(
            format_value_for_partition(Some("it's"), "varchar").unwrap(),
            ("'it''s'".to_string(), "=")
        );
        assert_eq!(
            format_value_for_partition(Some("2026-09-01"), "date").unwrap(),
            ("DATE'2026-09-01'".to_string(), "=")
        );
        assert_eq!(
            format_value_for_partition(Some("2026-09-01 00:00:00"), "timestamp").unwrap(),
            ("TIMESTAMP'2026-09-01 00:00:00'".to_string(), "=")
        );
        let err = format_value_for_partition(Some("1.5"), "double").unwrap_err();
        assert_eq!(err.kind(), AdapterErrorKind::UnsupportedType);
    }
}
