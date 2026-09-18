//! AWS API side of the Athena adapter: the S3 and Glue calls dbt-athena makes
//! through boto3 (`upload_seed_to_s3`, `delete_from_s3`, `clean_up_table`,
//! `expire_glue_table_versions`). Everything Athena can do through SQL stays
//! SQL; these are the operations that have no SQL equivalent.
//!
//! Credentials come from the profile the same way the driver's do: an AWS
//! profile name, static access keys, or the default chain (instance role,
//! environment). The clients are built once per process.

use std::sync::{Arc, OnceLock};

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use dbt_auth::AdapterConfig;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};

pub struct AthenaAwsClients {
    pub s3: aws_sdk_s3::Client,
    pub glue: aws_sdk_glue::Client,
}

static CLIENTS: OnceLock<Arc<AthenaAwsClients>> = OnceLock::new();

/// Run a future to completion from a blocking adapter thread.
pub(crate) fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(_) => dbt_common::tracing::spawn_traced_block_in_place(future),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future),
    }
}

fn aws_error_msg(op: &str, detail: impl std::fmt::Display) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Driver, format!("[athena/aws] {op} failed: {detail}"))
}

fn aws_error(op: &str, e: impl std::error::Error) -> AdapterError {
    // `SdkError`'s Display is a one-word category ("dispatch failure"); the
    // context walker prints the cause chain (expired SSO token, DNS, TLS...).
    let detail = aws_smithy_types::error::display::DisplayErrorContext(&e).to_string();
    AdapterError::new(AdapterErrorKind::Driver, format!("[athena/aws] {op} failed: {detail}"))
}

/// Build (once) the S3 and Glue clients from the adapter's profile config.
pub fn clients(config: &AdapterConfig) -> AdapterResult<Arc<AthenaAwsClients>> {
    if let Some(c) = CLIENTS.get() {
        return Ok(c.clone());
    }
    let region = config.get_str("region_name").map(str::to_string).ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            "Athena profile has no region_name; S3/Glue operations need it",
        )
    })?;
    let profile = config.get_str("aws_profile_name").map(str::to_string);
    let access_key = config.get_str("aws_access_key_id").map(str::to_string);
    let secret_key = config.get_str("aws_secret_access_key").map(str::to_string);
    let session_token = config.get_str("aws_session_token").map(str::to_string);

    let sdk_config = block_on(async move {
        let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region));
        if let Some(profile) = profile {
            loader = loader.profile_name(profile);
        }
        if let (Some(key), Some(secret)) = (access_key, secret_key) {
            loader = loader.credentials_provider(Credentials::new(
                key,
                secret,
                session_token,
                None,
                "dbt-athena-profile",
            ));
        }
        loader.load().await
    });
    let built = Arc::new(AthenaAwsClients {
        s3: aws_sdk_s3::Client::new(&sdk_config),
        glue: aws_sdk_glue::Client::new(&sdk_config),
    });
    Ok(CLIENTS.get_or_init(|| built).clone())
}

/// `s3://bucket/some/prefix` -> `("bucket", "some/prefix")`.
pub fn parse_s3_path(path: &str) -> AdapterResult<(String, String)> {
    let rest = path.strip_prefix("s3://").ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            format!("not an s3:// path: {path}"),
        )
    })?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!("s3 path has no bucket: {path}"),
        ));
    }
    Ok((bucket.to_string(), prefix.trim_start_matches('/').to_string()))
}

/// Upload `body` to `s3://bucket/key`.
pub fn put_object(clients: &Arc<AthenaAwsClients>, bucket: &str, key: &str, body: Vec<u8>) -> AdapterResult<()> {
    let s3 = clients.s3.clone();
    let (bucket, key) = (bucket.to_string(), key.to_string());
    block_on(async move {
        s3.put_object()
            .bucket(&bucket)
            .key(&key)
            .body(body.into())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| aws_error(&format!("PutObject s3://{bucket}/{key}"), e))
    })
}

/// Delete every object under `s3://bucket/prefix`. Returns the number deleted;
/// a prefix with no objects is not an error (dbt-athena logs and moves on).
pub fn delete_prefix(clients: &Arc<AthenaAwsClients>, bucket: &str, prefix: &str) -> AdapterResult<usize> {
    let s3 = clients.s3.clone();
    let (bucket, prefix) = (bucket.to_string(), prefix.to_string());
    block_on(async move {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};
        let mut deleted = 0usize;
        let mut token: Option<String> = None;
        loop {
            let mut req = s3.list_objects_v2().bucket(&bucket).prefix(&prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let page = req
                .send()
                .await
                .map_err(|e| aws_error(&format!("ListObjectsV2 s3://{bucket}/{prefix}"), e))?;
            let keys: Vec<ObjectIdentifier> = page
                .contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string))
                .filter_map(|k| ObjectIdentifier::builder().key(k).build().ok())
                .collect();
            for chunk in keys.chunks(1000) {
                let delete = Delete::builder()
                    .set_objects(Some(chunk.to_vec()))
                    .quiet(true)
                    .build()
                    .map_err(|e| aws_error("DeleteObjects request", e))?;
                let out = s3
                    .delete_objects()
                    .bucket(&bucket)
                    .delete(delete)
                    .send()
                    .await
                    .map_err(|e| aws_error(&format!("DeleteObjects s3://{bucket}/{prefix}"), e))?;
                if !out.errors().is_empty() {
                    let first = &out.errors()[0];
                    return Err(aws_error_msg(
                        "DeleteObjects",
                        format!(
                            "{} object(s) failed, first: key={:?} code={:?} message={:?}",
                            out.errors().len(),
                            first.key(),
                            first.code(),
                            first.message()
                        ),
                    ));
                }
                deleted += chunk.len();
            }
            token = page.next_continuation_token().map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        Ok(deleted)
    })
}

/// dbt-athena `expire_glue_table_versions`: keep the newest
/// `versions_to_keep` Glue table versions, delete the rest, and when
/// `delete_s3` is set also delete the S3 data of expired versions whose
/// location differs from the current one (the old `__ha` / replaced
/// locations). Returns `(versions_deleted, s3_objects_deleted)`.
pub fn expire_table_versions(
    clients: &Arc<AthenaAwsClients>,
    database: &str,
    table: &str,
    versions_to_keep: usize,
    delete_s3: bool,
) -> AdapterResult<(usize, usize)> {
    let glue = clients.glue.clone();
    let (database, table) = (database.to_string(), table.to_string());
    let (database_v, table_v) = (database.clone(), table.clone());
    let versions: Vec<(i64, Option<String>)> = block_on(async move {
        let (database, table) = (database_v, table_v);
        let mut all = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = glue.get_table_versions().database_name(&database).table_name(&table);
            if let Some(t) = &token {
                req = req.next_token(t);
            }
            let page = req
                .send()
                .await
                .map_err(|e| aws_error(&format!("GetTableVersions {database}.{table}"), e))?;
            for v in page.table_versions() {
                let id: i64 = v.version_id().and_then(|s| s.parse().ok()).unwrap_or(0);
                let location = v
                    .table()
                    .and_then(|t| t.storage_descriptor())
                    .and_then(|sd| sd.location())
                    .map(str::to_string);
                all.push((id, location));
            }
            token = page.next_token().map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        Ok::<_, AdapterError>(all)
    })?;

    let mut sorted = versions;
    sorted.sort_by(|a, b| b.0.cmp(&a.0));
    let current_location = sorted.first().and_then(|v| v.1.clone());
    let expired: Vec<(i64, Option<String>)> = sorted.into_iter().skip(versions_to_keep.max(1)).collect();
    if expired.is_empty() {
        return Ok((0, 0));
    }

    let glue = clients.glue.clone();
    let (database_c, table_c) = (database.to_string(), table.to_string());
    let ids: Vec<String> = expired.iter().map(|v| v.0.to_string()).collect();
    block_on(async move {
        for chunk in ids.chunks(100) {
            let out = glue
                .batch_delete_table_version()
                .database_name(&database_c)
                .table_name(&table_c)
                .set_version_ids(Some(chunk.to_vec()))
                .send()
                .await
                .map_err(|e| aws_error(&format!("BatchDeleteTableVersion {database_c}.{table_c}"), e))?;
            if !out.errors().is_empty() {
                return Err(aws_error_msg(
                    "BatchDeleteTableVersion",
                    format!("{} version(s) failed to delete", out.errors().len()),
                ));
            }
        }
        Ok::<_, AdapterError>(())
    })?;

    let deleted_versions = expired.len();
    let mut objects_deleted = 0usize;
    if delete_s3 {
        let mut seen = std::collections::BTreeSet::new();
        for (_, location) in expired {
            let Some(location) = location else { continue };
            if Some(&location) == current_location.as_ref() || !seen.insert(location.clone()) {
                continue;
            }
            let (bucket, prefix) = parse_s3_path(&location)?;
            objects_deleted += delete_prefix(clients, &bucket, &prefix)?;
        }
    }
    Ok((deleted_versions, objects_deleted))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_paths() {
        assert_eq!(
            parse_s3_path("s3://bucket/a/b/").unwrap(),
            ("bucket".to_string(), "a/b/".to_string())
        );
        assert_eq!(parse_s3_path("s3://bucket").unwrap(), ("bucket".to_string(), String::new()));
        assert!(parse_s3_path("gs://x/y").is_err());
    }
}

/// One `information_schema`-shaped catalog row (one per column), as dbt's
/// `get_catalog_relations` contract expects.
#[derive(Debug, Clone)]
/// One Glue table as the relation lookups need it. `kind` follows
/// dbt-athena's `TableType` values: `table`, `view` or `iceberg_table`.
pub struct GlueTableInfo {
    pub name: String,
    pub kind: &'static str,
    pub location: Option<String>,
    /// Regular columns followed by partition keys, as `(name, glue type)`.
    pub columns: Vec<(String, String)>,
}

fn glue_table_info(t: &aws_sdk_glue::types::Table) -> GlueTableInfo {
    let kind = if t.table_type() == Some("VIRTUAL_VIEW") {
        "view"
    } else if t
        .parameters()
        .and_then(|p| p.get("table_type"))
        .is_some_and(|v| v.eq_ignore_ascii_case("ICEBERG"))
    {
        "iceberg_table"
    } else {
        "table"
    };
    let regular = t
        .storage_descriptor()
        .map(|sd| sd.columns().to_vec())
        .unwrap_or_default();
    let columns = regular
        .iter()
        .chain(t.partition_keys().iter())
        .map(|c| (c.name().to_string(), c.r#type().unwrap_or("").to_string()))
        .collect();
    GlueTableInfo {
        name: t.name().to_string(),
        kind,
        location: t
            .storage_descriptor()
            .and_then(|sd| sd.location())
            .map(str::to_string),
        columns,
    }
}

/// Glue `GetTable`: `None` when the table (or its database) does not exist.
/// Names are lowercased first, as Athena folds identifiers and Glue stores
/// them that way.
pub fn glue_get_table(
    clients: &Arc<AthenaAwsClients>,
    database: &str,
    name: &str,
) -> AdapterResult<Option<GlueTableInfo>> {
    let glue = clients.glue.clone();
    let (database, name) = (database.to_lowercase(), name.to_lowercase());
    block_on(async move {
        match glue.get_table().database_name(&database).name(&name).send().await {
            Ok(out) => Ok(out.table().map(glue_table_info)),
            Err(e) if e.to_string().contains("EntityNotFoundException") => Ok(None),
            Err(e) => Err(aws_error(&format!("GetTable {database}.{name}"), e)),
        }
    })
}

/// Glue `GetTables` for one database, paginated. A missing database yields
/// an empty list, which is what relation-cache hydration wants for schemas
/// that are not created yet.
pub fn glue_list_tables(
    clients: &Arc<AthenaAwsClients>,
    database: &str,
) -> AdapterResult<Vec<GlueTableInfo>> {
    let glue = clients.glue.clone();
    let database = database.to_lowercase();
    block_on(async move {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = glue.get_tables().database_name(&database).max_results(100);
            if let Some(t) = &token {
                req = req.next_token(t);
            }
            let page = match req.send().await {
                Ok(p) => p,
                Err(e) if e.to_string().contains("EntityNotFoundException") => break,
                Err(e) => return Err(aws_error(&format!("GetTables {database}"), e)),
            };
            out.extend(page.table_list().iter().map(glue_table_info));
            token = page.next_token().map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        Ok(out)
    })
}

pub struct GlueCatalogRow {
    pub table_schema: String,
    pub table_name: String,
    pub table_type: String,
    pub table_comment: Option<String>,
    pub table_owner: Option<String>,
    pub column_name: String,
    pub column_index: i64,
    pub column_type: String,
    pub column_comment: Option<String>,
}

/// dbt-athena `get_catalog_by_relations`: list a Glue database's tables
/// (optionally only `tables`, compared lowercase) and flatten them into one
/// row per column, regular columns first and partition keys after, the way
/// Athena's `information_schema.columns` orders them. Unlike a schema-wide
/// `information_schema` query this never touches table data, so a table with
/// unreadable Iceberg metadata cannot fail the whole schema.
pub fn glue_catalog_rows(
    clients: &Arc<AthenaAwsClients>,
    database: &str,
    tables: Option<&[String]>,
) -> AdapterResult<Vec<GlueCatalogRow>> {
    let glue = clients.glue.clone();
    let database = database.to_string();
    let wanted: Option<std::collections::HashSet<String>> =
        tables.map(|ts| ts.iter().map(|t| t.to_lowercase()).collect());
    block_on(async move {
        let mut rows = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = glue.get_tables().database_name(&database).max_results(100);
            if let Some(t) = &token {
                req = req.next_token(t);
            }
            let page = match req.send().await {
                Ok(p) => p,
                // A schema that does not exist yields no catalog rows, not an error
                // (dbt hydrates the catalog for target schemas not yet created).
                Err(e) if e.to_string().contains("EntityNotFoundException") => break,
                Err(e) => return Err(aws_error(&format!("GetTables {database}"), e)),
            };
            for t in page.table_list() {
                let name = t.name().to_string();
                if let Some(w) = &wanted
                    && !w.contains(&name.to_lowercase())
                {
                    continue;
                }
                let table_type = match t.table_type() {
                    Some("VIRTUAL_VIEW") => "VIEW",
                    _ => "BASE TABLE",
                }
                .to_string();
                let regular = t
                    .storage_descriptor()
                    .map(|sd| sd.columns().to_vec())
                    .unwrap_or_default();
                let mut index = 0i64;
                for c in regular.iter().chain(t.partition_keys().iter()) {
                    index += 1;
                    rows.push(GlueCatalogRow {
                        table_schema: database.clone(),
                        table_name: name.clone(),
                        table_type: table_type.clone(),
                        table_comment: t.description().map(str::to_string),
                        table_owner: t.owner().map(str::to_string),
                        column_name: c.name().to_string(),
                        column_index: index,
                        column_type: c.r#type().unwrap_or("").to_string(),
                        column_comment: c.comment().map(str::to_string),
                    });
                }
            }
            token = page.next_token().map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        Ok(rows)
    })
}
