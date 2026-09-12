//! Feldera table contract, PostgreSQL table names, and routing.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Mutex;

use crate::config::env_or;
use crate::decode::{ColumnMetadata, RelationMetadata, Row, quote_ident};

pub(crate) fn refuse_unusable_identity(
    contract: &TargetContract,
    schema: &str,
    table: &str,
    columns: &[ColumnMetadata],
) -> Result<(), Box<dyn std::error::Error>> {
    if contract.primary_key.is_empty() {
        return Ok(());
    }

    if columns.iter().any(|column| column.is_key) {
        return Ok(());
    }

    Err(format!(
        "{schema}.{table} has REPLICA IDENTITY NOTHING (or DEFAULT with no primary key), so \
         PostgreSQL will not log a pre-image for UPDATE or DELETE. Feldera table {} is keyed \
         by ({}), and a delete without that key is ignored without an error. ALTER TABLE \
         {schema}.{table} REPLICA IDENTITY FULL; or DEFAULT on a primary key that covers \
         Feldera's key.",
        contract.table.name,
        identifier_list(&contract.primary_key),
    )
    .into())
}
// ============================================================
// TARGET CONTRACT
// ============================================================

/// A Feldera SQL identifier in the canonical form the compiler stores it in.
///
/// Unquoted names are folded to upper case by Feldera and to lower case by
/// PostgreSQL, so the two systems never spell the same unquoted identifier the
/// same way and comparing them has to ignore case. Quoted names are kept
/// verbatim on both sides and have to match exactly.
#[derive(Debug, Clone)]
pub(crate) struct SqlIdentifier {
    pub(crate) name: String,
    pub(crate) case_sensitive: bool,
}

impl SqlIdentifier {
    pub(crate) fn from_json(value: &serde_json::Value) -> Option<Self> {
        Some(Self {
            name: value.get("name")?.as_str()?.to_string(),
            case_sensitive: value
                .get("case_sensitive")
                .and_then(|flag| flag.as_bool())
                .unwrap_or(false),
        })
    }

    pub(crate) fn matches(&self, postgres_name: &str) -> bool {
        if self.case_sensitive {
            self.name == postgres_name
        } else {
            self.name.eq_ignore_ascii_case(postgres_name)
        }
    }
}

pub(crate) fn identifier_list(identifiers: &[SqlIdentifier]) -> String {
    identifiers
        .iter()
        .map(|identifier| identifier.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Stable JSON for a key, so the same row hashed from an insert and from a
/// query lands on the same set entry even when object key order differs.
pub(crate) fn canonical_json(value: &serde_json::Value) -> String {
    canonical_value(value).to_string()
}

pub(crate) fn canonical_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(fields) => {
            let ordered: BTreeMap<String, serde_json::Value> = fields
                .iter()
                .map(|(name, nested)| (name.clone(), canonical_value(nested)))
                .collect();

            serde_json::Value::Object(ordered.into_iter().collect())
        }

        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonical_value).collect())
        }

        other => other.clone(),
    }
}

/// The part of a row a Feldera delete has to name.
///
/// Field names are taken from the contract rather than from whichever document
/// supplied the values, so an insert that used PostgreSQL's `id` and a query
/// that used Feldera's `ID` collapse to the same key.
pub(crate) fn retract_key(row: &serde_json::Value, contract: &TargetContract) -> String {
    let Some(fields) = row.as_object() else {
        return canonical_json(row);
    };

    if contract.primary_key.is_empty() {
        return canonical_json(row);
    }

    let mut projected = serde_json::Map::new();

    for key in &contract.primary_key {
        if let Some((_, value)) = fields.iter().find(|(name, _)| key.matches(name)) {
            projected.insert(key.name.clone(), value.clone());
        }
    }

    canonical_json(&serde_json::Value::Object(projected))
}

/// Which configured tables a TRUNCATE names.
///
/// A missing RELATION is not a skip. Skipping a TRUNCATE because the cache is
/// empty is how the destination keeps rows the source just erased, and then
/// the COMMIT advances the slot so the miss is never replayed. One unknown id
/// with one configured table still retracts that table. Several configured
/// tables and no cache is ambiguous, so every table is retracted rather than
/// guessed.
pub(crate) fn truncate_route_indices(
    routes: &[TableRoute],
    relations: &HashMap<u32, RelationMetadata>,
    rel_ids: &[u32],
) -> Vec<usize> {
    let mut hit = BTreeSet::new();
    let mut unknown = false;

    for rel_id in rel_ids {
        match relations.get(rel_id) {
            Some(relation) => {
                if let Ok(index) = route_index_for(routes, relation) {
                    hit.insert(index);
                }
            }

            None => unknown = true,
        }
    }

    if !hit.is_empty() {
        return hit.into_iter().collect();
    }

    if unknown || rel_ids.is_empty() {
        eprintln!(
            "WARNING: TRUNCATE named relation IDs {rel_ids:?} with no cached RELATION that \
             matches TABLES; retracting every table this connector writes"
        );

        return (0..routes.len()).collect();
    }

    Vec::new()
}

/// What Feldera promises about the destination table.
///
/// Redelivering a transaction is only harmless because Feldera can recognise a
/// row it already holds and overwrite it instead of counting it twice. That
/// recognition is the PRIMARY KEY, and it is declared in somebody else's SQL,
/// in another process, which is exactly why it gets checked rather than
/// assumed.
#[derive(Debug)]
pub(crate) struct TargetContract {
    pub(crate) table: SqlIdentifier,
    pub(crate) primary_key: Vec<SqlIdentifier>,
    /// Every column Feldera declared. Used when there is no primary key: a
    /// retraction then has to name the whole row, because Z-set delete matches
    /// on the record, not on a key.
    pub(crate) fields: Vec<SqlIdentifier>,
}

/// One PostgreSQL table and the Feldera ingress it writes to.
///
/// The pair is the routing key. Feldera's HTTP API is one URL per table, so a
/// row that arrives for the wrong member of this list is not "close enough" —
/// it would be inserted under another table's name and never found again.
pub(crate) struct TableRoute {
    pub(crate) source: TableName,
    pub(crate) ingress_url: String,
    pub(crate) contract: TargetContract,
    pub(crate) session_keys: Mutex<BTreeSet<String>>,
}

impl TableRoute {
    pub(crate) fn matches_source(&self, schema: &str, table: &str) -> bool {
        self.source.matches(schema, table)
    }

    pub(crate) fn lock_session_keys(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        self.session_keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn remember_durable_events(&self, events: &[serde_json::Value]) {
        let mut keys = self.lock_session_keys();

        for event in events {
            if let Some(row) = event.get("insert") {
                keys.insert(retract_key(row, &self.contract));
            } else if let Some(row) = event.get("delete") {
                keys.remove(&retract_key(row, &self.contract));
            }
        }
    }

    pub(crate) fn session_delete_events(&self) -> Vec<serde_json::Value> {
        self.lock_session_keys()
            .iter()
            .filter_map(|key| serde_json::from_str::<serde_json::Value>(key).ok())
            .map(|row| serde_json::json!({ "delete": row }))
            .collect()
    }

    pub(crate) fn merge_retract_events(
        &self,
        queried: Vec<serde_json::Value>,
    ) -> Vec<serde_json::Value> {
        let mut seen = BTreeSet::new();
        let mut merged = Vec::new();

        for event in queried.into_iter().chain(self.session_delete_events()) {
            let Some(row) = event.get("delete") else {
                continue;
            };

            if seen.insert(retract_key(row, &self.contract)) {
                merged.push(event);
            }
        }

        merged
    }
}

/// Which configured route a RELATION belongs to.
///
/// Refusing an unknown table is the same rule as before, now over a list:
/// there is no ingress that can honestly accept those rows, so guessing one
/// files them under the wrong name.
pub(crate) fn route_index_for(
    routes: &[TableRoute],
    relation: &RelationMetadata,
) -> Result<usize, Box<dyn std::error::Error>> {
    if let Some((index, _)) = routes
        .iter()
        .enumerate()
        .find(|(_, route)| route.matches_source(&relation.schema, &relation.table))
    {
        return Ok(index);
    }

    let known = routes
        .iter()
        .map(|route| route.source.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    Err(format!(
        "Replication delivered rows for {}.{}, which is not one of this connector's tables \
         ({known}). Every row from the extra table would be filed under the wrong name. \
         Restrict the publication, or add the table to TABLES.",
        relation.schema, relation.table
    )
    .into())
}

/// Report whether PostgreSQL's replica identity covers Feldera's key.
///
/// Feldera's key has to be a subset of the replica identity, not equal to it:
/// REPLICA IDENTITY FULL logs every column and so satisfies any key, and what
/// breaks is only a key column PostgreSQL never sends.
///
/// A gap is reported rather than refused because the replica identity governs
/// nothing but the pre-image of an UPDATE or DELETE. Inserts are unaffected, so
/// an append-only table is legitimately usable with no identity at all, and
/// refusing here would also wedge the connector permanently on WAL recorded
/// before the identity was widened — history cannot be rewritten to satisfy a
/// check. The condition is enforced in [`check_delete_carries_key`], at the one
/// point where it actually costs correctness.
pub(crate) fn report_identity_coverage(contract: &TargetContract, relation: &RelationMetadata) {
    let identity: Vec<&str> = relation
        .columns
        .iter()
        .filter(|column| column.is_key)
        .map(|column| column.name.as_str())
        .collect();

    let missing: Vec<&str> = contract
        .primary_key
        .iter()
        .filter(|key| !identity.iter().any(|column| key.matches(column)))
        .map(|key| key.name.as_str())
        .collect();

    if missing.is_empty() {
        println!(
            "  Replica identity ({}) covers Feldera's key, so redelivery is idempotent",
            identity.join(", ")
        );

        return;
    }

    let covered = if identity.is_empty() {
        "nothing".to_string()
    } else {
        identity.join(", ")
    };

    eprintln!(
        "WARNING: the REPLICA IDENTITY of {}.{} covers {}, which omits ({}) from Feldera's key. \
         Inserts are unaffected, but any UPDATE or DELETE will be refused rather than sent \
         unkeyed. Fix it with: ALTER TABLE {}.{} REPLICA IDENTITY FULL; or with an identity \
         index that covers Feldera's primary key.",
        relation.schema,
        relation.table,
        covered,
        missing.join(", "),
        relation.schema,
        relation.table,
    );
}

/// Refuse to emit a deletion Feldera cannot key.
///
/// This is the failure worth spending a hard error on. An unkeyed delete is not
/// rejected by Feldera: it looks up a key it was never given, matches nothing,
/// and ignores the event by design. The row survives, no error is raised on
/// either side, and the divergence is permanent and silent.
pub(crate) fn check_delete_carries_key(
    contract: &TargetContract,
    row: &Row,
) -> Result<(), Box<dyn std::error::Error>> {
    let missing: Vec<&str> = contract
        .primary_key
        .iter()
        .filter(|key| !row.values.iter().any(|(column, _)| key.matches(column)))
        .map(|key| key.name.as_str())
        .collect();

    if !missing.is_empty() {
        return Err(format!(
            "Cannot retract a row from Feldera table {}: it is keyed by ({}) and the pre-image \
             PostgreSQL logged carries only ({}), so ({}) is missing. Sending this anyway would \
             match nothing and be ignored without an error, leaving the row in place for good. \
             Widen the table's REPLICA IDENTITY.",
            contract.table.name,
            identifier_list(&contract.primary_key),
            row.values
                .iter()
                .map(|(column, _)| column.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            missing.join(", "),
        )
        .into());
    }

    Ok(())
}
pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// The PostgreSQL table this connector reads.
///
/// Separate from the Feldera table name on purpose. The two are spelled
/// differently by default — PostgreSQL folds unquoted names down, Feldera folds
/// them up — and they are not required to agree at all, so conflating them
/// would make a renamed destination impossible to configure.
#[derive(Debug, Clone)]
pub(crate) struct TableName {
    pub(crate) schema: String,
    pub(crate) table: String,
}

impl TableName {
    /// Accepts `table` or `schema.table`. Deliberately not a general parser: a
    /// name containing a dot or a quote has to be spelled out with PG_SCHEMA
    /// rather than guessed at here.
    pub(crate) fn parse(raw: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let (schema, table) = match raw.split_once('.') {
            Some((schema, table)) => (schema.to_string(), table.to_string()),
            None => (env_or("PG_SCHEMA", "public"), raw.to_string()),
        };

        if schema.is_empty() || table.is_empty() {
            return Err(format!("{raw:?} is not a usable table name").into());
        }

        Ok(Self { schema, table })
    }

    pub(crate) fn matches(&self, schema: &str, table: &str) -> bool {
        self.schema.eq_ignore_ascii_case(schema) && self.table.eq_ignore_ascii_case(table)
    }

    pub(crate) fn quoted(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.table)
    }
}

/// The tables this connector moves, in configure order.
///
/// `TABLES=users,items` is the common case: PostgreSQL and Feldera spell the
/// same word, and each side folds it their own way. `users:USERS` or
/// `public.items:LINEITEMS` is the override when the names actually differ.
///
/// `FELDERA_TABLE` / `PG_TABLE` still mean one table, so every earlier harness
/// keeps working.
pub(crate) fn parse_table_pairs() -> Result<Vec<(TableName, String)>, Box<dyn std::error::Error>> {
    if let Ok(raw) = std::env::var("TABLES") {
        let raw = raw.trim();

        if !raw.is_empty() {
            return parse_tables_list(raw);
        }
    }

    let feldera = env_or("FELDERA_TABLE", "users");
    let source = TableName::parse(&env_or("PG_TABLE", &feldera))?;

    Ok(vec![(source, feldera)])
}

pub(crate) fn parse_tables_list(
    raw: &str,
) -> Result<Vec<(TableName, String)>, Box<dyn std::error::Error>> {
    let mut pairs = Vec::new();
    let mut seen_source = BTreeSet::new();
    let mut seen_feldera = BTreeSet::new();

    for part in raw.split(',') {
        let part = part.trim();

        if part.is_empty() {
            continue;
        }

        let (source_raw, feldera) = match part.split_once(':') {
            Some((source, feldera)) => (source.trim(), feldera.trim().to_string()),
            None => (part, part.to_string()),
        };

        if feldera.is_empty() {
            return Err(format!("TABLES entry {part:?} names no Feldera table").into());
        }

        let source = TableName::parse(source_raw)?;
        let source_key = format!(
            "{}.*{}",
            source.schema.to_ascii_lowercase(),
            source.table.to_ascii_lowercase()
        );
        let feldera_key = feldera.to_ascii_lowercase();

        if !seen_source.insert(source_key) {
            return Err(format!("TABLES names PostgreSQL table {source} more than once").into());
        }

        if !seen_feldera.insert(feldera_key) {
            return Err(format!("TABLES names Feldera table {feldera:?} more than once").into());
        }

        pairs.push((source, feldera));
    }

    if pairs.is_empty() {
        return Err("TABLES is set but names no tables".into());
    }

    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_list_splits_and_defaults_the_schema() {
        let pairs = parse_tables_list("users,items").unwrap();

        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0.table, "users");
        assert_eq!(pairs[0].1, "users");
        assert_eq!(pairs[1].0.table, "items");
        assert_eq!(pairs[1].1, "items");
    }

    #[test]
    fn tables_list_renames_when_the_sides_differ() {
        let pairs = parse_tables_list("public.items:LINEITEMS").unwrap();

        assert_eq!(pairs[0].0.schema, "public");
        assert_eq!(pairs[0].0.table, "items");
        assert_eq!(pairs[0].1, "LINEITEMS");
    }

    #[test]
    fn tables_list_rejects_a_duplicate_source() {
        assert!(parse_tables_list("users,USERS").is_err());
    }

    #[test]
    fn retract_key_uses_the_contract_spelling() {
        let contract = TargetContract {
            table: SqlIdentifier {
                name: "USERS".into(),
                case_sensitive: false,
            },
            primary_key: vec![SqlIdentifier {
                name: "ID".into(),
                case_sensitive: false,
            }],
            fields: Vec::new(),
        };

        let from_insert = retract_key(
            &serde_json::json!({"id": 9050, "name": "SYSTEM"}),
            &contract,
        );
        let from_query = retract_key(&serde_json::json!({"ID": 9050}), &contract);

        assert_eq!(from_insert, from_query);
        assert_eq!(from_insert, "{\"ID\":9050}");
    }
}
