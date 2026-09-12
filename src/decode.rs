//! Decode PostgreSQL text values from COPY and pgoutput into one `Value`.

use postgres_replication::protocol::TupleData;

#[derive(Debug)]
pub(crate) struct RelationMetadata {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) columns: Vec<ColumnMetadata>,
}

#[derive(Debug)]
pub(crate) struct ColumnMetadata {
    pub(crate) name: String,
    pub(crate) type_id: i32,
    /// Set when the column belongs to the table's replica identity.
    pub(crate) is_key: bool,
}

/// A decoded cell, in a form that no longer depends on how it arrived.
///
/// `Unchanged` is the hole pgoutput leaves when a TOASTed column was not
/// rewritten by an UPDATE; it has to be filled from the old image before
/// anything is sent.
#[derive(Debug, Clone)]
pub(crate) enum Value {
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Numeric(String),
    Text(String),
    Date(String),
    Time(String),
    Timestamp(String),
    Timestamptz(String),
    Uuid(String),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Null,
    Unchanged,
}

#[derive(Debug)]
pub(crate) struct Row {
    pub(crate) values: Vec<(String, Value)>,
}

/// What PostgreSQL logged about a row as it was before an UPDATE or DELETE.
#[derive(Debug)]
pub(crate) enum PreviousRow {
    /// Every column of the previous row, sent under REPLICA IDENTITY FULL.
    Full(Row),
    /// Only the replica identity columns.
    Key(Row),
    /// Nothing was logged, which means the replica identity did not change.
    Absent,
}

// ============================================================
// ROW DECODING
// ============================================================

// Built-in type OIDs; anything else is refused below.
const BOOLOID: i32 = 16;
const BYTEAOID: i32 = 17;
const INT8OID: i32 = 20;
const INT2OID: i32 = 21;
const INT4OID: i32 = 23;
const TEXTOID: i32 = 25;
const JSONOID: i32 = 114;
const FLOAT4OID: i32 = 700;
const FLOAT8OID: i32 = 701;
const BPCHAROID: i32 = 1042;
const VARCHAROID: i32 = 1043;
const DATEOID: i32 = 1082;
const TIMEOID: i32 = 1083;
const TIMESTAMPOID: i32 = 1114;
const TIMESTAMPTZOID: i32 = 1184;
const NUMERICOID: i32 = 1700;
const UUIDOID: i32 = 2950;
const JSONBOID: i32 = 3802;
const NAMEOID: i32 = 19;

pub(crate) fn decode_text_value(
    column: &ColumnMetadata,
    index: usize,
    text: Option<&[u8]>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let Some(bytes) = text else {
        return Ok(Value::Null);
    };

    let decoded_value = match column.type_id {
        BOOLOID => Value::Bool(parse_bool(bytes)?),

        INT2OID => Value::Int16(parse_utf8(bytes)?.parse::<i16>()?),
        INT4OID => Value::Int32(parse_utf8(bytes)?.parse::<i32>()?),
        INT8OID => Value::Int64(parse_utf8(bytes)?.parse::<i64>()?),

        FLOAT4OID => match parse_float(bytes)? {
            Some(value) => Value::Float32(value as f32),
            None => Value::Null,
        },
        FLOAT8OID => match parse_float(bytes)? {
            Some(value) => Value::Float64(value),
            None => Value::Null,
        },

        NUMERICOID => match parse_utf8(bytes)? {
            "NaN" | "Infinity" | "-Infinity" => Value::Null,
            digits => Value::Numeric(digits.to_string()),
        },

        TEXTOID | VARCHAROID | BPCHAROID | NAMEOID => Value::Text(parse_utf8(bytes)?.to_string()),

        DATEOID => Value::Date(parse_utf8(bytes)?.to_string()),
        TIMEOID => Value::Time(parse_utf8(bytes)?.to_string()),
        TIMESTAMPOID => Value::Timestamp(parse_utf8(bytes)?.to_string()),
        TIMESTAMPTZOID => Value::Timestamptz(timestamptz_to_utc(parse_utf8(bytes)?)?),

        UUIDOID => Value::Uuid(parse_utf8(bytes)?.to_string()),

        JSONOID | JSONBOID => Value::Json(serde_json::from_slice(bytes)?),

        BYTEAOID => Value::Bytes(parse_bytea(bytes)?),

        _ => {
            return Err(format!(
                "Column {} ({}): unsupported PostgreSQL type {}",
                index, column.name, column.type_id
            )
            .into());
        }
    };

    Ok(decoded_value)
}

pub(crate) fn parse_utf8(bytes: &[u8]) -> Result<&str, Box<dyn std::error::Error>> {
    Ok(std::str::from_utf8(bytes)?)
}

// boolout prints t/f; the true/false spellings are accepted too.
pub(crate) fn parse_bool(bytes: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
    match bytes {
        b"t" | b"true" | b"TRUE" => Ok(true),
        b"f" | b"false" | b"FALSE" => Ok(false),
        other => Err(format!(
            "not a boolean: {:?}",
            std::str::from_utf8(other).unwrap_or("\\x??")
        )
        .into()),
    }
}

// JSON has no NaN or infinity; Feldera maps them to null, and so do we.
pub(crate) fn parse_float(bytes: &[u8]) -> Result<Option<f64>, Box<dyn std::error::Error>> {
    match parse_utf8(bytes)? {
        "NaN" | "Infinity" | "-Infinity" => Ok(None),
        text => Ok(Some(text.parse::<f64>()?)),
    }
}

// PostgreSQL's hex bytea format: \x followed by hex digits.
pub(crate) fn parse_bytea(bytes: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let text = parse_utf8(bytes)?;

    let hex = text
        .strip_prefix("\\x")
        .ok_or_else(|| format!("bytea is not in hex format (bytea_output=hex): {text:?}"))?;

    if hex.len() % 2 != 0 {
        return Err(format!("bytea hex has an odd number of digits: {text:?}").into());
    }

    let mut decoded = Vec::with_capacity(hex.len() / 2);

    for pair in hex.as_bytes().chunks_exact(2) {
        let digit = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(format!("bytea hex is not hexadecimal: {text:?}")),
        };

        decoded.push((digit(pair[0])? << 4) | digit(pair[1])?);
    }

    Ok(decoded)
}

// Shift a timestamptz text value to UTC so the snapshot connection and the
// walsender agree even when their sessions use different TimeZone settings.
pub(crate) fn timestamptz_to_utc(text: &str) -> Result<String, Box<dyn std::error::Error>> {
    let (datetime, offset) = split_timestamptz(text)?;
    let (date, time) = datetime
        .split_once(' ')
        .ok_or_else(|| format!("timestamptz is missing a date/time split: {text:?}"))?;

    let mut date_parts = date.split('-');
    let year: i32 = date_parts.next().ok_or("timestamptz: no year")?.parse()?;
    let month: u32 = date_parts.next().ok_or("timestamptz: no month")?.parse()?;
    let day: u32 = date_parts.next().ok_or("timestamptz: no day")?.parse()?;

    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, fraction),
        None => (time, ""),
    };

    let mut clock_parts = clock.split(':');
    let hour: i64 = clock_parts.next().ok_or("timestamptz: no hour")?.parse()?;
    let minute: i64 = clock_parts
        .next()
        .ok_or("timestamptz: no minute")?
        .parse()?;
    let second: i64 = clock_parts
        .next()
        .ok_or("timestamptz: no second")?
        .parse()?;

    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset;

    let utc_days = seconds.div_euclid(86_400);
    let utc_secs = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(utc_days);
    let hour = utc_secs / 3_600;
    let minute = (utc_secs % 3_600) / 60;
    let second = utc_secs % 60;

    if fraction.is_empty() {
        Ok(format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}+00"
        ))
    } else {
        Ok(format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{fraction}+00"
        ))
    }
}

pub(crate) fn split_timestamptz(text: &str) -> Result<(&str, i64), Box<dyn std::error::Error>> {
    let Some(space) = text.find(' ') else {
        return Err(format!("timestamptz is not 'date time offset': {text:?}").into());
    };

    let tail = &text[space + 1..];
    let offset_at = tail
        .rfind(['+', '-'])
        .ok_or_else(|| format!("timestamptz has no offset: {text:?}"))?;

    let datetime = &text[..space + 1 + offset_at];
    let offset = parse_tz_offset(&tail[offset_at..])?;

    Ok((datetime.trim_end(), offset))
}

/// PostgreSQL prints `+00`, `-05`, `+05:30`, `+00:00`.
pub(crate) fn parse_tz_offset(text: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let (sign, rest) = match text.as_bytes().first() {
        Some(b'+') => (1_i64, &text[1..]),
        Some(b'-') => (-1_i64, &text[1..]),
        _ => return Err(format!("timezone offset is missing a sign: {text:?}").into()),
    };

    let (hours, minutes) = match rest.split_once(':') {
        Some((hours, minutes)) => (hours.parse::<i64>()?, minutes.parse::<i64>()?),
        None => (rest.parse::<i64>()?, 0),
    };

    Ok(sign * (hours * 3_600 + minutes * 60))
}

// Howard Hinnant's civil-from-days / days-from-civil, shifted so day 0 is
// 1970-01-01.
pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = (year - era * 400) as u32;
    let shifted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    i64::from(era) * 146_097 + i64::from(day_of_era) - 719_468
}

pub(crate) fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = (z - era * 146_097) as u32;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_097) / 365;
    let mut year = year_of_era as i32 + (era as i32) * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_period = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_period + 2) / 5 + 1;
    let month = if month_period < 10 {
        month_period + 3
    } else {
        month_period - 9
    };

    if month <= 2 {
        year += 1;
    }

    (year, month, day)
}

pub(crate) fn decode_value(
    column: &ColumnMetadata,
    index: usize,
    value: &TupleData,
) -> Result<Value, Box<dyn std::error::Error>> {
    match value {
        TupleData::Text(bytes) => decode_text_value(column, index, Some(bytes)),

        TupleData::Null => decode_text_value(column, index, None),

        // Toasted column was not rewritten, so pgoutput sent no value; the
        // hole has to be filled from an old image later.
        TupleData::UnchangedToast => Ok(Value::Unchanged),

        TupleData::Binary(_) => Err(format!(
            "Column {} ({}): binary tuple values are not used; pgoutput was started in text mode",
            index, column.name
        )
        .into()),
    }
}

// A tuple carries one slot per column of the relation, so a width mismatch
// means our cached metadata is stale.
pub(crate) fn check_tuple_width(
    relation: &RelationMetadata,
    tuple_values: &[TupleData],
) -> Result<(), Box<dyn std::error::Error>> {
    // agar column or tuple match nahi hota jese ki column mai 3 columns hai lekin tuple 4 values return karta hai to error
    if tuple_values.len() != relation.columns.len() {
        return Err(format!(
            "Tuple contains {} values but relation {}.{} has {} columns",
            tuple_values.len(),
            relation.schema,
            relation.table,
            relation.columns.len()
        )
        .into());
    }

    Ok(())
}

pub(crate) fn decode_row(
    relation: &RelationMetadata,
    tuple_values: &[TupleData], // Tuple ka data
) -> Result<Row, Box<dyn std::error::Error>> {
    check_tuple_width(relation, tuple_values)?;

    let mut values = Vec::new(); // values ko store karne ke liye vector banate hain

    for (index, value) in tuple_values.iter().enumerate() {
        // tuple ke values ko iterate karte hain, jese index 0 se value: first cell, index 1 se value: second cell
        let column = &relation.columns[index];

        values.push((column.name.clone(), decode_value(column, index, value)?));
    }

    Ok(Row { values })
}

// Decode a key tuple, keeping only the replica identity columns. A key tuple
// has a slot for every column and NULLs in the non-identity ones, which must
// not be forwarded as real NULLs.
pub(crate) fn decode_key_row(
    relation: &RelationMetadata,
    tuple_values: &[TupleData],
) -> Result<Row, Box<dyn std::error::Error>> {
    check_tuple_width(relation, tuple_values)?;

    let mut values = Vec::new();

    for (index, value) in tuple_values.iter().enumerate() {
        let column = &relation.columns[index];

        if !column.is_key {
            continue;
        }

        values.push((column.name.clone(), decode_value(column, index, value)?));
    }

    if values.is_empty() {
        return Err(format!(
            "Relation {}.{} sent a key tuple but declares no replica identity columns",
            relation.schema, relation.table
        )
        .into());
    }

    Ok(Row { values })
}

// Read which pre-image PostgreSQL actually sent for an UPDATE or DELETE. The
// server emits at most one of the two tuples, chosen by REPLICA IDENTITY.
pub(crate) fn decode_previous_row(
    relation: &RelationMetadata,
    old_tuple: Option<&postgres_replication::protocol::Tuple>,
    key_tuple: Option<&postgres_replication::protocol::Tuple>,
) -> Result<PreviousRow, Box<dyn std::error::Error>> {
    match (old_tuple, key_tuple) {
        (Some(tuple), _) => Ok(PreviousRow::Full(decode_row(relation, tuple.tuple_data())?)),
        (None, Some(tuple)) => Ok(PreviousRow::Key(decode_key_row(
            relation,
            tuple.tuple_data(),
        )?)),
        (None, None) => Ok(PreviousRow::Absent),
    }
}

// Fill every `Unchanged` hole in the new row from a FULL old image; under any
// thinner identity there is nothing honest to put in the hole, and this refuses.
pub(crate) fn fill_toast_holes(
    new_row: &mut Row,
    old: &PreviousRow,
    relation: &RelationMetadata,
) -> Result<(), Box<dyn std::error::Error>> {
    let holes: Vec<usize> = new_row
        .values
        .iter()
        .enumerate()
        .filter(|(_, (_, value))| matches!(value, Value::Unchanged))
        .map(|(index, _)| index)
        .collect();

    if holes.is_empty() {
        return Ok(());
    }

    let PreviousRow::Full(old_row) = old else {
        let names: Vec<&str> = holes
            .iter()
            .map(|&index| new_row.values[index].0.as_str())
            .collect();

        return Err(format!(
            "UPDATE of {}.{} left TOASTed column(s) ({}) unsent. PostgreSQL does that when \
             the column was not rewritten, and the only image that still carries the value is \
             a FULL old tuple. ALTER TABLE {}.{} REPLICA IDENTITY FULL;",
            relation.schema,
            relation.table,
            names.join(", "),
            relation.schema,
            relation.table
        )
        .into());
    };

    for index in holes {
        let name = new_row.values[index].0.clone();
        let Some((_, value)) = old_row.values.iter().find(|(column, _)| *column == name) else {
            return Err(format!(
                "UPDATE of {}.{} has no old value for TOASTed column {name}",
                relation.schema, relation.table
            )
            .into());
        };

        if matches!(value, Value::Unchanged) {
            return Err(format!(
                "old image of {}.{} also left {name} unsent",
                relation.schema, relation.table
            )
            .into());
        }

        new_row.values[index].1 = value.clone();
    }

    Ok(())
}

// ============================================================
// JSON CONVERSION
// ============================================================

pub(crate) fn row_to_json(row: &Row) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut object = serde_json::Map::new();

    for (name, value) in &row.values {
        let json_value = match value {
            Value::Bool(value) => serde_json::json!(value),
            Value::Int16(value) => serde_json::json!(value),
            Value::Int32(value) => serde_json::json!(value),
            Value::Int64(value) => serde_json::json!(value),
            Value::Float32(value) => serde_json::json!(value),
            Value::Float64(value) => serde_json::json!(value),
            Value::Numeric(value) => serde_json::json!(value),
            Value::Text(value) => serde_json::json!(value),
            Value::Date(value) => serde_json::json!(value),
            Value::Time(value) => serde_json::json!(value),
            Value::Timestamp(value) => serde_json::json!(value),
            Value::Timestamptz(value) => serde_json::json!(value),
            Value::Uuid(value) => serde_json::json!(value),
            Value::Json(value) => value.clone(),
            // Feldera's default JSON flavor encodes BYTEA as an array of bytes.
            Value::Bytes(bytes) => serde_json::Value::Array(
                bytes.iter().copied().map(serde_json::Value::from).collect(),
            ),
            Value::Null => serde_json::Value::Null,
            Value::Unchanged => {
                return Err(format!(
                    "column {name} is an unfilled TOAST hole; it cannot be sent to Feldera"
                )
                .into());
            }
        };

        object.insert(name.clone(), json_value);
    }

    Ok(serde_json::Value::Object(object))
}

/// Quote an identifier for interpolation into SQL.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Undo the escaping `COPY ... TO STDOUT` applies to a text field.
pub(crate) fn unescape_copy_text(field: &[u8]) -> Vec<u8> {
    let mut unescaped = Vec::with_capacity(field.len());
    let mut bytes = field.iter().copied();

    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            unescaped.push(byte);

            continue;
        }

        match bytes.next() {
            Some(b'b') => unescaped.push(0x08),
            Some(b'f') => unescaped.push(0x0c),
            Some(b'n') => unescaped.push(b'\n'),
            Some(b'r') => unescaped.push(b'\r'),
            Some(b't') => unescaped.push(b'\t'),
            Some(b'v') => unescaped.push(0x0b),
            Some(b'\\') => unescaped.push(b'\\'),
            // COPY FROM treats `\` followed by anything else as that character.
            Some(other) => unescaped.push(other),
            // A trailing backslash cannot come out of COPY TO; keep the byte.
            None => unescaped.push(b'\\'),
        }
    }

    unescaped
}

/// Split one line of `COPY` text output into fields.
///
/// `\N` is the null marker and is checked before unescaping.
pub(crate) fn parse_copy_text_row(line: &[u8]) -> Vec<Option<Vec<u8>>> {
    line.split(|byte| *byte == b'\t')
        .map(|field| {
            if field == b"\\N" {
                None
            } else {
                Some(unescape_copy_text(field))
            }
        })
        .collect()
}

/// Turn one line of `COPY` output into a row, using the same value decoder the
/// replication stream uses.
pub(crate) fn decode_copy_row(
    columns: &[ColumnMetadata],
    line: &[u8],
) -> Result<Row, Box<dyn std::error::Error>> {
    let fields = parse_copy_text_row(line);

    if fields.len() != columns.len() {
        return Err(format!(
            "COPY produced a row of {} fields for a table of {} columns",
            fields.len(),
            columns.len()
        )
        .into());
    }

    let mut values = Vec::with_capacity(columns.len());

    for (index, (column, field)) in columns.iter().zip(&fields).enumerate() {
        values.push((
            column.name.clone(),
            decode_text_value(column, index, field.as_deref())?,
        ));
    }

    Ok(Row { values })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn columns() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata {
                name: "id".into(),
                type_id: INT8OID,
                is_key: true,
            },
            ColumnMetadata {
                name: "val".into(),
                type_id: TEXTOID,
                is_key: false,
            },
        ]
    }

    fn relation() -> RelationMetadata {
        RelationMetadata {
            schema: "public".into(),
            table: "items".into(),
            columns: columns(),
        }
    }

    fn row(id: i64, val: Value) -> Row {
        Row {
            values: vec![("id".into(), Value::Int64(id)), ("val".into(), val)],
        }
    }

    // Invariant 5: an Unchanged hole is either filled from a FULL old image or
    // the change is refused.

    #[test]
    fn toast_hole_is_filled_from_a_full_old_image() {
        let mut new_row = row(7, Value::Unchanged);
        let old = PreviousRow::Full(row(7, Value::Text("old value".into())));

        fill_toast_holes(&mut new_row, &old, &relation()).unwrap();

        assert!(matches!(new_row.values[1].1, Value::Text(ref text) if text == "old value"));
    }

    #[test]
    fn toast_hole_is_refused_without_a_full_old_image() {
        let mut new_row = row(7, Value::Unchanged);
        let key_only = PreviousRow::Key(Row {
            values: vec![("id".into(), Value::Int64(7))],
        });

        let error = fill_toast_holes(&mut new_row, &key_only, &relation()).unwrap_err();

        assert!(error.to_string().contains("REPLICA IDENTITY FULL"));

        let mut new_row = row(7, Value::Unchanged);
        assert!(fill_toast_holes(&mut new_row, &PreviousRow::Absent, &relation()).is_err());
    }

    #[test]
    fn toast_hole_is_refused_when_the_old_image_is_a_hole_too() {
        let mut new_row = row(7, Value::Unchanged);
        let old_hole = PreviousRow::Full(row(7, Value::Unchanged));

        let error = fill_toast_holes(&mut new_row, &old_hole, &relation()).unwrap_err();

        assert!(error.to_string().contains("unsent"));
    }

    #[test]
    fn a_row_without_toast_holes_is_left_alone() {
        let mut new_row = row(7, Value::Text("new value".into()));
        let old = PreviousRow::Full(row(7, Value::Text("old value".into())));

        fill_toast_holes(&mut new_row, &old, &relation()).unwrap();

        assert!(matches!(new_row.values[1].1, Value::Text(ref text) if text == "new value"));
    }

    #[test]
    fn row_to_json_refuses_an_unfilled_toast_hole() {
        let error = row_to_json(&row(7, Value::Unchanged)).unwrap_err();

        assert!(error.to_string().contains("TOAST hole"));
    }

    #[rstest]
    #[case("2024-01-15 12:00:00+00", "2024-01-15 12:00:00+00")]
    #[case("2024-01-15 12:00:00.123+05:30", "2024-01-15 06:30:00.123+00")]
    #[case("2024-07-01 00:00:00-05", "2024-07-01 05:00:00+00")]
    #[case("2023-12-31 19:59:00-05", "2024-01-01 00:59:00+00")]
    #[case("2020-02-29 23:59:59.5+00", "2020-02-29 23:59:59.5+00")]
    #[case("2021-01-01 00:00:00+05", "2020-12-31 19:00:00+00")]
    fn timestamptz_always_shifts_to_utc(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(timestamptz_to_utc(input).unwrap(), expected);
    }

    #[rstest]
    #[case("+05:30", 19800)]
    #[case("-00:00", 0)]
    #[case("+00", 0)]
    #[case("-05", -18000)]
    fn tz_offset_is_seconds(#[case] input: &str, #[case] expected: i64) {
        assert_eq!(parse_tz_offset(input).unwrap(), expected);
    }

    #[test]
    fn timestamptz_splits_datetime_from_offset() {
        let (datetime, offset) = split_timestamptz("2024-01-15 12:00:00+05:30").unwrap();

        assert_eq!(datetime, "2024-01-15 12:00:00");
        assert_eq!(offset, 19800);
    }

    #[rstest]
    #[case(b"\\x", vec![])]
    #[case(b"\\x00ff41", vec![0u8, 255, 65])]
    #[case(b"\\x414243", b"ABC".to_vec())]
    fn bytea_hex_decodes(#[case] input: &[u8], #[case] expected: Vec<u8>) {
        assert_eq!(parse_bytea(input).unwrap(), expected);
    }

    #[test]
    fn bytea_rejects_odd_or_non_hex() {
        assert!(parse_bytea(b"\\x0").is_err());
        assert!(parse_bytea(b"\\xzz").is_err());
    }

    #[rstest]
    #[case(b"a\t\\N\tb", vec![Some(b"a".to_vec()), None, Some(b"b".to_vec())])]
    #[case(b"\\N", vec![None])]
    #[case(b"a\\tb", vec![Some(b"a\tb".to_vec())])]
    #[case(b"\\\\", vec![Some(b"\\".to_vec())])]
    #[case(b"\\n\\r", vec![Some(b"\n\r".to_vec())])]
    fn copy_text_decodes_copy_escaping(
        #[case] input: &[u8],
        #[case] expected: Vec<Option<Vec<u8>>>,
    ) {
        assert_eq!(parse_copy_text_row(input), expected);
    }

    #[test]
    fn copy_row_decodes_with_the_stream_decoder() {
        let decoded = decode_copy_row(&columns(), b"7\tvalue").unwrap();

        assert!(matches!(decoded.values[0].1, Value::Int64(7)));
        assert!(matches!(decoded.values[1].1, Value::Text(ref text) if text == "value"));
    }

    #[test]
    fn copy_row_width_mismatch_is_refused() {
        assert!(decode_copy_row(&columns(), b"7\tvalue\textra").is_err());
    }

    // Throughput of the decode and encode pipeline in release builds. Run with:
    //   cargo test --release decode -- --ignored
    // Prints rows/events per second; asserts nothing, so it cannot go stale.

    fn bench_row() -> Vec<u8> {
        b"123456\t789\t1\t2\t3\t4\t5".to_vec()
    }

    fn bench_columns() -> Vec<ColumnMetadata> {
        (1..8)
            .map(|index| ColumnMetadata {
                name: format!("c{index}"),
                type_id: INT4OID,
                is_key: index == 1,
            })
            .collect()
    }

    #[test]
    #[ignore]
    fn decode_and_encode_pipeline_throughput() {
        let columns = bench_columns();
        let raw = bench_row();
        let iterations = 200_000;

        let started = std::time::Instant::now();
        let mut rows = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            rows.push(decode_copy_row(&columns, &raw).unwrap());
        }
        let decode_elapsed = started.elapsed();

        let started = std::time::Instant::now();
        let mut events = Vec::with_capacity(iterations);
        for row in &rows {
            events.push(serde_json::json!({ "insert": row_to_json(row).unwrap() }));
        }
        let json_elapsed = started.elapsed();

        let started = std::time::Instant::now();
        for _ in 0..4 {
            let _ = crate::feldera::encode_change_events(&events);
        }
        let encode_elapsed = started.elapsed();

        println!(
            "decode_copy_row: {iterations} in {decode_elapsed:?} \
                 ({:.0} rows/s)",
            iterations as f64 / decode_elapsed.as_secs_f64()
        );
        println!(
            "row_to_json:     {iterations} in {json_elapsed:?} \
                 ({:.0} rows/s)",
            iterations as f64 / json_elapsed.as_secs_f64()
        );
        println!(
            "encode batch:    4x{iterations} in {encode_elapsed:?} \
                 ({:.0} events/s)",
            4.0 * iterations as f64 / encode_elapsed.as_secs_f64()
        );
    }
}
