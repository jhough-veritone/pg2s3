use crate::errors::metadata_not_found::MetadataNotFound;
use crate::structs::{bounds::Bounds, column_data::ColumnData};
use aws_sdk_s3::{self, types::ByteStream};
use postgres::{Client, Config, NoTls};
use rayon::prelude::*;
use std::error::Error;
use std::sync::mpsc::{Receiver, Sender};
use tracing::{self, instrument};

#[instrument]
pub fn get_pg_batch(
    pg_args: (
        String,
        u16,
        String,
        String,
        String,
        Vec<String>,
        String,
        String,
    ),
    formatting_rules_args: (bool, bool, bool),
    iter_args: (i64, i64, i64),
    output_args: (String, String),
    tx: Sender<Vec<postgres::SimpleQueryMessage>>,
) -> Result<(), Box<dyn Error>> {
    let (pg_host, pg_port, pg_database, pg_schema, pg_table, pg_keys, pg_user, pg_password) =
        pg_args;
    let (clean_dates, arr_to_json, clean_json) = formatting_rules_args;
    let (batch_size, start, _) = iter_args;
    let (null, _) = output_args;
    tracing::info!("Connecting to postgres instance");
    let mut pg_client: Client = Config::new()
        .user(&pg_user)
        .password(&pg_password)
        .dbname(&pg_database)
        .host(&pg_host)
        .port(pg_port)
        .connect(NoTls)?;

    // Get table metadata
    let metadata_statement = pg_client.prepare(format!("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' AND table_schema = '{}'", pg_table, pg_schema).as_str())?;
    tracing::info!("Collecting table metadata");
    let column_metadata: Vec<ColumnData> = pg_client
        .query(&metadata_statement, &[])?
        .into_iter()
        .map(|row| ColumnData {
            name: row.get("column_name"),
            data_type: row.get("data_type"),
            formatted_name: row.get("column_name"),
            current_value: "".to_string(),
        })
        .collect::<Vec<ColumnData>>()
        .iter_mut()
        .map(|it|
            {
                if it.data_type.to_uppercase().contains("TIMESTAMP") && clean_dates {
                    it.formatted_name = format!(
                        "CASE WHEN EXTRACT('YEAR' FROM {}) > 9999 THEN {} + (9999 - EXTRACT('YEAR' FROM {}) || ' years')::interval ELSE {} END AS {}",
                        it.name,
                        it.name,
                        it.name,
                        it.name,
                        it.name
                    );
                }
                else if it.data_type.to_uppercase().contains("ARRAY") && arr_to_json {
                    it.formatted_name = format!("TO_JSON({}) AS {}", it.name, it.name);
                }
                else if it.data_type.to_uppercase().contains("JSON") && clean_json {
                    it.formatted_name = format!("REGEXP_REPLACE({}::text, '\n|\r', 'g')::json AS {}", it.name, it.name); 
                }
                it
            }.to_owned()
        )
        .collect();

    if column_metadata.is_empty() {
        return Err(Box::new(MetadataNotFound(
            "No metadata for the specified table.".into(),
        )));
    }

    let mut key_metadata: Vec<ColumnData> = column_metadata
        .clone()
        .into_iter()
        .filter(|row| pg_keys.contains(&row.name))
        .collect::<Vec<ColumnData>>();

    if key_metadata.is_empty() {
        return Err(Box::new(MetadataNotFound(
            "No metadata for the specified key.".into(),
        )));
    }

    let order_by: String = key_metadata
        .iter()
        .map(|it| it.name.clone())
        .collect::<Vec<String>>()
        .join(", ");

    let columns: String = column_metadata
        .iter()
        .map(|it| it.formatted_name.clone())
        .collect::<Vec<String>>()
        .join(", ");

    let is_numeric: bool = [
        "smallint",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "real",
        "double precision",
        "smallserial",
        "serial",
        "bigserial",
    ]
    .contains(
        &key_metadata
            .get(0)
            .expect("No key retreived for is_numeric comparison")
            .data_type
            .as_str(),
    );

    let bounds_statement: postgres::Statement = pg_client
            .prepare(
        is_numeric
                .then_some(
                    format!(
                        "SELECT MIN({})::int8 AS min, MAX({})::int8 AS max FROM {}.{}",
                        &key_metadata.get(0).expect("Empty key data").name,
                        &key_metadata.get(0).expect("Empty key data").name,
                        pg_schema,
                        pg_table
                    )
                ).or_else(|| Some(
                    format!(
                        "SELECT c.reltuples::bigint AS max, 0 AS min FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = {} AND n.nspname = {}",
                        pg_table,
                        pg_schema
                    )
                ))
                .unwrap()
                .as_ref()
            )?;

    let bounds: Bounds = {
        let row = pg_client.query_one(&bounds_statement, &[])?;
        Bounds {
            max: row.try_get::<&str, i64>("max")?,
            min: row.try_get::<&str, i64>("min")?,
        }
    };

    let iterations: i64 = bounds.get_iterations(&batch_size);
    tracing::info!(
        iterations = iterations,
        min = bounds.min,
        max = bounds.max,
        message = "Retreived iterations"
    );

    for i in start..iterations {
        let predicate: String = is_numeric
            .then_some(format!(
                "{} >= {} AND {} < {}",
                key_metadata.get(0).expect("No key found").name,
                i * &batch_size + &bounds.min,
                key_metadata.get(0).expect("No key found").name,
                (i + 1) * &batch_size + &bounds.min,
            ))
            .or_else(|| {
                Some(
                    key_metadata
                        .iter()
                        .map(|it| format!("'{}'::text >= '{}'::text", it.name, it.current_value))
                        .collect::<Vec<String>>()
                        .join(" AND "),
                )
            })
            .expect("Something went wrong processing the predicate");

        let batch_statement: String = format!(
            "SELECT {} FROM {}.{} WHERE {} ORDER BY {} LIMIT {}",
            &columns, pg_schema, pg_table, &predicate, &order_by, &batch_size
        );

        tracing::info!(i = i, "Retreiving data from table");
        let rows: Vec<postgres::SimpleQueryMessage> =
            pg_client.simple_query(batch_statement.as_str())?;

        if rows.len() == 0 {
            tracing::info!(i = i, "No rows returned from query");
            continue;
        }

        for i in 0..key_metadata.len() {
            if let postgres::SimpleQueryMessage::Row(row) =
                rows.last().expect("No last row message found")
            {
                key_metadata[i].current_value = get_simple_data(row, &null)?
                    .get(i)
                    .expect("No last row data found")
                    .clone();
            }
        }

        tracing::info!(
            i = i,
            rows_retreived = rows.len(),
            "Sending data to processing thread"
        );
        tx.send(rows)?;
    }

    Ok(())
}

fn get_simple_data(
    row: &postgres::SimpleQueryRow,
    null: &str,
) -> Result<Vec<String>, Box<dyn Error>> {
    tracing::debug!("Retreiving data from row");
    Ok(row
        .columns()
        .iter()
        .map(|column| {
            row.try_get(column.name())
                .expect(
                    format!(
                        "Could not find column {} when processing data",
                        column.name()
                    )
                    .as_str(),
                )
                .map_or_else(|| null.to_string(), |x| x.to_string())
        })
        .collect::<Vec<String>>())
}

#[instrument]
pub fn process_pg_rows(
    output_args: (String, String),
    tx: Sender<ByteStream>,
    rx: Receiver<Vec<postgres::SimpleQueryMessage>>,
) -> Result<(), Box<dyn Error>> {
    let (null, delimiter) = output_args;
    for received in rx {
        tracing::info!(rows_received = received.len(), "Processing received rows");
        let data: ByteStream = ByteStream::from(
            received
                .par_iter()
                .filter_map(|message| match message {
                    postgres::SimpleQueryMessage::Row(row) => {
                        let data = get_simple_data(&row, &null)
                            .expect("Error occurred retreiving data")
                            .join(&delimiter);
                        Some(data)
                    }
                    _ => None,
                })
                .collect::<Vec<String>>()
                .join("\n")
                .as_bytes()
                .to_owned(),
        );
        tracing::info!("Passing byte stream to S3 sender");
        tx.send(data)?;
    }
    Ok(())
}
