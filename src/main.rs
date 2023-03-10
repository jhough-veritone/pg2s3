use aws_config;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::{self, types::ByteStream};
use chrono::Datelike;
use chrono::Timelike;
use clap::Parser;
use postgres::{Client, Config, NoTls};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::ops::Div;
use std::str;
use std::sync::mpsc::{self, Receiver, Sender};
use tokio;
use tracing::{self, instrument};
use tracing_subscriber;

#[derive(Debug)]
struct MetadataNotFound(String);

impl Error for MetadataNotFound {}

impl std::fmt::Display for MetadataNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Bounds {
    max: i64,
    min: i64,
}

impl Bounds {
    fn get_iterations(&self, batch_size: &i64) -> i64 {
        let raw: i64 = ((self.max - self.min).div(batch_size) as f64).floor() as i64;
        let retval: i64 = if raw == 0 { 1 } else { raw };
        return retval;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ColumnData {
    name: String,
    data_type: String,
    formatted_name: String,
    current_value: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    aws_profile: String,
    aws_bucket: String,
    pg_host: String,
    pg_database: String,
    pg_schema: String,
    pg_table: String,
    pg_keys: Vec<String>,
    #[arg(short, long)]
    aws_prefix: Option<String>,
    #[arg(short, long, default_value_t = 0)]
    start: i64,
    #[arg(short = 'm', long)]
    max: Option<u32>,
    #[arg(short = 'U', long, default_value_t = String::from("postgres"))]
    pg_user: String,
    #[arg(short = 'W', long, default_value_t = String::from("postgres"))]
    pg_password: String,
    #[arg(short = 'p', long, default_value_t = 5432)]
    pg_port: u16,
    #[arg(short = 'd', long, default_value_t = String::from(","))]
    delimiter: String,
    #[arg(short, long, default_value_t = 150000)]
    batch_size: i64,
    #[arg(short, long, default_value_t = String::from("[NULL]"))]
    null: String,
    #[arg(long, default_value_t = false)]
    clean_dates: bool,
    #[arg(long, default_value_t = false)]
    arr_to_json: bool,
    #[arg(long, default_value_t = false)]
    clean_json: bool,
}

#[instrument]
fn get_pg_batch(
    args: Args,
    tx: Sender<Vec<postgres::SimpleQueryMessage>>,
) -> Result<(), Box<dyn Error>> {
    tracing::info!("Connecting to postgres instance");
    let mut pg_client: Client = Config::new()
        .user(&args.pg_user)
        .password(&args.pg_password)
        .dbname(&args.pg_database)
        .host(&args.pg_host)
        .port(args.pg_port)
        .connect(NoTls)?;

    // Get table metadata
    let metadata_statement = pg_client.prepare(format!("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' AND table_schema = '{}'", &args.pg_table, &args.pg_schema).as_str())?;
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
                if it.data_type.to_uppercase().contains("TIMESTAMP") && args.clean_dates {
                    it.formatted_name = format!(
                        "CASE WHEN EXTRACT('YEAR' FROM {}) > 9999 THEN {} + (9999 - EXTRACT('YEAR' FROM {}) || ' years')::interval ELSE {} END AS {}",
                        it.name,
                        it.name,
                        it.name,
                        it.name,
                        it.name
                    );
                }
                else if it.data_type.to_uppercase().contains("ARRAY") && args.arr_to_json {
                    it.formatted_name = format!("TO_JSON({}) AS {}", it.name, it.name);
                }
                else if it.data_type.to_uppercase().contains("JSON") && args.clean_json {
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
        .filter(|row| args.pg_keys.contains(&row.name))
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
                        &args.pg_schema,
                        &args.pg_table
                    )
                ).or_else(|| Some(
                    format!(
                        "SELECT c.reltuples::bigint AS max, 0 AS min FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = {} AND n.nspname = {}",
                        &args.pg_table,
                        &args.pg_schema
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

    let iterations: i64 = bounds.get_iterations(&args.batch_size);
    tracing::info!(
        iterations = iterations,
        min = bounds.min,
        max = bounds.max,
        message = "Retreived iterations"
    );

    for i in args.start..iterations {
        let predicate: String = is_numeric
            .then_some(format!(
                "{} >= {} AND {} < {}",
                key_metadata.get(0).expect("No key found").name,
                i * &args.batch_size + &bounds.min,
                key_metadata.get(0).expect("No key found").name,
                (i + 1) * &args.batch_size + &bounds.min,
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
            &columns, &args.pg_schema, &args.pg_table, &predicate, &order_by, &args.batch_size
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
                key_metadata[i].current_value = get_simple_data(row, args.null.clone())?
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

#[instrument]
fn process_pg_rows(
    tx: Sender<ByteStream>,
    rx: Receiver<Vec<postgres::SimpleQueryMessage>>,
    delimiter: String,
    null: String,
) -> Result<(), Box<dyn Error>> {
    for received in rx {
        tracing::info!(rows_received = received.len(), "Processing received rows");
        let data: ByteStream = ByteStream::from(
            received
                .par_iter()
                .filter_map(|message| match message {
                    postgres::SimpleQueryMessage::Row(row) => {
                        let data = get_simple_data(&row, null.clone())
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

fn get_simple_data(
    row: &postgres::SimpleQueryRow,
    null: String,
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
async fn send_to_s3(args: Args, rx: Receiver<ByteStream>) -> Result<(), Box<dyn Error>> {
    // Get s3 client
    let config: aws_config::SdkConfig = aws_config::from_env()
        .profile_name(&args.aws_profile)
        .load()
        .await;
    let s3_client: S3Client = aws_sdk_s3::Client::new(&config);
    let prefix: String = vec![
        args.aws_prefix.clone().unwrap_or("".to_string()),
        args.pg_database.clone(),
        args.pg_schema.clone(),
        args.pg_table.clone(),
    ]
    .join("/");

    for received in rx {
        let current_time = chrono::Utc::now();
        let filename: String = vec![
            prefix.clone(),
            current_time.year().to_string(),
            current_time.month().to_string(),
            current_time.day().to_string(),
            current_time.hour().to_string(),
            uuid::Uuid::new_v4().as_hyphenated().to_string(),
        ]
        .into_iter()
        .map(|value| value.to_uppercase())
        .collect::<Vec<String>>()
        .join("/");

        tracing::info!("Sending data to S3 bucket");
        s3_client
            .put_object()
            .bucket(&args.aws_bucket)
            .body(received)
            .key(&filename)
            .send()
            .await
            .unwrap();
    }
    Ok(())
}

#[instrument]
#[tokio::main]
async fn main() {
    // Set logging
    let subscriber = tracing_subscriber::fmt()
        .pretty()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Check and validate args
    let args: Args = Args::parse();
    if args.pg_keys.is_empty() {
        panic!("No key struct was sent. Please check your positional arguments.")
    }
    let delimiter: String = args.delimiter.clone();
    let null: String = args.null.clone();

    // Create threading channels
    let (pg_tx, pg_rx): (
        Sender<Vec<postgres::SimpleQueryMessage>>,
        Receiver<Vec<postgres::SimpleQueryMessage>>,
    ) = mpsc::channel();
    let (processing_tx, processing_rx): (Sender<ByteStream>, Receiver<ByteStream>) =
        mpsc::channel();

    // Start sync threads
    let get_rows_handle = std::thread::spawn(|| {
        if let Err(e) = get_pg_batch(args, pg_tx) {
            tracing::error!("An error occurred getting data from the table: {}", e);
            panic!("{}", e)
        }
    });
    let process_rows_handle = std::thread::spawn(|| {
        if let Err(e) = process_pg_rows(processing_tx, pg_rx, delimiter, null) {
            tracing::error!("An error occurred processing data: {}", e);
            panic!("{}", e);
        }
    });

    // Start async threads
    let put_object_handle = std::thread::spawn(|| async {
        if let Err(e) = send_to_s3(Args::parse(), processing_rx).await {
            tracing::error!("An error occurred sending data to S3: {}", e);
            panic!("{}", e);
        }
    });

    // Wait until all threads are finished
    if let Err(e) = get_rows_handle.join() {
        tracing::error!("An error occurred getting data from table: {:#?}", e);
        panic!("{:#?}", e);
    };
    if let Err(e) = process_rows_handle.join() {
        tracing::error!("An error occured processing data {:#?}", e);
        panic!("{:#?}", e);
    };
    match put_object_handle.join() {
        Ok(f) => f.await,
        Err(e) => {
            tracing::error!("An error occurred putting data in S3: {:#?}", e);
            panic!("{:#?}", e)
        }
    };

    tracing::info!("Finished sending all data available to S3");
    ()
}
