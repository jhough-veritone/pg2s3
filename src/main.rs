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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Bounds {
    max: i32,
    min: i32,
}

impl Bounds {
    fn get_iterations(&self, batch_size: &i32) -> i32 {
        ((self.max - self.min).div(batch_size) as f32).floor() as i32
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
    start: i32,
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
    batch_size: i32,
    #[arg(short, long, default_value_t = String::from("[NULL]"))]
    null: String,
}

fn get_pg_batch(
    args: Args,
    tx: Sender<Vec<postgres::SimpleQueryMessage>>,
) -> Result<(), Box<dyn Error>> {
    let mut pg_client: Client = Config::new()
        .user(&args.pg_user)
        .password(&args.pg_password)
        .dbname(&args.pg_database)
        .host(&args.pg_host)
        .port(args.pg_port)
        .connect(NoTls)?;

    // Get table metadata
    let metadata_statement = pg_client.prepare(format!("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' AND table_schema = '{}'", &args.pg_table, &args.pg_schema).as_str())?;
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
                if it.data_type.to_uppercase().contains("TIMESTAMP") {
                    it.formatted_name = format!(
                        "CASE WHEN EXTRACT('YEAR' FROM {}) > 9999 THEN {} + (9999 - EXTRACT('YEAR' FROM {}) || ' years')::interval ELSE {} END AS {}",
                        it.name,
                        it.name,
                        it.name,
                        it.name,
                        it.name
                    );
                }
                else if it.data_type.to_uppercase().contains("ARRAY") {
                    it.formatted_name = format!("TO_JSON({}) AS {}", it.name, it.name);
                }
                else if it.data_type.to_uppercase().contains("JSON") {
                    it.formatted_name = format!("REGEXP_REPLACE({}::text, '\n|\r', 'g')::json AS {}", it.name, it.name); 
                }
                it
            }.to_owned()
        )
        .collect();

    if column_metadata.is_empty() {
        panic!("No metadata retrieved");
    }

    let mut key_metadata: Vec<ColumnData> = column_metadata
        .clone()
        .into_iter()
        .filter(|row| args.pg_keys.contains(&row.name))
        .collect::<Vec<ColumnData>>();

    if key_metadata.is_empty() {
        panic!("No key metadata retreived");
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
                        "SELECT MIN({}) AS min, MAX({}) AS max FROM {}.{}",
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
            max: row.get("max"),
            min: row.get("min"),
        }
    };

    let iterations: i32 = bounds.get_iterations(&args.batch_size);

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

        let rows: Vec<postgres::SimpleQueryMessage> =
            pg_client.simple_query(batch_statement.as_str())?;

        for i in 0..key_metadata.len() {
            if let postgres::SimpleQueryMessage::Row(row) =
                rows.last().expect("No last row message found")
            {
                key_metadata[i].current_value = get_simple_data(row)
                    .get(i)
                    .expect("No last row data found")
                    .clone();
            }
        }

        tx.send(rows)?;
    }

    Ok(())
}

fn process_pg_rows(
    tx: Sender<ByteStream>,
    rx: Receiver<Vec<postgres::SimpleQueryMessage>>,
    delimiter: String,
) -> Result<(), Box<dyn Error>> {
    for received in rx {
        tx.send(ByteStream::from(
            received
                .par_iter()
                .filter_map(|message| match message {
                    postgres::SimpleQueryMessage::Row(row) => {
                        Some(get_simple_data(&row).join(&delimiter))
                    }
                    _ => None,
                })
                .collect::<Vec<String>>()
                .join("\n")
                .as_bytes()
                .to_owned(),
        ))?;
    }
    Ok(())
}

fn get_simple_data(row: &postgres::SimpleQueryRow) -> Vec<String> {
    row.columns()
        .iter()
        .map(|column| row.get(column.name()).expect("Null row data").to_string())
        .collect::<Vec<String>>()
}

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

#[tokio::main]
async fn main() {
    // Check and validate args
    let args: Args = Args::parse();
    if args.pg_keys.is_empty() {
        panic!("No key struct was sent. Please check your positional arguments.")
    }
    let delimiter: String = args.delimiter.clone();

    // Create threading channels
    let (pg_tx, pg_rx): (
        Sender<Vec<postgres::SimpleQueryMessage>>,
        Receiver<Vec<postgres::SimpleQueryMessage>>,
    ) = mpsc::channel();
    let (processing_tx, processing_rx): (Sender<ByteStream>, Receiver<ByteStream>) =
        mpsc::channel();

    // Start sync threads
    let get_rows_handle = std::thread::spawn(|| get_pg_batch(args, pg_tx).unwrap());
    let process_rows_handle =
        std::thread::spawn(|| process_pg_rows(processing_tx, pg_rx, delimiter).unwrap());

    // Start async threads
    let put_object_handle = std::thread::spawn(|| async {
        send_to_s3(Args::parse(), processing_rx).await.unwrap();
    });

    // Wait until all threads are finished
    get_rows_handle.join().unwrap();
    process_rows_handle.join().unwrap();
    put_object_handle.join().unwrap().await;

    ()
}
