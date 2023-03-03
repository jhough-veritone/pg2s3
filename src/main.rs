// use aws_config;
// use aws_sdk_s3::presigning::config::PresigningConfig;
// use aws_sdk_s3::Client as S3Client;
// use aws_sdk_s3::{self, types::ByteStream};
// use chrono;
// use chrono::Datelike;
// use chrono::Timelike;
use clap::Parser;
// use postgres::Row;
use postgres::{Client, Config, NoTls};
use serde::{Deserialize, Serialize};
// use sha2::Digest;
use std::error::Error;
// use std::hash::{Hash, Hasher};
use std::ops::Div;
use std::str;
use std::sync::mpsc::{self, Receiver, Sender};
// use std::thread;
// use std::time::Duration;

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

fn get_pg_batch(args: Args, _tx: Sender<String>) -> Result<(), Box<dyn Error>> {
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

        let batch_statement = format!(
            "SELECT {} FROM {}.{} WHERE {} ORDER BY {} LIMIT {}",
            &columns, &args.pg_schema, &args.pg_table, &predicate, &order_by, &args.batch_size
        );

        let rows: Vec<Vec<String>> = pg_client
            .simple_query(batch_statement.as_str())?
            .into_iter()
            .filter_map(|message| match message {
                postgres::SimpleQueryMessage::Row(row) => Some(get_simple_data(row)),
                _ => None,
            })
            .collect();

        // Send rows as ByteStream by sender

        for i in 0..key_metadata.len() {
            key_metadata[i].current_value = rows
                .last()
                .expect("No last row was retrieved")
                .get(i)
                .expect("No last row data at the index was found")
                .clone();
        }
    }

    Ok(())
}

fn get_simple_data(row: postgres::SimpleQueryRow) -> Vec<String> {
    row.columns()
        .iter()
        .map(|column| row.get(column.name()).expect("Null row data").to_string())
        .collect::<Vec<String>>()
}

// async fn send_to_s3(
//     profile_name: String,
//     aws_bucket: String,
//     rx: Receiver<String>,
// ) -> Result<(), Box<dyn Error>> {
//     // Get s3 client
//     let config: aws_config::SdkConfig = aws_config::from_env()
//         .profile_name(profile_name)
//         .load()
//         .await;
//     let s3_client: S3Client = aws_sdk_s3::Client::new(&config);
//     let mut hasher: sha2::Sha256 = sha2::Sha256::new();

//     // Extrapolate bucket from time as bucket/Y/M/D/H/hash

//     for received in rx {
//         let current_time = chrono::Utc::now();
//         let year = current_time.year();
//         let month = current_time.month();
//         let day = current_time.day();
//         let hour = current_time.hour();

//         let bucket = format!("{}/{}/{}/{}/{}", &aws_bucket, &year, &month, &day, &hour);
//         hasher.update(current_time.to_string());
//         let hash_bytes: Vec<u8> = hasher.finalize().into_iter().collect();
//         let key =
//             std::str::from_utf8(&hash_bytes).expect("Failed to convert hashed bytes to string.");
//         put_object(&s3_client, &bucket, key, body).await?;
//     }
//     Ok(())
// }

fn main() {
    // Validate and parse arguments
    let args: Args = Args::parse();
    // let aws_profile: String = args.aws_profile.clone();
    // let aws_bucket: String = args.aws_bucket.clone();
    if args.pg_keys.is_empty() {
        panic!("No key struct was sent. Please check your positional arguments.")
    }

    let (tx, _): (Sender<String>, Receiver<String>) = mpsc::channel();

    get_pg_batch(args, tx).unwrap();

    ()
}
