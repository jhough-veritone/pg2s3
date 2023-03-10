use crate::funcs::async_fn::send_to_s3;
use crate::funcs::sync_fn::{get_pg_batch, process_pg_rows};
use aws_sdk_s3::{self, types::ByteStream};
use clap::Parser;
use std::str;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{spawn, JoinHandle};
use tokio;
use tracing::{self, instrument};
use tracing_subscriber;

mod errors;
mod funcs;
mod structs;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    pub aws_profile: String,
    pub aws_bucket: String,
    pub pg_host: String,
    pub pg_database: String,
    pub pg_schema: String,
    pub pg_table: String,
    pub pg_keys: Vec<String>,
    #[arg(short, long)]
    pub aws_prefix: Option<String>,
    #[arg(short, long, default_value_t = 0)]
    pub start: i64,
    #[arg(short = 'm', long)]
    pub max: Option<u32>,
    #[arg(short = 'U', long, default_value_t = String::from("postgres"))]
    pub pg_user: String,
    #[arg(short = 'W', long, default_value_t = String::from("postgres"))]
    pub pg_password: String,
    #[arg(short = 'p', long, default_value_t = 5432)]
    pub pg_port: u16,
    #[arg(short = 'd', long, default_value_t = String::from(","))]
    pub delimiter: String,
    #[arg(short, long, default_value_t = 150000)]
    pub batch_size: i64,
    #[arg(short, long, default_value_t = String::from("[NULL]"))]
    pub null: String,
    #[arg(long, default_value_t = false)]
    pub clean_dates: bool,
    #[arg(long, default_value_t = false)]
    pub arr_to_json: bool,
    #[arg(long, default_value_t = false)]
    pub clean_json: bool,
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
    let sync_handlers: [JoinHandle<()>; 2] = [
        spawn(|| {
            get_pg_batch(args, pg_tx)
                .map_err(|e| tracing::error!(error = e, "An error occurred retreiving data"))
                .unwrap()
        }),
        spawn(|| {
            process_pg_rows(processing_tx, pg_rx, delimiter, null)
                .map_err(|e| tracing::error!(error = e, "An error occurred processing rows"))
                .unwrap()
        })
    ];

    // Start async threads
    let async_handlers = [
    spawn(|| async {
            send_to_s3(Args::parse(), processing_rx)
                .await
                .map_err(|e| tracing::error!(error = e, "An error occurred send data to S3"))
                .unwrap()
        })
    ];

    // Wait until all threads are finished
    for handler in sync_handlers {
        handler
        .join()
        .map_err(|e| tracing::error!(error = ?e, "An error occurred joining a sync handler"))
        .unwrap()
    }

    for handler in async_handlers {
        handler
        .join()
        .map_err(|e| {tracing::error!(error = ?e, "An error occurred joining an async handler")})
        .unwrap()
        .await
    }

    tracing::info!("Finished sending all data available to S3");
    ()
}
