extern crate pg2s3;
use aws_sdk_s3::{self, types::ByteStream};
use clap::Parser;
use pg2s3::funcs::async_fn::send_to_s3;
use pg2s3::funcs::sync_fn::{get_pg_batch, process_pg_rows};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{spawn, JoinHandle};
use tokio;
use tracing::{self, instrument};
use tracing_subscriber;

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
    pub max: Option<i64>,
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

impl Args {
    pub fn get_pg_args(
        &self,
    ) -> (
        String,
        u16,
        String,
        String,
        String,
        Vec<String>,
        String,
        String,
    ) {
        (
            self.pg_host.clone(),
            self.pg_port,
            self.pg_database.clone(),
            self.pg_schema.clone(),
            self.pg_table.clone(),
            self.pg_keys.clone(),
            self.pg_user.clone(),
            self.pg_password.clone(),
        )
    }

    pub fn get_formatting_rules_args(&self) -> (bool, bool, bool) {
        (self.clean_dates, self.arr_to_json, self.clean_json)
    }

    pub fn get_aws_args(&self) -> (String, String, String) {
        (
            self.aws_profile.clone(),
            self.aws_bucket.clone(),
            self.aws_prefix
                .as_ref()
                .map_or("".to_string(), |v| v.to_string())
                .clone(),
        )
    }

    pub fn get_iter_args(&self) -> (i64, i64, i64) {
        (self.batch_size, self.start, self.max.map_or(0, |v| v))
    }

    pub fn get_output_args(&self) -> (String, String) {
        (self.delimiter.clone(), self.null.clone())
    }
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
    let pg_args = args.get_pg_args();
    let pg_args_2 = pg_args.clone();
    let aws_args = args.get_aws_args();
    let iter_args = args.get_iter_args();
    let formatting_rules_args = args.get_formatting_rules_args();
    let output_args = args.get_output_args();
    let output_args_2 = output_args.clone();

    // Create threading channels
    let (pg_tx, pg_rx): (
        Sender<Vec<postgres::SimpleQueryMessage>>,
        Receiver<Vec<postgres::SimpleQueryMessage>>,
    ) = mpsc::channel();
    let (processing_tx, processing_rx): (Sender<ByteStream>, Receiver<ByteStream>) =
        mpsc::channel();

    // Start sync threads
    let sync_handlers: [JoinHandle<()>; 2] = [
        spawn(move || {
            get_pg_batch(
                pg_args,
                formatting_rules_args,
                iter_args,
                output_args,
                pg_tx,
            )
            .map_err(|e| {
                tracing::error!(error = e, "An error occurred retreiving data");
                panic!("{}", e)
            })
            .unwrap();
        }),
        spawn(move || {
            process_pg_rows(output_args_2, processing_tx, pg_rx)
                .map_err(|e| {
                    tracing::error!(error = e, "An error occurred processing rows");
                    panic!("{}", e)
                })
                .unwrap();
        }),
    ];

    // Start async threads
    let async_handlers = [spawn(move || async move {
        send_to_s3(aws_args, pg_args_2, processing_rx)
            .await
            .map_err(|e| {
                tracing::error!(error = e, "An error occurred send data to S3");
                panic!("{}", e)
            })
            .unwrap()
    })];

    // Wait until all threads are finished
    for handler in sync_handlers {
        handler
            .join()
            .map_err(|e| tracing::error!(error = ?e, "An error occurred joining a sync handler"))
            .expect("An error occurred in a sync process")
    }

    for handler in async_handlers {
        handler
            .join()
            .map_err(|e| tracing::error!(error = ?e, "An error occurred joining an async handler"))
            .expect("An error occurred in an async process")
            .await
    }

    tracing::info!("Finished sending all data available to S3");
}
