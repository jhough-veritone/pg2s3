use aws_config;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::{self, types::ByteStream};
use chrono::Datelike;
use chrono::Timelike;
use crate::mpsc::{Receiver};
use crate::Args;
use std::error::Error;
use tracing::{self, instrument};

#[instrument]
pub async fn send_to_s3(args: Args, rx: Receiver<ByteStream>) -> Result<(), Box<dyn Error>> {
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