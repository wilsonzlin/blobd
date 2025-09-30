use std::sync::Arc;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{primitives::ByteStream, types::{builders::CompletedMultipartUploadBuilder, CompletedMultipartUpload, CompletedPart}, Client};
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use dashmap::DashMap;
use futures::StreamExt;
use itertools::Itertools;
use tokio_util::io::ReaderStream;

use crate::{BlobdProvider, CommitObjectInput, CommitObjectOutput, CreateObjectInput, CreateObjectOutput, DeleteObjectInput, InspectObjectInput, InspectObjectOutput, ReadObjectInput, ReadObjectOutput, WriteObjectInput};

pub struct S3StoreConfig {
    pub region: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub part_size: u64,
}

impl S3StoreConfig {
    pub async fn build_store(self) -> S3Store {
        let S3StoreConfig { region, endpoint, access_key_id, secret_access_key, bucket, part_size } = self;
        let creds = Credentials::from_keys(access_key_id, secret_access_key, None);
        let config = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(creds)
            .endpoint_url(endpoint)
            .region(Region::new(region))
            .load()
            .await;
        let client = Client::new(&config);
        S3Store { client, bucket, part_size, in_flight_uploads: DashMap::new() }
    }
}

#[derive(Default)]
struct InFlightUpload {
    key: String,
    part_etags: DashMap<i32, String>,
}

pub struct S3Store {
    client: Client,
    bucket: String,
    part_size: u64,
    in_flight_uploads: DashMap<String, InFlightUpload>,
}

#[async_trait]
impl BlobdProvider for S3Store {
    fn metrics(&self) -> Vec<(&'static str, u64)> {
        vec![]
    }

    async fn wait_for_end(&self) {
    }

    async fn create_object(&self, input: CreateObjectInput) -> CreateObjectOutput {
        let res = self.client.create_multipart_upload()
            .bucket(&self.bucket)
            .key(BASE64_URL_SAFE_NO_PAD.encode(&input.key))
            .send()
            .await
            .unwrap();
        CreateObjectOutput {
            token: Arc::new(res.upload_id.unwrap()),
        }
    }

    async fn write_object<'a>(&'a self, input: WriteObjectInput<'a>) {
        assert_eq!(input.offset % self.part_size, 0);
        let part_number = i32::try_from(input.offset / self.part_size).unwrap();
        let key = BASE64_URL_SAFE_NO_PAD.encode(&input.key);
        let upload_id = input.incomplete_token.downcast::<String>().unwrap();
        let res = self.client.upload_part()
            .bucket(&self.bucket)
            .key(key.clone())
            .upload_id(upload_id.to_string())
            .part_number(part_number)
            .body(input.data.to_vec().into())
            .send()
            .await
            .unwrap();
        let etag = res.e_tag.unwrap();
        let in_flight_upload = self.in_flight_uploads.entry(upload_id.to_string()).or_insert_with(|| InFlightUpload { key, part_etags: DashMap::new() }).part_etags.insert(part_number, etag);
        assert!(in_flight_upload.is_none());
    }

    async fn commit_object(&self, input: CommitObjectInput) -> CommitObjectOutput {
        let upload_id = input.incomplete_token.downcast::<String>().unwrap();
        let in_flight_upload = self.in_flight_uploads.remove(upload_id.as_str()).unwrap().1;
        let parts = in_flight_upload.part_etags.into_iter().map(|(part_number, etag)| CompletedPart::builder().part_number(part_number).e_tag(etag).build()).collect_vec();
        self.client.complete_multipart_upload()
            .bucket(&self.bucket)
            .key(in_flight_upload.key)
            .upload_id(upload_id.to_string())
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
            .send()
            .await
            .unwrap();
        CommitObjectOutput {
            object_id: None,
        }
    }

    async fn inspect_object(&self, input: InspectObjectInput) -> InspectObjectOutput {
        let res = self.client.head_object()
            .bucket(&self.bucket)
            .key(BASE64_URL_SAFE_NO_PAD.encode(&input.key))
            .send()
            .await
            .unwrap();
        InspectObjectOutput {
            id: None,
            size: res.content_length.unwrap().try_into().unwrap(),
        }
    }

    async fn read_object(&self, input: ReadObjectInput) -> ReadObjectOutput {
        let res = self.client.get_object()
            .bucket(&self.bucket)
            .key(BASE64_URL_SAFE_NO_PAD.encode(&input.key))
            .send()
            .await
            .unwrap();
        ReadObjectOutput {
            data_stream: ReaderStream::new(res.body.into_async_read()).map(|chunk| chunk.unwrap().to_vec()).boxed(),
        }
    }

    async fn delete_object(&self, input: DeleteObjectInput) {
        self.client.delete_object()
            .bucket(&self.bucket)
            .key(BASE64_URL_SAFE_NO_PAD.encode(&input.key))
            .send()
            .await
            .unwrap();
    }
}

