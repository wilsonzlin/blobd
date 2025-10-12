use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use blobd_token::BlobdTokens;
use bytes::Bytes;
use futures::channel::mpsc::UnboundedSender;
use futures::stream::iter;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use itertools::Itertools;
use off64::int::create_u16_be;
use off64::int::create_u40_be;
use percent_encoding::utf8_percent_encode;
use percent_encoding::CONTROLS;
use reqwest::header::CONTENT_LENGTH;
use reqwest::header::RANGE;
use reqwest::Body;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::fmt::Display;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use url::Url;

// This client has internal state that is not very cheap to clone, nor memory efficient when cloned lots. Therefore, we don't derive Clone for it; wrap it in an Arc for cheap sharing.
pub struct BlobdClient {
  client: reqwest::Client,
  endpoint: String,
  tokens: BlobdTokens,
}

fn now() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .expect("system clock is before 1970")
    .as_secs()
}

type BoxErr = Box<dyn Error + Send + Sync>;

pub struct BatchCreateObjectEntry<DS: Stream<Item = Result<Bytes, BoxErr>>> {
  pub size: u64,
  pub data_stream: DS,
  pub key: Vec<u8>,
}

pub struct BatchCreatedObjects {
  pub successful_count: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct CreatedObject {
  pub upload_token: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WrittenObjectPart {
  pub write_receipt: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct InspectedObject {
  pub object_id: u64,
  pub content_length: u64,
}

impl BlobdClient {
  /// The `endpoint` should be something like `https://127.0.0.1:8080` or `https://my.blobd.io`. Make sure to omit the trailing slash (i.e. empty path).
  pub fn new(endpoint: String, token_secret: [u8; 32]) -> BlobdClient {
    BlobdClient {
      client: reqwest::Client::new(),
      endpoint,
      tokens: BlobdTokens::new(token_secret),
    }
  }

  fn build_url(&self, key: &str) -> Url {
    let mut url = self.endpoint.clone();
    for p in key.split('/') {
      url.push('/');
      url.extend(utf8_percent_encode(p, CONTROLS));
    }
    Url::parse(&url).unwrap()
  }

  /// NOTE: This does not encode the parameter value, as it's expected this would be passed into a URL builder. However, currently the token doesn't contain any character that would require encoding anyway, so it's safe either way.
  pub fn generate_token_query_param(
    &self,
    action: AuthTokenAction,
    expires_in_seconds: u64,
  ) -> (&'static str, String) {
    let t = AuthToken::new(&self.tokens, action, now() + expires_in_seconds);
    ("t", t)
  }

  pub fn generate_presigned_url(
    &self,
    key: &str,
    action: AuthTokenAction,
    expires_in_seconds: u64,
  ) -> Url {
    let mut url = self.build_url(key);
    let (k, v) = self.generate_token_query_param(action, expires_in_seconds);
    url.query_pairs_mut().append_pair(k, &v);
    url
  }

  /// Parts to this method:
  /// - The list of objects. It's an infallible stream to allow for scenarios where the list is not known ahead of time or is buffered asynchronously.
  /// - Each object's data. It's a fallable stream of chunks of bytes, using the bytes::Bytes type as that's what reqwest requires.
  /// Approaches to the list of objects:
  /// - If it's all in memory, use `futures::stream::iter(my_list_of_objects)`.
  /// - If it can be derived from an existing stream, use the `StreamExt` methods to transform items into `BatchCreateObjectEntry`.
  /// - If it is dynamically built from another thread/async function, use `futures::channel::mpsc::unbounded`; the receiver already implements `Stream` and can be provided as the list.
  /// Approaches to object data stream:
  /// - If it's all in memory, use `futures::stream::once(Ok(bytes::Bytes::from(my_object_data)))`.
  /// - If it's a synchronous Read, wrap in a Stream that reads chunks into a `Bytes`, preferably on a different thread (e.g. `spawn_blocking`).
  /// - If it's an AsyncRead, read in chunks and wrap each chunk with `Bytes::from`.
  /// Approaches to error handling:
  /// - The blobd server will never return a bad status, but will bail as soon as an error occurs.
  /// - However, reqwest will bail if the request body fails, which can occur if a `data_stream` of a `BatchCreateObjectEntry` yields an `Err` chunk.
  /// - If reqwest bails, the response will be unavailable and the amount of successfully committed objects will not be returned.
  /// - Therefore, there are some optional approaches worth considering:
  ///   - Filter out `BatchCreateObjectEntry` items that have a `data_stream` that will definitely fail, as once an entry starts being transferred, there's no way to skip over it midway.
  ///   - When a `data_stream` chunk is about to yield `Err`, return an `Ok` instead with empty data and stop the object list stream (as the current data transferred is midway in an object and there's no way to skip over it). The server will notice that the object was not completely uploaded, decide to bail, and return the successful count.
  /// Provide an optional async channel sender for `transfer_byte_counter` to get data stream chunk sizes as they're about to be uploaded, which can be useful for calculating transfer progress or rate.
  pub async fn batch_create_objects<DS, Objects>(
    &self,
    objects: Objects,
    transfer_byte_counter: Option<UnboundedSender<usize>>,
  ) -> reqwest::Result<BatchCreatedObjects>
  where
    DS: 'static + Stream<Item = Result<Bytes, BoxErr>> + Send + Sync,
    Objects: 'static + Stream<Item = BatchCreateObjectEntry<DS>> + Send + Sync,
  {
    let body_stream = objects.flat_map(move |e| {
      let transfer_byte_counter = transfer_byte_counter.clone();
      iter(vec![
        Ok(Bytes::from(
          create_u16_be(e.key.len().try_into().unwrap()).to_vec(),
        )),
        Ok(Bytes::from(e.key)),
        Ok(Bytes::from(create_u40_be(e.size).to_vec())),
      ])
      .chain(e.data_stream.inspect_ok(move |chunk| {
        if let Some(c) = &transfer_byte_counter {
          c.unbounded_send(chunk.len()).unwrap();
        }
      }))
    });
    let body = Body::wrap_stream(body_stream);
    let res = self
      .client
      .post(self.endpoint.clone())
      .query(&[self.generate_token_query_param(AuthTokenAction::BatchCreateObjects {}, 300)])
      .body(body)
      .send()
      .await?
      .error_for_status()?;
    Ok(BatchCreatedObjects {
      successful_count: res
        .headers()
        .get("x-blobd-objects-created-count")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap(),
    })
  }

  pub async fn create_object(&self, key: &str, size: u64) -> reqwest::Result<CreatedObject> {
    let res = self
      .client
      .post(self.build_url(key))
      .query(&[
        ("size", size.to_string()),
        self.generate_token_query_param(
          AuthTokenAction::CreateObject {
            key: key.as_bytes().to_vec(),
            size,
          },
          300,
        ),
      ])
      .send()
      .await?
      .error_for_status()?;
    Ok(CreatedObject {
      upload_token: res
        .headers()
        .get("x-blobd-upload-token")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap(),
    })
  }

  pub async fn commit_object(
    &self,
    key: &str,
    creation: CreatedObject,
    write_receipts: impl IntoIterator<Item = impl Display>,
  ) -> reqwest::Result<()> {
    self
      .client
      .put(self.build_url(key))
      .query(&[
        ("upload_token", creation.upload_token.to_string()),
        ("write_receipts", write_receipts.into_iter().join(",")),
        self.generate_token_query_param(
          AuthTokenAction::CommitObject {
            key: key.as_bytes().to_vec(),
          },
          300,
        ),
      ])
      .send()
      .await?
      .error_for_status()?;
    Ok(())
  }

  pub async fn delete_object(&self, key: &str) -> reqwest::Result<()> {
    self
      .client
      .delete(self.build_url(key))
      .query(&[self.generate_token_query_param(
        AuthTokenAction::DeleteObject {
          key: key.as_bytes().to_vec(),
        },
        300,
      )])
      .send()
      .await?
      .error_for_status()?;
    Ok(())
  }

  pub async fn inspect_object(&self, key: &str) -> reqwest::Result<InspectedObject> {
    let res = self
      .client
      .head(self.build_url(key))
      .query(&[self.generate_token_query_param(
        AuthTokenAction::InspectObject {
          key: key.as_bytes().to_vec(),
        },
        300,
      )])
      .send()
      .await?
      .error_for_status()?;
    Ok(InspectedObject {
      object_id: res
        .headers()
        .get("x-blobd-object-id")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap(),
      content_length: res
        .headers()
        .get(CONTENT_LENGTH)
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap(),
    })
  }

  pub async fn read_object(
    &self,
    key: &str,
    start: u64,
    end: Option<u64>,
  ) -> reqwest::Result<impl Stream<Item = reqwest::Result<Bytes>>> {
    let res = self
      .client
      .get(self.build_url(key))
      .query(&[self.generate_token_query_param(
        AuthTokenAction::ReadObject {
          key: key.as_bytes().to_vec(),
        },
        300,
      )])
      .header(
        RANGE,
        format!(
          "bytes={}-{}",
          start,
          end.map(|e| e.to_string()).unwrap_or_default()
        ),
      )
      .send()
      .await?
      .error_for_status()?;
    Ok(res.bytes_stream())
  }

  pub async fn write_object(
    &self,
    key: &str,
    creation: CreatedObject,
    offset: u64,
    data: impl Into<Body>,
  ) -> reqwest::Result<WrittenObjectPart> {
    let res = self
      .client
      .patch(self.build_url(key))
      .query(&[
        ("offset", offset.to_string()),
        ("upload_token", creation.upload_token.to_string()),
        self.generate_token_query_param(
          AuthTokenAction::WriteObject {
            key: key.as_bytes().to_vec(),
          },
          300,
        ),
      ])
      .body(data)
      .send()
      .await?
      .error_for_status()?;
    Ok(WrittenObjectPart {
      write_receipt: res
        .headers()
        .get("x-blobd-write-receipt")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap(),
    })
  }
}
