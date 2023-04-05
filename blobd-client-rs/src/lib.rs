use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use blobd_token::BlobdTokens;
use bytes::Bytes;
use futures::channel::mpsc::UnboundedSender;
use futures::stream::iter;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use off64::create_u16_be;
use off64::create_u40_be;
use percent_encoding::utf8_percent_encode;
use percent_encoding::CONTROLS;
use reqwest::header::CONTENT_LENGTH;
use reqwest::header::RANGE;
use reqwest::Body;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::fmt::Write;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

// This client has internal state that is not very cheap to clone, nor memory efficient when cloned lots. Therefore, we don't derive Clone for it; wrap it in an Arc for cheap sharing.
pub struct BlobdClient {
  client: reqwest::Client,
  url_prefix: String,
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
  pub object_id: u64,
  pub upload_id: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct InspectedObject {
  pub object_id: u64,
  pub content_length: u64,
}

impl BlobdClient {
  pub fn new(url_prefix: String, token_secret: [u8; 32]) -> BlobdClient {
    BlobdClient {
      client: reqwest::Client::new(),
      tokens: BlobdTokens::new(token_secret),
      url_prefix,
    }
  }

  fn build_url(&self, key: &str) -> String {
    let mut url = self.url_prefix.clone();
    for (i, p) in key.split('/').enumerate() {
      if i > 0 {
        url.push('/');
      };
      url.extend(utf8_percent_encode(p, CONTROLS));
    }
    url
  }

  pub fn generate_presigned_url(
    &self,
    key: &str,
    action: AuthTokenAction,
    expires_in_seconds: u64,
  ) -> String {
    let mut url = self.build_url(key);
    let t = AuthToken::new(&self.tokens, action, now() + expires_in_seconds);
    write!(url, "?t={}", utf8_percent_encode(&t, CONTROLS)).unwrap();
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
  /// - If it's all in memory, use `futures::stream::Once(Ok(bytes::Bytes::from(my_object_data)))`.
  /// - If it's a synchronous Read, wrap in a Stream that reads chunks into a `Bytes`, preferably on a different thread (e.g. `spawn_blocking`).
  /// - If it's an AsyncRead, read in chunks and wrap each chunk with `Bytes::from`.
  /// Approaches to error handling:
  /// - The blobd server will never return a bad status, but will bail as soon as an error occurs.
  /// - However, reqwest will bail if the request body fails, which can occur if a `data_stream` of a `BatchCreateObjectEntry` yields an `Err` chunk.
  /// - If reqwest bails, the response will be unavailable and the amount of successfully committed objects will not be returned.
  /// - Therefore, there are some optional approaches worth considering:
  ///   - Filter out `BatchCreateObjectEntry` items that have a `data_stream` that will definitely fail. Once an entry starts being transferred, there's no way to skip over it midway.
  ///   - When a `data_stream` chunk is about to yield `Err`, return an `Ok` instead with empty data and stop the object list stream (as the current data transferred is midway in an object and there's no way to skip over it). The server will notice that the object was not completely uploaded, decide to bail, and return the successful count.
  pub async fn batch_create_objects<DS, Objects>(
    &self,
    objects: Objects,
    transfer_byte_counter: UnboundedSender<usize>,
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
      .chain(
        e.data_stream
          .inspect_ok(move |chunk| transfer_byte_counter.unbounded_send(chunk.len()).unwrap()),
      )
    });
    let body = Body::wrap_stream(body_stream);
    let t = AuthToken::new(
      &self.tokens,
      AuthTokenAction::BatchCreateObjects {},
      now() + 300,
    );
    let res = self
      .client
      .post(self.url_prefix.clone())
      .query(&[("t", t)])
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
    let t = AuthToken::new(
      &self.tokens,
      AuthTokenAction::CreateObject {
        key: key.as_bytes().to_vec(),
        size,
      },
      now() + 300,
    );
    let url = self.build_url(key);
    let res = self
      .client
      .post(url)
      .query(&[("size", size.to_string()), ("t", t)])
      .send()
      .await?
      .error_for_status()?;
    Ok(CreatedObject {
      object_id: res
        .headers()
        .get("x-blobd-object-id")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap(),
      upload_id: res
        .headers()
        .get("x-blobd-upload-id")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap(),
    })
  }

  pub async fn commit_object(&self, key: &str, creation: CreatedObject) -> reqwest::Result<()> {
    let t = AuthToken::new(
      &self.tokens,
      AuthTokenAction::CommitObject {
        object_id: creation.object_id,
      },
      now() + 300,
    );
    let url = self.build_url(key);
    self
      .client
      .put(url)
      .query(&[
        ("object_id", creation.object_id.to_string()),
        ("upload_id", creation.upload_id.to_string()),
        ("t", t),
      ])
      .send()
      .await?
      .error_for_status()?;
    Ok(())
  }

  pub async fn delete_object(&self, key: &str) -> reqwest::Result<()> {
    let t = AuthToken::new(
      &self.tokens,
      AuthTokenAction::DeleteObject {
        key: key.as_bytes().to_vec(),
      },
      now() + 300,
    );
    let url = self.build_url(key);
    self
      .client
      .delete(url)
      .query(&[("t", t)])
      .send()
      .await?
      .error_for_status()?;
    Ok(())
  }

  pub async fn inspect_object(&self, key: &str) -> reqwest::Result<InspectedObject> {
    let t = AuthToken::new(
      &self.tokens,
      AuthTokenAction::InspectObject {
        key: key.as_bytes().to_vec(),
      },
      now() + 300,
    );
    let url = self.build_url(key);
    let res = self
      .client
      .head(url)
      .query(&[("t", t)])
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
    let t = AuthToken::new(
      &self.tokens,
      AuthTokenAction::ReadObject {
        key: key.as_bytes().to_vec(),
      },
      now() + 300,
    );
    let url = self.build_url(key);
    let res = self
      .client
      .get(url)
      .query(&[("t", t)])
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
  ) -> reqwest::Result<()> {
    let t = AuthToken::new(
      &self.tokens,
      AuthTokenAction::WriteObject {
        object_id: creation.object_id,
        offset,
      },
      now() + 300,
    );
    let url = self.build_url(key);
    self
      .client
      .patch(url)
      .query(&[
        ("offset", offset.to_string()),
        ("object_id", creation.object_id.to_string()),
        ("upload_id", creation.upload_id.to_string()),
        ("t", t),
      ])
      .body(data)
      .send()
      .await?
      .error_for_status()?;
    Ok(())
  }
}
