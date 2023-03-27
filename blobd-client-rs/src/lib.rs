use blobd_token::AuthToken;
use blobd_token::AuthTokenAction;
use blobd_token::BlobdTokens;
use bytes::Bytes;
use futures::Stream;
use percent_encoding::utf8_percent_encode;
use percent_encoding::CONTROLS;
use reqwest::header::CONTENT_LENGTH;
use reqwest::Body;
use serde::Deserialize;
use serde::Serialize;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

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
      .query(&("t", t))
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
      .query(&("t", t))
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
      .query(&("t", t))
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
