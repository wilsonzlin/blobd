use ahash::HashMap;
use ahash::HashMapExt;
use benchmark_types::*;
use maud::{html, Markup, PreEscaped};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;

static FILENAME_RE: Lazy<Regex> =
  Lazy::new(|| Regex::new(r"^results\.(\d+)o\.(\d+)b\.json$").unwrap());

#[derive(Serialize)]
struct Dataset {
  object_size: u64,
  results: Vec<BenchmarkResults>,
}

fn parse_filename(filename: &str) -> Option<(u64, u64)> {
  let caps = FILENAME_RE.captures(filename)?;
  let objects = caps.get(1)?.as_str().parse().ok()?;
  let object_size = caps.get(2)?.as_str().parse().ok()?;
  Some((objects, object_size))
}

fn load_results() -> Vec<Dataset> {
  let cfg_dir = PathBuf::from("../benchmark-runner/cfg");
  let mut results_by_size: HashMap<u64, Vec<BenchmarkResults>> = HashMap::new();

  for entry in fs::read_dir(&cfg_dir).unwrap() {
    let entry = entry.unwrap();
    if !entry.file_type().unwrap().is_dir() {
      continue;
    }

    let benchmark_dir = entry.path();
    for file_entry in fs::read_dir(&benchmark_dir).unwrap() {
      let file_entry = file_entry.unwrap();
      let filename = file_entry.file_name().to_string_lossy().to_string();

      let Some((_, object_size)) = parse_filename(&filename) else {
        continue;
      };

      let content = fs::read_to_string(file_entry.path()).unwrap();
      let result: BenchmarkResults = serde_json::from_str(&content).unwrap();
      results_by_size
        .entry(object_size)
        .or_insert_with(Vec::new)
        .push(result);
    }
  }

  results_by_size
    .into_iter()
    .map(|(object_size, results)| Dataset {
      object_size,
      results,
    })
    .collect()
}

fn generate_html(datasets: Vec<Dataset>) -> Markup {
  let json_data = serde_json::to_string(&datasets).unwrap();
  // Escape for JavaScript string literal: escape backslashes and quotes
  let json_escaped = json_data
    .replace('\\', r#"\\"#)
    .replace('"', r#"\""#)
    .replace('\n', r#"\n"#)
    .replace('\r', r#"\r"#)
    .replace('\t', r#"\t"#);

  html! {
    (maud::DOCTYPE)
    html lang="en" {
      head {
        meta charset="UTF-8" {}
        meta name="viewport" content="width=device-width, initial-scale=1.0" {}
        title { "Benchmark results" }
        link rel="preconnect" href="https://fonts.googleapis.com" {}
        link rel="preconnect" href="https://fonts.gstatic.com" crossorigin {}
        link href="https://fonts.googleapis.com/css2?family=Roboto+Flex:opsz,wght@8..144,300;8..144,400;8..144,500;8..144,600&display=swap" rel="stylesheet" {}
        style {
          (PreEscaped(include_str!("index.css")))
        }
        script src="https://cdn.plot.ly/plotly-2.26.0.min.js" {}
        script {
          (PreEscaped(format!(r#"const data = JSON.parse("{}");"#, json_escaped)))
        }
        script {
          (PreEscaped(include_str!("index.js")))
        }
      }
      body {
        div.container {
          div.controls id="controls" {
            div.control-group {
              label for="metric" { "Metric" }
              select id="metric" {}
            }
          }
          div.section {
            div id="chart" {}
          }
        }
      }
    }
  }
}

fn main() {
  let datasets = load_results();
  println!("Loaded {} datasets", datasets.len());

  let html = generate_html(datasets);
  
  // Create out directory if it doesn't exist
  fs::create_dir_all("out").unwrap();
  
  fs::write("out/index.html", html.into_string()).unwrap();

  println!("Generated out/index.html successfully!");
  println!("Open out/index.html in your browser to view the interactive charts.");
}
