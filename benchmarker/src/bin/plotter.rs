use ahash::HashMap;
use ahash::HashMapExt;
use blobd_benchmarker::*;
use charts_rs::svg_to_webp;
use charts_rs::Box as ChartBox;
use charts_rs::HorizontalBarChart;
use charts_rs::LegendCategory;
use charts_rs::LineChart;
use charts_rs::Series;
use chrono::DateTime;
use chrono::Utc;
use once_cell::sync::Lazy;
use regex::Regex;
use std::fs;
use std::path::PathBuf;

static FILENAME_RE: Lazy<Regex> =
  Lazy::new(|| Regex::new(r"^results\.(\d+)o\.(\d+)b\.json$").unwrap());

struct Dataset {
  object_size: u64,
  results: Vec<BenchmarkResults>,
}

fn parse_filename(filename: &str) -> Option<(u64, u64)> {
  // Pattern: results.{objects}o.{object_size}b.json
  let caps = FILENAME_RE.captures(filename)?;
  let objects = caps.get(1)?.as_str().parse().ok()?;
  let object_size = caps.get(2)?.as_str().parse().ok()?;
  Some((objects, object_size))
}

fn load_results() -> Vec<Dataset> {
  let cfg_dir = PathBuf::from("cfg");
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

fn generate_horizontal_bar_chart(path: &PathBuf, title: &str, data: Vec<(String, f64)>) {
  let mut sorted_data = data;
  sorted_data.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

  let labels: Vec<String> = sorted_data.iter().map(|(label, _)| label.clone()).collect();
  let values: Vec<f32> = sorted_data.iter().map(|(_, val)| *val as f32).collect();

  let series = Series::new("".to_string(), values);
  let mut chart = HorizontalBarChart::new_with_theme(vec![series], labels, "light");

  chart.title_text = title.to_string();
  chart.width = 1200.0;
  chart.height = 80.0 + sorted_data.len() as f32 * 35.0;
  chart.legend_show = Some(false);

  let svg = chart.svg().unwrap();
  let webp_data = svg_to_webp(&svg).unwrap();
  fs::write(path, webp_data).unwrap();
}

fn generate_line_chart(
  path: &PathBuf,
  title: &str,
  series_data: Vec<(String, Vec<(f64, f64)>)>,
  y_label: &str,
) {
  if series_data.is_empty() || series_data.iter().all(|(_, data)| data.is_empty()) {
    return;
  }

  // Normalize X to 0-100% and sample at regular intervals
  let num_points = 101; // 0%, 1%, 2%, ..., 100%
  let mut series_list = Vec::new();

  for (label, data) in &series_data {
    if data.is_empty() {
      continue;
    }

    // Find time range
    let max_x = data.iter().map(|(x, _)| *x).fold(0.0f64, f64::max);
    if max_x <= 0.0 {
      continue;
    }

    // Sample data at regular intervals from 0% to 100%
    let y_values: Vec<f32> = (0..num_points)
      .map(|i| {
        let percent = i as f64 / (num_points - 1) as f64; // 0.0 to 1.0
        let target_x = percent * max_x;

        // Find the closest point or interpolate
        let pos = match data.iter().position(|(x, _)| *x >= target_x) {
          None => return data.last().unwrap().1 as f32,
          Some(0) => return data[0].1 as f32,
          Some(pos) => pos,
        };

        // Interpolate between pos-1 and pos
        let (x1, y1) = data[pos - 1];
        let (x2, y2) = data[pos];
        let y = if x2 > x1 {
          let ratio = (target_x - x1) / (x2 - x1);
          y1 + ratio * (y2 - y1)
        } else {
          y1
        };
        y as f32
      })
      .collect();

    series_list.push(Series::new(label.to_string(), y_values));
  }

  if series_list.is_empty() {
    return;
  }

  // X-axis labels: show 0%, 10%, 20%, ..., 100%
  let x_axis: Vec<String> = (0..num_points)
    .map(|i| {
      let percent = (i * 100) / (num_points - 1);
      if i % 10 == 0 {
        format!("{}%", percent)
      } else {
        String::new()
      }
    })
    .collect();

  let mut chart = LineChart::new_with_theme(series_list, x_axis, "light");
  chart.title_text = format!("{} - {}", title, y_label);
  chart.width = 1200.0;
  chart.height = 800.0;
  chart.legend_category = LegendCategory::Normal;
  // Add small margins around legend to prevent overlap with title
  chart.legend_margin = Some(ChartBox {
    top: chart.title_height + 10.0, // Margin above legend (below title)
    bottom: 5.0,
    ..Default::default()
  });

  let svg = chart.svg().unwrap();
  let webp_data = svg_to_webp(&svg).unwrap();
  fs::write(path, webp_data).unwrap();
}

fn slice_metrics(
  metrics: &[SystemMetricsSample],
  op_start: DateTime<Utc>,
  op_duration: f64,
) -> Vec<SystemMetricsSample> {
  let op_end = op_start + chrono::Duration::milliseconds((op_duration * 1000.0) as i64);
  metrics
    .iter()
    .filter(|m| m.timestamp >= op_start && m.timestamp <= op_end)
    .cloned()
    .collect()
}

fn generate_graphs(dataset: Dataset) {
  let base_dir = PathBuf::from(format!("graphs/{}b", dataset.object_size));
  fs::create_dir_all(&base_dir).unwrap();

  // Horizontal bar charts
  // 1. put_ops_per_second: (create + write + commit, or just write for rocksdb/fs)
  let put_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let write = r.op.write.as_ref()?;
      let total_secs = if r.should_skip_create() || r.should_skip_commit() {
        // For rocksdb/fs, just use write time
        write.exec_secs
      } else {
        // For others, use create + write + commit
        let create = r.op.create.as_ref()?;
        let commit = r.op.commit.as_ref()?;
        create.exec_secs + write.exec_secs + commit.exec_secs
      };
      Some((r.label(), r.objects as f64 / total_secs))
    })
    .collect();

  if !put_data.is_empty() {
    generate_horizontal_bar_chart(
      &base_dir.join("put_ops_per_second.webp"),
      "Put Operations per Second",
      put_data,
    );
  }

  // 2. read_ops_per_second
  let read_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let read = r.op.read.as_ref()?;
      Some((r.label(), r.objects as f64 / read.exec_secs))
    })
    .collect();

  generate_horizontal_bar_chart(
    &base_dir.join("read_ops_per_second.webp"),
    "Read Operations per Second",
    read_data,
  );

  // 3. inspect_ops_per_second
  let inspect_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let inspect = r.op.inspect.as_ref()?;
      Some((r.label(), r.objects as f64 / inspect.exec_secs))
    })
    .collect();

  generate_horizontal_bar_chart(
    &base_dir.join("inspect_ops_per_second.webp"),
    "Inspect Operations per Second",
    inspect_data,
  );

  // 4. delete_ops_per_second
  let delete_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let delete = r.op.delete.as_ref()?;
      Some((r.label(), r.objects as f64 / delete.exec_secs))
    })
    .collect();

  if !delete_data.is_empty() {
    generate_horizontal_bar_chart(
      &base_dir.join("delete_ops_per_second.webp"),
      "Delete Operations per Second",
      delete_data,
    );
  }

  // 5. write_mbs
  let write_mbs_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let write = r.op.write.as_ref()?;
      let total_bytes = r.objects * r.object_size;
      let mbs = total_bytes as f64 / write.exec_secs / 1024.0 / 1024.0;
      Some((r.label(), mbs))
    })
    .collect();

  if !write_mbs_data.is_empty() {
    generate_horizontal_bar_chart(
      &base_dir.join("write_mbs.webp"),
      "Write Throughput",
      write_mbs_data,
    );
  }

  // 6. read_mbs
  let read_mbs_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let read = r.op.read.as_ref()?;
      let total_bytes = r.objects * r.object_size;
      let mbs = total_bytes as f64 / read.exec_secs / 1024.0 / 1024.0;
      Some((r.label(), mbs))
    })
    .collect();

  generate_horizontal_bar_chart(
    &base_dir.join("read_mbs.webp"),
    "Read Throughput",
    read_mbs_data,
  );

  // 7. wait_for_end_secs
  let wait_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .map(|r| (r.label(), r.wait_for_end_secs))
    .collect();

  generate_horizontal_bar_chart(
    &base_dir.join("wait_for_end_secs.webp"),
    "Wait For End Duration",
    wait_data,
  );

  // 8. Total CPU seconds used (lower is better)
  let total_cpu_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let last_metric = r.system_metrics.last()?;
      let total_cpu = last_metric.cpu_user_secs + last_metric.cpu_system_secs;
      Some((r.label(), total_cpu))
    })
    .collect();

  generate_horizontal_bar_chart(
    &base_dir.join("total_cpu_secs.webp"),
    "Total CPU Seconds Used (lower is better)",
    total_cpu_data,
  );

  // 9. Total disk read MB (lower is better)
  let total_disk_read_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let last_metric = r.system_metrics.last()?;
      let total_mb = last_metric.disk_read_bytes as f64 / 1024.0 / 1024.0;
      Some((r.label(), total_mb))
    })
    .collect();

  generate_horizontal_bar_chart(
    &base_dir.join("total_disk_read_mb.webp"),
    "Total Disk Read MB (lower is better)",
    total_disk_read_data,
  );

  // 10. Total disk write MB (lower is better)
  let total_disk_write_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .filter_map(|r| {
      let last_metric = r.system_metrics.last()?;
      let total_mb = last_metric.disk_write_bytes as f64 / 1024.0 / 1024.0;
      Some((r.label(), total_mb))
    })
    .collect();

  generate_horizontal_bar_chart(
    &base_dir.join("total_disk_write_mb.webp"),
    "Total Disk Write MB (lower is better)",
    total_disk_write_data,
  );

  // 11. Max memory usage GB (lower is better)
  let max_memory_data: Vec<(String, f64)> = dataset
    .results
    .iter()
    .map(|r| {
      let max_memory = r
        .system_metrics
        .iter()
        .map(|m| m.memory_used_bytes)
        .max()
        .unwrap_or(0);
      let max_gb = max_memory as f64 / 1024.0 / 1024.0 / 1024.0;
      (r.label(), max_gb)
    })
    .collect();

  generate_horizontal_bar_chart(
    &base_dir.join("max_memory_gb.webp"),
    "Max Memory Usage GB (lower is better)",
    max_memory_data,
  );

  // Time-series charts for each operation
  let operations: Vec<(&str, Box<dyn Fn(&BenchmarkResults) -> Option<&OpResult>>)> = vec![
    (
      "create",
      Box::new(|r: &BenchmarkResults| r.op.create.as_ref()),
    ),
    (
      "write",
      Box::new(|r: &BenchmarkResults| r.op.write.as_ref()),
    ),
    (
      "commit",
      Box::new(|r: &BenchmarkResults| r.op.commit.as_ref()),
    ),
    (
      "inspect",
      Box::new(|r: &BenchmarkResults| r.op.inspect.as_ref()),
    ),
    ("read", Box::new(|r: &BenchmarkResults| r.op.read.as_ref())),
    (
      "delete",
      Box::new(|r: &BenchmarkResults| r.op.delete.as_ref()),
    ),
  ];

  for (op_name, op_getter) in &operations {
    // Filter based on operation and target type
    let filtered_results: Vec<&BenchmarkResults> = dataset
      .results
      .iter()
      .filter(|r| {
        if *op_name == "create" && r.should_skip_create() {
          return false;
        }
        if *op_name == "commit" && r.should_skip_commit() {
          return false;
        }
        true
      })
      .collect();

    // Check if any result has this operation
    if !filtered_results.iter().any(|r| op_getter(r).is_some()) {
      continue;
    }

    // Create operation subdirectory
    let op_dir = base_dir.join(op_name);
    fs::create_dir_all(&op_dir).unwrap();

    // CPU chart (cumulative CPU seconds, all cores combined)
    let cpu_series: Vec<(String, Vec<(f64, f64)>)> = filtered_results
      .iter()
      .filter_map(|r| {
        let op = op_getter(r)?;
        let metrics = slice_metrics(&r.system_metrics, op.started, op.exec_secs);
        let data: Vec<(f64, f64)> = metrics
          .iter()
          .map(|m| {
            let secs =
              (m.timestamp.timestamp_millis() - op.started.timestamp_millis()) as f64 / 1000.0;
            let cpu_secs = m.cpu_user_secs + m.cpu_system_secs;
            (secs, cpu_secs)
          })
          .collect();
        Some((r.label(), data))
      })
      .collect();

    generate_line_chart(
      &op_dir.join("cpu.webp"),
      &format!("{} - CPU Usage", op_name.to_uppercase()),
      cpu_series,
      "Cumulative CPU Seconds",
    );

    // Memory chart
    let memory_series: Vec<(String, Vec<(f64, f64)>)> = filtered_results
      .iter()
      .filter_map(|r| {
        let op = op_getter(r)?;
        let metrics = slice_metrics(&r.system_metrics, op.started, op.exec_secs);
        let data: Vec<(f64, f64)> = metrics
          .iter()
          .map(|m| {
            let secs =
              (m.timestamp.timestamp_millis() - op.started.timestamp_millis()) as f64 / 1000.0;
            let mem_gb = m.memory_used_bytes as f64 / 1024.0 / 1024.0 / 1024.0;
            (secs, mem_gb)
          })
          .collect();
        Some((r.label(), data))
      })
      .collect();

    generate_line_chart(
      &op_dir.join("memory.webp"),
      &format!("{} - Memory Usage", op_name.to_uppercase()),
      memory_series,
      "Memory (GB)",
    );

    // Disk read ops chart
    let disk_read_ops_series: Vec<(String, Vec<(f64, f64)>)> = filtered_results
      .iter()
      .filter_map(|r| {
        let op = op_getter(r)?;
        let metrics = slice_metrics(&r.system_metrics, op.started, op.exec_secs);
        let data: Vec<(f64, f64)> = metrics
          .iter()
          .map(|m| {
            let secs =
              (m.timestamp.timestamp_millis() - op.started.timestamp_millis()) as f64 / 1000.0;
            (secs, m.disk_read_ops as f64)
          })
          .collect();
        Some((r.label(), data))
      })
      .collect();

    generate_line_chart(
      &op_dir.join("disk_read_ops.webp"),
      &format!("{} - Disk Read Ops", op_name.to_uppercase()),
      disk_read_ops_series,
      "Cumulative Read Ops",
    );

    // Disk write ops chart
    let disk_write_ops_series: Vec<(String, Vec<(f64, f64)>)> = filtered_results
      .iter()
      .filter_map(|r| {
        let op = op_getter(r)?;
        let metrics = slice_metrics(&r.system_metrics, op.started, op.exec_secs);
        let data: Vec<(f64, f64)> = metrics
          .iter()
          .map(|m| {
            let secs =
              (m.timestamp.timestamp_millis() - op.started.timestamp_millis()) as f64 / 1000.0;
            (secs, m.disk_write_ops as f64)
          })
          .collect();
        Some((r.label(), data))
      })
      .collect();

    generate_line_chart(
      &op_dir.join("disk_write_ops.webp"),
      &format!("{} - Disk Write Ops", op_name.to_uppercase()),
      disk_write_ops_series,
      "Cumulative Write Ops",
    );

    // Disk read MB/s chart
    let disk_read_mbs_series: Vec<(String, Vec<(f64, f64)>)> = filtered_results
      .iter()
      .filter_map(|r| {
        let op = op_getter(r)?;
        let metrics = slice_metrics(&r.system_metrics, op.started, op.exec_secs);
        let data: Vec<(f64, f64)> = metrics
          .iter()
          .map(|m| {
            let secs =
              (m.timestamp.timestamp_millis() - op.started.timestamp_millis()) as f64 / 1000.0;
            let mbs = m.disk_read_bytes as f64 / 1024.0 / 1024.0;
            (secs, mbs)
          })
          .collect();
        Some((r.label(), data))
      })
      .collect();

    generate_line_chart(
      &op_dir.join("disk_read_mbs.webp"),
      &format!("{} - Disk Read Throughput", op_name.to_uppercase()),
      disk_read_mbs_series,
      "Cumulative Read MB",
    );

    // Disk write MB/s chart
    let disk_write_mbs_series: Vec<(String, Vec<(f64, f64)>)> = filtered_results
      .iter()
      .filter_map(|r| {
        let op = op_getter(r)?;
        let metrics = slice_metrics(&r.system_metrics, op.started, op.exec_secs);
        let data: Vec<(f64, f64)> = metrics
          .iter()
          .map(|m| {
            let secs =
              (m.timestamp.timestamp_millis() - op.started.timestamp_millis()) as f64 / 1000.0;
            let mbs = m.disk_write_bytes as f64 / 1024.0 / 1024.0;
            (secs, mbs)
          })
          .collect();
        Some((r.label(), data))
      })
      .collect();

    generate_line_chart(
      &op_dir.join("disk_write_mbs.webp"),
      &format!("{} - Disk Write Throughput", op_name.to_uppercase()),
      disk_write_mbs_series,
      "Cumulative Write MB",
    );

    // Disk in-flight chart
    let disk_in_flight_series: Vec<(String, Vec<(f64, f64)>)> = filtered_results
      .iter()
      .filter_map(|r| {
        let op = op_getter(r)?;
        let metrics = slice_metrics(&r.system_metrics, op.started, op.exec_secs);
        let data: Vec<(f64, f64)> = metrics
          .iter()
          .map(|m| {
            let secs =
              (m.timestamp.timestamp_millis() - op.started.timestamp_millis()) as f64 / 1000.0;
            (secs, m.disk_in_flight as f64)
          })
          .collect();
        Some((r.label(), data))
      })
      .collect();

    generate_line_chart(
      &op_dir.join("disk_in_flight.webp"),
      &format!("{} - Disk Queue Depth", op_name.to_uppercase()),
      disk_in_flight_series,
      "In-Flight Requests",
    );
  }

  println!(
    "Generated graphs for object_size={}b in {:?}",
    dataset.object_size, base_dir
  );
}

fn main() {
  let datasets = load_results();
  println!("Loaded {} datasets", datasets.len());

  for dataset in datasets {
    generate_graphs(dataset);
  }

  println!("All graphs generated successfully!");
}
