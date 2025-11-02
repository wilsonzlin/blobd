// Benchmark Results Visualization JavaScript
// This file handles all interactive chart rendering and data manipulation
//
// URL Hash Parameters:
//   - metric=<metric_id>       Select which metric to display (e.g., #metric=read_ops_per_second)
//                              If not specified, defaults to the first metric in the list
//   - hideControls=true        Hide the metric dropdown selector
//                              By default, controls are always visible
//   - showFs=true              Show file system benchmarks (ext4, xfs, btrfs, f2fs)
//                              By default, file systems are hidden
//
// Examples:
//   - #metric=read_latency                      Show read latency with dropdown visible, no file systems
//   - #metric=write_mbs&hideControls=true       Show write throughput without dropdown (minimal view)
//   - #metric=read_ops_per_second&showFs=true   Show read ops/sec including file systems
//   - (no hash)                                 Show first metric with dropdown visible, no file systems

// Metric definitions: each metric has an id, display name, unit, type, and compute function
// The compute function extracts the metric value from a benchmark result object
// type: 'time' for time-based metrics, 'storage' for storage amounts, 'storage-rate' for throughput, 'ops' for operations
const metrics = [
  {
    id: 'create_ops_per_second',
    name: 'Create operations per second',
    unit: 'ops/s',
    type: 'ops',
    compute: (r) => {
      if (!r.op.create) return null;
      return r.objects / r.op.create.exec_secs;
    }
  },
  {
    id: 'write_ops_per_second',
    name: 'Write operations per second',
    unit: 'ops/s',
    type: 'ops',
    compute: (r) => {
      if (!r.op.write) return null;
      return r.objects / r.op.write.exec_secs;
    }
  },
  {
    id: 'commit_ops_per_second',
    name: 'Commit operations per second',
    unit: 'ops/s',
    type: 'ops',
    compute: (r) => {
      if (!r.op.commit) return null;
      return r.objects / r.op.commit.exec_secs;
    }
  },
  {
    id: 'read_ops_per_second',
    name: 'Read operations per second',
    unit: 'ops/s',
    type: 'ops',
    compute: (r) => {
      if (!r.op.read) return null;
      return r.objects / r.op.read.exec_secs;
    }
  },
  {
    id: 'inspect_ops_per_second',
    name: 'Inspect operations per second',
    unit: 'ops/s',
    type: 'ops',
    compute: (r) => {
      if (!r.op.inspect) return null;
      return r.objects / r.op.inspect.exec_secs;
    }
  },
  {
    id: 'delete_ops_per_second',
    name: 'Delete operations per second',
    unit: 'ops/s',
    type: 'ops',
    compute: (r) => {
      if (!r.op.delete) return null;
      return r.objects / r.op.delete.exec_secs;
    }
  },
  {
    id: 'write_mbs',
    name: 'Write throughput',
    unit: 'MB/s',
    type: 'storage-rate',
    compute: (r) => {
      if (!r.op.write) return null;
      // Calculate throughput: total bytes written / execution time (return bytes/sec)
      const totalBytes = r.objects * r.object_size;
      return totalBytes / r.op.write.exec_secs; // Return bytes/sec, formatter will convert to MB/s, GB/s, etc.
    }
  },
  {
    id: 'read_mbs',
    name: 'Read throughput',
    unit: 'MB/s',
    type: 'storage-rate',
    compute: (r) => {
      if (!r.op.read) return null;
      // Calculate throughput: total bytes read / execution time (return bytes/sec)
      const totalBytes = r.objects * r.object_size;
      return totalBytes / r.op.read.exec_secs; // Return bytes/sec, formatter will convert to MB/s, GB/s, etc.
    }
  },
  {
    id: 'wait_for_end_secs',
    name: 'Wait for end duration',
    unit: 'seconds',
    type: 'time',
    compute: (r) => r.wait_for_end_secs
  },
  {
    id: 'random_read_ops_per_second',
    name: 'Random read operations per second',
    unit: 'ops/s',
    type: 'ops',
    compute: (r) => {
      if (!r.op.random_read) return null;
      return r.objects / r.op.random_read.exec_secs;
    }
  },
  {
    id: 'random_read_mbs',
    name: 'Random read throughput',
    unit: 'MB/s',
    type: 'storage-rate',
    compute: (r) => {
      if (!r.op.random_read) return null;
      // Random reads only read 4000 bytes per object
      const bytesPerRead = Math.min(4000, r.object_size);
      const totalBytes = r.objects * bytesPerRead;
      return totalBytes / r.op.random_read.exec_secs;
    }
  },
  {
    id: 'read_ttfb',
    name: 'Read TTFB (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.read || !r.op.read.ttfb) return null;
      return {
        avg: r.op.read.ttfb.avg_ms / 1000, // Convert ms to seconds
        p99: r.op.read.ttfb.p99_ms / 1000
      };
    }
  },
  {
    id: 'random_read_ttfb',
    name: 'Random read TTFB (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.random_read || !r.op.random_read.ttfb) return null;
      return {
        avg: r.op.random_read.ttfb.avg_ms / 1000, // Convert ms to seconds
        p99: r.op.random_read.ttfb.p99_ms / 1000
      };
    }
  },
  {
    id: 'total_cpu_secs',
    name: 'Total CPU seconds used',
    unit: 'seconds',
    type: 'time',
    compute: (r) => {
      if (!r.system_metrics) return null;
      return r.system_metrics.total_cpu_user_secs + r.system_metrics.total_cpu_system_secs;
    }
  },
  {
    id: 'total_disk_read_mb',
    name: 'Total disk read',
    unit: 'MB',
    type: 'storage',
    compute: (r) => {
      if (!r.system_metrics) return null;
      return r.system_metrics.total_disk_read_bytes; // Return bytes, formatter will convert to MB, GB, etc.
    }
  },
  {
    id: 'total_disk_write_mb',
    name: 'Total disk write',
    unit: 'MB',
    type: 'storage',
    compute: (r) => {
      if (!r.system_metrics) return null;
      return r.system_metrics.total_disk_write_bytes; // Return bytes, formatter will convert to MB, GB, etc.
    }
  },
  {
    id: 'total_disk_read_ops',
    name: 'Total disk read operations',
    unit: 'ops',
    type: 'ops',
    compute: (r) => {
      if (!r.system_metrics) return null;
      return r.system_metrics.total_disk_read_ops;
    }
  },
  {
    id: 'total_disk_write_ops',
    name: 'Total disk write operations',
    unit: 'ops',
    type: 'ops',
    compute: (r) => {
      if (!r.system_metrics) return null;
      return r.system_metrics.total_disk_write_ops;
    }
  },
  {
    id: 'max_memory_gb',
    name: 'Peak memory usage',
    unit: 'GB',
    type: 'storage',
    compute: (r) => {
      if (!r.system_metrics) return null;
      return r.system_metrics.peak_memory_bytes; // Return bytes, formatter will convert to MB, GB, etc.
    }
  },
  // Latency metrics with spread visualization
  {
    id: 'create_latency',
    name: 'Create latency (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.create || !r.op.create.latency) return null;
      return {
        avg: r.op.create.latency.avg_ms / 1000, // Convert to seconds
        p99: r.op.create.latency.p99_ms / 1000
      };
    }
  },
  {
    id: 'write_latency',
    name: 'Write latency (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.write || !r.op.write.latency) return null;
      return {
        avg: r.op.write.latency.avg_ms / 1000,
        p99: r.op.write.latency.p99_ms / 1000
      };
    }
  },
  {
    id: 'commit_latency',
    name: 'Commit latency (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.commit || !r.op.commit.latency) return null;
      return {
        avg: r.op.commit.latency.avg_ms / 1000,
        p99: r.op.commit.latency.p99_ms / 1000
      };
    }
  },
  {
    id: 'inspect_latency',
    name: 'Inspect latency (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.inspect || !r.op.inspect.latency) return null;
      return {
        avg: r.op.inspect.latency.avg_ms / 1000,
        p99: r.op.inspect.latency.p99_ms / 1000
      };
    }
  },
  {
    id: 'random_read_latency',
    name: 'Random read latency (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.random_read || !r.op.random_read.latency) return null;
      return {
        avg: r.op.random_read.latency.avg_ms / 1000,
        p99: r.op.random_read.latency.p99_ms / 1000
      };
    }
  },
  {
    id: 'read_latency',
    name: 'Read latency (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.read || !r.op.read.latency) return null;
      return {
        avg: r.op.read.latency.avg_ms / 1000,
        p99: r.op.read.latency.p99_ms / 1000
      };
    }
  },
  {
    id: 'delete_latency',
    name: 'Delete latency (avg to p99 spread)',
    unit: 'ms',
    type: 'latency',
    compute: (r) => {
      if (!r.op.delete || !r.op.delete.latency) return null;
      return {
        avg: r.op.delete.latency.avg_ms / 1000,
        p99: r.op.delete.latency.p99_ms / 1000
      };
    }
  }
];


// Format bytes to human-readable string (GB, MB, KB, B)
function formatBytes(bytes) {
  if (bytes >= 1e9) return (bytes / 1e9).toFixed(1) + ' GB';
  if (bytes >= 1e6) return (bytes / 1e6).toFixed(1) + ' MB';
  if (bytes >= 1e3) return (bytes / 1e3).toFixed(1) + ' KB';
  return bytes + ' B';
}

// Format time in seconds to human-readable string (e.g., "1h 30m", "45s", "2h 15m 30s")
function formatTime(seconds) {
  if (seconds < 60) {
    return seconds.toFixed(1) + 's';
  }
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  
  const parts = [];
  if (hours > 0) parts.push(hours + 'h');
  if (minutes > 0) parts.push(minutes + 'm');
  if (secs > 0 && hours === 0) parts.push(secs.toFixed(1) + 's');
  
  return parts.join(' ') || '0s';
}

// Format bytes for axis ticks (for storage metrics)
function formatBytesAxis(value) {
  if (value >= 1e9) return (value / 1e9).toFixed(1) + ' GB';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + ' MB';
  if (value >= 1e3) return (value / 1e3).toFixed(1) + ' KB';
  return value.toFixed(0) + ' B';
}

// Format bytes per second for axis ticks (for storage-rate metrics)
function formatBytesPerSecAxis(value) {
  if (value >= 1e9) return (value / 1e9).toFixed(1) + ' GB/s';
  if (value >= 1e6) return (value / 1e6).toFixed(1) + ' MB/s';
  if (value >= 1e3) return (value / 1e3).toFixed(1) + ' KB/s';
  return value.toFixed(1) + ' B/s';
}

// Format time for axis ticks
function formatTimeAxis(value) {
  if (value < 60) {
    return value.toFixed(1) + 's';
  }
  const hours = Math.floor(value / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  const secs = value % 60;
  
  const parts = [];
  if (hours > 0) parts.push(hours + 'h');
  if (minutes > 0) parts.push(minutes + 'm');
  if (secs > 0 && hours === 0) parts.push(secs.toFixed(0) + 's');
  
  return parts.join(' ') || '0s';
}

// Get tick formatter function based on metric type
function getTickFormatter(metricType) {
  if (metricType === 'storage') return formatBytesAxis;
  if (metricType === 'storage-rate') return formatBytesPerSecAxis;
  if (metricType === 'time' || metricType === 'latency') return formatTimeAxis;
  return null; // Default formatting for ops/s and others
}

// Parse URL hash parameters (e.g., #metric=read_ops_per_second&showFs=true)
function parseHashParams() {
  const params = {};
  const hash = window.location.hash.substring(1); // Remove the '#'
  if (hash) {
    hash.split('&').forEach(param => {
      const [key, value] = param.split('=');
      if (key) params[key] = decodeURIComponent(value || '');
    });
  }
  return params;
}

// Update URL hash with current selection
function updateHash(metric) {
  if (metric) {
    window.location.hash = `metric=${encodeURIComponent(metric)}`;
  }
}

// Wait for DOM to be ready before accessing elements
document.addEventListener('DOMContentLoaded', () => {
  // Get DOM references
  const metricSelect = document.getElementById('metric');
  const controlsDiv = document.getElementById('controls');

  // Parse hash parameters
  const hashParams = parseHashParams();
  const initialMetric = hashParams.metric || metrics[0].id;
  const hideControls = hashParams.hideControls === 'true';
  const showFs = hashParams.showFs === 'true';

  // Hide controls if hideControls hash parameter is set
  if (hideControls) {
    controlsDiv.style.display = 'none';
  }

  // Populate metric dropdown with available metrics
  metrics.forEach(metric => {
    const option = document.createElement('option');
    option.value = metric.id;
    option.textContent = metric.name;
    option.selected = metric.id === initialMetric;
    metricSelect.appendChild(option);
  });

  // Update the main performance chart (line graph showing metric vs object size)
  function updateMainChart() {
    const metricSelect = document.getElementById('metric');
    const selectedMetric = metrics.find(m => m.id === metricSelect.value);
    if (!selectedMetric) return;

    // Group results by benchmark name
    const benchmarks = new Set();
    data.forEach(d => d.results.forEach(r => benchmarks.add(r.benchmark_name)));
    let benchmarkNames = Array.from(benchmarks).sort();
    
    // Filter out file-system benchmarks unless showFs is true
    if (!showFs) {
      const fsPatterns = ['ext4', 'xfs', 'btrfs', 'f2fs'];
      benchmarkNames = benchmarkNames.filter(name => {
        const lowerName = name.toLowerCase();
        return !fsPatterns.some(pattern => lowerName.includes(pattern));
      });
    }

    // Filter out benchmarks that don't support the operation
    // Create operation: skip RocksDB
    // Commit operation: skip RocksDB and FS
    const createMetrics = ['create_ops_per_second', 'create_latency'];
    const commitMetrics = ['commit_ops_per_second', 'commit_latency'];
    
    if (createMetrics.includes(selectedMetric.id) || commitMetrics.includes(selectedMetric.id)) {
      benchmarkNames = benchmarkNames.filter(benchmarkName => {
        // Find any result with this benchmark name to check its target type
        const sampleResult = data
          .flatMap(d => d.results)
          .find(r => r.benchmark_name === benchmarkName);
        
        if (!sampleResult) return true;
        
        const targetType = sampleResult.cfg.target;
        
        // Filter based on metric type
        if (createMetrics.includes(selectedMetric.id)) {
          // RocksDB doesn't have create operation
          return targetType !== 'RocksDB';
        }
        if (commitMetrics.includes(selectedMetric.id)) {
          // RocksDB and FS don't have commit operation
          return targetType !== 'RocksDB' && targetType !== 'FS';
        }
        
        return true;
      });
    }

    // Color palette for different benchmarks
    const colors = [
      '#2563eb', '#dc2626', '#16a34a', '#ea580c', 
      '#9333ea', '#0891b2', '#ca8a04', '#db2777'
    ];

    // Create a trace (line) for each benchmark
    const traces = [];
    const isLatencyMetric = selectedMetric.type === 'latency';
    
    benchmarkNames.forEach((benchmarkName, idx) => {
      const x = []; // Object sizes
      const y_avg = []; // Average values (for latency) or regular values
      const y_p99 = []; // P99 values (for latency only)
      const sizes = data.map(d => d.object_size).sort((a, b) => a - b);

      // Collect data points for this benchmark across all object sizes
      sizes.forEach(size => {
        const dataset = data.find(d => d.object_size === size);
        if (!dataset) return;
        const result = dataset.results.find(r => r.benchmark_name === benchmarkName);
        if (!result) return;
        const value = selectedMetric.compute(result);
        
        if (isLatencyMetric && value !== null && value.avg !== undefined && value.p99 !== undefined) {
          // Latency metric with spread
          x.push(size);
          y_avg.push(value.avg);
          y_p99.push(value.p99);
        } else if (!isLatencyMetric && value !== null && !isNaN(value)) {
          // Regular metric
          x.push(size);
          y_avg.push(value);
        }
      });

      if (x.length === 0) return;

      const color = colors[idx % colors.length];
      
      if (isLatencyMetric) {
        // Create two traces: one for p99 (upper bound) and one for avg (lower bound) with fill
        // Convert hex color to rgba for fill
        let fillColor = color;
        if (color.startsWith('#')) {
          const r = parseInt(color.slice(1, 3), 16);
          const g = parseInt(color.slice(3, 5), 16);
          const b = parseInt(color.slice(5, 7), 16);
          fillColor = `rgba(${r}, ${g}, ${b}, 0.2)`;
        } else if (color.startsWith('rgb')) {
          fillColor = color.replace(')', ', 0.2)').replace('rgb', 'rgba');
        }
        
        // Upper bound (p99) - invisible line
        traces.push({
          x: x,
          y: y_p99,
          type: 'scatter',
          mode: 'lines',
          name: benchmarkName + ' (p99)',
          showlegend: false,
          line: { width: 0 },
          hoverinfo: 'skip'
        });
        
        // Lower bound (avg) - visible line with fill to previous trace
        traces.push({
          x: x,
          y: y_avg,
          type: 'scatter',
          mode: 'lines+markers',
          name: benchmarkName,
          line: { width: 2.5, color: color },
          marker: { size: 8, color: color },
          fill: 'tonexty',
          fillcolor: fillColor,
          customdata: y_p99.map((p99, i) => [p99, y_avg[i]]),
          hovertemplate: '<b>%{fullData.name}</b><br>Object size: %{x}<br>Avg: %{customdata[1]:.4f}s<br>P99: %{customdata[0]:.4f}s<extra></extra>'
        });
      } else {
        // Regular metric - single line
        traces.push({
          x: x,
          y: y_avg,
          type: 'scatter',
          mode: 'lines+markers',
          name: benchmarkName,
          line: { width: 2.5, color: color },
          marker: { size: 8, color: color }
        });
      }
    });

    // Format X-axis (object size) ticks as bytes
    const allXValues = traces.flatMap(t => t.x).filter(v => !isNaN(v) && isFinite(v));
    let xAxisConfig = {
      title: 'Object size',
      type: 'log', // Logarithmic scale for object sizes
      titlefont: { size: 14, color: '#6e6e73' },
      tickfont: { size: 12, color: '#6e6e73' },
      gridcolor: '#f5f5f7',
      showgrid: true
    };
    
    if (allXValues.length > 0) {
      const minX = Math.min(...allXValues);
      const maxX = Math.max(...allXValues);
      // Generate tick values on log scale
      const numTicks = 6;
      const logMin = Math.log10(minX);
      const logMax = Math.log10(maxX);
      const logStep = (logMax - logMin) / (numTicks - 1);
      
      const tickvals = [];
      const ticktext = [];
      for (let i = 0; i < numTicks; i++) {
        const logValue = logMin + i * logStep;
        const tickValue = Math.pow(10, logValue);
        tickvals.push(tickValue);
        ticktext.push(formatBytesAxis(tickValue));
      }
      
      xAxisConfig.tickmode = 'array';
      xAxisConfig.tickvals = tickvals;
      xAxisConfig.ticktext = ticktext;
    }

    // Calculate Y-axis tick formatting
    const yTickFormatter = getTickFormatter(selectedMetric.type);
    let yAxisConfig = {
      title: '', // No title - values are obvious from formatted ticks
      titlefont: { size: 14, color: '#6e6e73' },
      tickfont: { size: 12, color: '#6e6e73' },
      gridcolor: '#f5f5f7',
      showgrid: true
    };
    
    // If we have a custom formatter, calculate tick values and labels
    if (yTickFormatter) {
      // Get all Y values to determine range
      const allYValues = traces.flatMap(t => t.y).filter(v => !isNaN(v) && isFinite(v));
      if (allYValues.length > 0) {
        const minY = Math.min(...allYValues);
        const maxY = Math.max(...allYValues);
        const range = maxY - minY;
        const numTicks = 5;
        const tickStep = range / (numTicks - 1);
        
        const tickvals = [];
        const ticktext = [];
        for (let i = 0; i < numTicks; i++) {
          const tickValue = minY + i * tickStep;
          tickvals.push(tickValue);
          ticktext.push(yTickFormatter(tickValue));
        }
        
        yAxisConfig.tickmode = 'array';
        yAxisConfig.tickvals = tickvals;
        yAxisConfig.ticktext = ticktext;
      }
    }

    const layout = {
      title: {
        text: selectedMetric.name,
        font: { size: 24, color: '#1d1d1f' }
      },
      xaxis: xAxisConfig,
      yaxis: yAxisConfig,
      margin: { l: 80, r: 50, t: 80, b: 120 },
      paper_bgcolor: 'white',
      plot_bgcolor: 'white',
      font: { family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif' },
      legend: { 
        orientation: 'h',
        x: 0.5,
        xanchor: 'center',
        y: -0.15,
        yanchor: 'top'
      }
    };

    Plotly.newPlot('chart', traces, layout, {
      responsive: true,
      displayModeBar: true,
      modeBarButtonsToRemove: ['lasso2d', 'select2d']
    });
  }

  // Set up event listeners
  metricSelect.addEventListener('change', () => {
    updateHash(metricSelect.value);
    updateMainChart();
  });

  // Initial render
  updateMainChart();
});
