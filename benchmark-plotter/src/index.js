// Benchmark Results Visualization JavaScript
// This file handles all interactive chart rendering and data manipulation

// Metric definitions: each metric has an id, display name, unit, type, and compute function
// The compute function extracts the metric value from a benchmark result object
// type: 'time' for time-based metrics, 'storage' for storage amounts, 'storage-rate' for throughput, 'ops' for operations
const metrics = [
  {
    id: 'put_ops_per_second',
    name: 'Put effective throughput',
    unit: 'MB/s',
    type: 'storage-rate',
    compute: (r) => {
      const write = r.op.write;
      if (!write) return null;
      // For systems with create/commit, use total time; for others (RocksDB/FS), just write time
      const totalSecs = (r.op.create && r.op.commit) 
        ? (r.op.create.exec_secs + write.exec_secs + r.op.commit.exec_secs)
        : write.exec_secs;
      // Calculate throughput: total bytes written / execution time (return bytes/sec)
      const totalBytes = r.objects * r.object_size;
      return totalBytes / totalSecs; // Return bytes/sec, formatter will convert to MB/s, GB/s, etc.
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
    id: 'total_cpu_secs',
    name: 'Total CPU seconds used',
    unit: 'seconds',
    type: 'time',
    compute: (r) => {
      if (!r.system_metrics || r.system_metrics.length === 0) return null;
      const last = r.system_metrics[r.system_metrics.length - 1];
      return last.cpu_user_secs + last.cpu_system_secs;
    }
  },
  {
    id: 'total_disk_read_mb',
    name: 'Total disk read',
    unit: 'MB',
    type: 'storage',
    compute: (r) => {
      if (!r.system_metrics || r.system_metrics.length === 0) return null;
      const last = r.system_metrics[r.system_metrics.length - 1];
      return last.disk_read_bytes; // Return bytes, formatter will convert to MB, GB, etc.
    }
  },
  {
    id: 'total_disk_write_mb',
    name: 'Total disk write',
    unit: 'MB',
    type: 'storage',
    compute: (r) => {
      if (!r.system_metrics || r.system_metrics.length === 0) return null;
      const last = r.system_metrics[r.system_metrics.length - 1];
      return last.disk_write_bytes; // Return bytes, formatter will convert to MB, GB, etc.
    }
  },
  {
    id: 'max_memory_gb',
    name: 'Maximum memory usage',
    unit: 'GB',
    type: 'storage',
    compute: (r) => {
      if (!r.system_metrics || r.system_metrics.length === 0) return null;
      const max = Math.max(...r.system_metrics.map(m => m.memory_used_bytes));
      return max; // Return bytes, formatter will convert to MB, GB, etc.
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
  if (metricType === 'time') return formatTimeAxis;
  return null; // Default formatting for ops/s and others
}

// Wait for DOM to be ready before accessing elements
document.addEventListener('DOMContentLoaded', () => {
  // Get DOM references
  const metricSelect = document.getElementById('metric');

  // Populate metric dropdown with available metrics
  metrics.forEach(metric => {
    const option = document.createElement('option');
    option.value = metric.id;
    option.textContent = metric.name;
    if (metrics.indexOf(metric) === 0) option.selected = true;
    metricSelect.appendChild(option);
  });

  // Update the main performance chart (line graph showing metric vs object size)
  function updateMainChart() {
    const metricSelect = document.getElementById('metric');
    const showFsCheckbox = document.getElementById('showFs');
    const selectedMetric = metrics.find(m => m.id === metricSelect.value);
    if (!selectedMetric) return;

    // Group results by benchmark name
    const benchmarks = new Set();
    data.forEach(d => d.results.forEach(r => benchmarks.add(r.benchmark_name)));
    let benchmarkNames = Array.from(benchmarks).sort();
    
    // Filter out file-system series if checkbox is unchecked
    if (!showFsCheckbox.checked) {
      const fsPatterns = ['ext4', 'xfs', 'btrfs', 'f2fs'];
      benchmarkNames = benchmarkNames.filter(name => {
        const lowerName = name.toLowerCase();
        return !fsPatterns.some(pattern => lowerName.includes(pattern));
      });
    }

    // Color palette for different benchmarks
    const colors = [
      '#2563eb', '#dc2626', '#16a34a', '#ea580c', 
      '#9333ea', '#0891b2', '#ca8a04', '#db2777'
    ];

    // Create a trace (line) for each benchmark
    const traces = benchmarkNames.map((benchmarkName, idx) => {
      const x = []; // Object sizes
      const y = []; // Metric values
      const sizes = data.map(d => d.object_size).sort((a, b) => a - b);

      // Collect data points for this benchmark across all object sizes
      sizes.forEach(size => {
        const dataset = data.find(d => d.object_size === size);
        if (!dataset) return;
        const result = dataset.results.find(r => r.benchmark_name === benchmarkName);
        if (!result) return;
        const value = selectedMetric.compute(result);
        if (value !== null && !isNaN(value)) {
          x.push(size);
          y.push(value);
        }
      });

      if (x.length === 0) return null;

      return {
        x: x,
        y: y,
        type: 'scatter',
        mode: 'lines+markers',
        name: benchmarkName,
        line: { width: 2.5, color: colors[idx % colors.length] },
        marker: { size: 8, color: colors[idx % colors.length] }
      };
    }).filter(t => t !== null);

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
      margin: { l: 80, r: 50, t: 80, b: 60 },
      paper_bgcolor: 'white',
      plot_bgcolor: 'white',
      font: { family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif' },
      legend: { x: 1.02, y: 1 }
    };

    Plotly.newPlot('chart', traces, layout, {
      responsive: true,
      displayModeBar: true,
      modeBarButtonsToRemove: ['lasso2d', 'select2d']
    });
  }

  // Set up event listeners
  metricSelect.addEventListener('change', updateMainChart);
  document.getElementById('showFs').addEventListener('change', updateMainChart);

  // Initial render
  updateMainChart();
});
