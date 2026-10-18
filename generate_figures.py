import io
import matplotlib.pyplot as plt
import pandas as pd

raw_lat = """Mechanism,MessageBytes,Median_us,P99_us
Pipe,64,100.33,283.89
Pipe,256,102.06,307.80
Pipe,1024,89.83,290.33
Pipe,4096,83.95,273.51
Pipe,16384,88.85,292.48
Pipe,65536,108.76,435.52
Pipe,262144,696.84,1893.98
Pipe,1048576,3175.33,6512.73
UnixSocket,64,86.56,358.18
UnixSocket,256,85.50,354.51
UnixSocket,1024,86.23,331.35
UnixSocket,4096,87.26,333.18
UnixSocket,16384,77.44,305.62
UnixSocket,65536,95.52,471.07
UnixSocket,262144,240.42,869.37
UnixSocket,1048576,657.64,2416.52
TCP_Loopback,64,103.62,303.03
TCP_Loopback,256,103.87,321.28
TCP_Loopback,1024,104.46,290.81
TCP_Loopback,4096,107.58,370.49
TCP_Loopback,16384,110.46,348.03
TCP_Loopback,65536,139.38,487.51
TCP_Loopback,262144,164.48,556.81
TCP_Loopback,1048576,453.49,1644.38
SharedMem,64,86.22,218.94
SharedMem,256,87.46,200.88
SharedMem,1024,86.73,193.97
SharedMem,4096,88.18,195.76
SharedMem,16384,89.02,232.82
SharedMem,65536,85.74,232.52
SharedMem,262144,101.52,319.09
SharedMem,1048576,212.76,749.53"""

raw_tp = """Mechanism,ChunkBytes,Throughput_MBs
Pipe,64,121.74
Pipe,256,492.87
Pipe,1024,1683.70
Pipe,4096,5293.27
Pipe,16384,4044.15
Pipe,65536,1115.73
Pipe,262144,639.83
Pipe,1048576,688.93
UnixSocket,64,56.64
UnixSocket,256,221.99
UnixSocket,1024,865.60
UnixSocket,4096,2033.46
UnixSocket,16384,5927.65
UnixSocket,65536,7898.83
UnixSocket,262144,8556.89
UnixSocket,1048576,6293.35
TCP_Loopback,64,8.60
TCP_Loopback,256,33.32
TCP_Loopback,1024,126.02
TCP_Loopback,4096,453.15
TCP_Loopback,16384,1623.34
TCP_Loopback,65536,2097.75
TCP_Loopback,262144,3373.35
TCP_Loopback,1048576,4553.85
SharedMem,64,1.24
SharedMem,256,4.96
SharedMem,1024,18.71
SharedMem,4096,59.41
SharedMem,16384,205.57
SharedMem,65536,834.92
SharedMem,262144,2952.04
SharedMem,1048576,4181.40"""

df_lat = pd.read_csv(io.StringIO(raw_lat))
df_tp = pd.read_csv(io.StringIO(raw_tp))

mechanisms = ['SharedMem', 'UnixSocket', 'TCP_Loopback', 'Pipe']
colors = {
    'SharedMem': '#1f77b4',
    'UnixSocket': '#2ca02c',
    'TCP_Loopback': '#ff7f0e',
    'Pipe': '#d62728',
}
markers = {'SharedMem': 'o', 'UnixSocket': 's', 'TCP_Loopback': '^', 'Pipe': 'D'}

# 1. Latency Plot
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5), dpi=300)
for m in mechanisms:
  s = df_lat[df_lat['Mechanism'] == m]
  ax1.plot(
      s['MessageBytes'],
      s['Median_us'],
      marker=markers[m],
      color=colors[m],
      label=m,
      linewidth=1.8,
  )
  ax2.plot(
      s['MessageBytes'],
      s['P99_us'],
      marker=markers[m],
      color=colors[m],
      label=m,
      linewidth=1.8,
  )

for ax in (ax1, ax2):
  ax.set_xscale('log', base=2)
  ax.set_yscale('log')
  ax.set_xlabel('Message Payload (Bytes)')
  ax.grid(True, which='both', linestyle='--', alpha=0.6)
  ax.set_xticks([64, 256, 1024, 4096, 16384, 65536, 262144, 1048576])
  ax.set_xticklabels(
      ['64B', '256B', '1KB', '4KB', '16KB', '64KB', '256KB', '1MB'],
      rotation=45,
  )

ax1.set_ylabel('Median Latency (µs)')
ax1.set_title('(a) Median Latency (50th Percentile)')
ax1.legend(loc='upper left')

ax2.set_ylabel('P99 Tail Latency (µs)')
ax2.set_title('(b) Tail Latency (99th Percentile)')
ax2.legend(loc='upper left')

plt.tight_layout()
plt.savefig('latency_benchmark_curves.png')
plt.close()

# 2. Throughput Plot
fig, ax = plt.subplots(figsize=(7.5, 4.5), dpi=300)
for m in mechanisms:
  s = df_tp[df_tp['Mechanism'] == m]
  ax.plot(
      s['ChunkBytes'],
      s['Throughput_MBs'],
      marker=markers[m],
      color=colors[m],
      label=m,
      linewidth=1.8,
  )

ax.set_xscale('log', base=2)
ax.set_yscale('log')
ax.set_xlabel('Chunk Payload Size (Bytes)')
ax.set_ylabel('Streaming Throughput (MB/s)')
ax.set_title('IPC Streaming Throughput Comparison (Log-Log Scale)')
ax.grid(True, which='both', linestyle='--', alpha=0.6)
ax.set_xticks([64, 256, 1024, 4096, 16384, 65536, 262144, 1048576])
ax.set_xticklabels(
    ['64B', '256B', '1KB', '4KB', '16KB', '64KB', '256KB', '1MB'], rotation=45
)
ax.legend(loc='upper left')

plt.tight_layout()
plt.savefig('throughput_benchmark_curve.png')
plt.close()
print('Plots generated successfully!')