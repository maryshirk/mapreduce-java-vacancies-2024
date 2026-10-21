import json
from pathlib import Path
from statistics import median

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

root = Path(__file__).resolve().parent
trials = json.loads((root/'profiling/benchmark.json').read_text())
cases = ['pandas', 'mapreduce_1', 'mapreduce_4', 'duckdb_csv', 'duckdb_parquet']
times = [median(r['wall_seconds'] for r in trials if r['case'] == c) for c in cases]
memory = [median(r['peak_rss_bytes']/1024**2 for r in trials if r['case'] == c) for c in cases]
labels = ['pandas', 'MR 1', 'MR 4', 'DuckDB\nCSV', 'DuckDB\nParquet']
plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                     'axes.spines.top': False, 'axes.spines.right': False, 'pdf.fonttype': 42})
fig, axes = plt.subplots(1, 2, figsize=(10, 3.2))
for ax, values, title in zip(axes, [times, memory], ['Время, с', 'Пик RSS, МиБ']):
    bars = ax.bar(range(5), values, color='white', edgecolor='black',
                  hatch=['', '//', 'xx', '..', '\\\\'])
    ax.set_xticks(range(5), labels)
    ax.set_title(title)
    ax.set_ylim(0, max(values)*1.22)
    for bar, value in zip(bars, values):
        label = f'{value:.2f}' if title == 'Время, с' else f'{value:.0f}'
        ax.text(bar.get_x()+bar.get_width()/2, value+max(values)*.025,
                label, ha='center', fontsize=9)
fig.tight_layout()
(root/'images').mkdir(exist_ok=True)
fig.savefig(root/'images/performance.pdf', bbox_inches='tight')
plt.close(fig)
