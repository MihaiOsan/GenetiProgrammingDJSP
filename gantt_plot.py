import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

def plot_gantt(ms, schedule, num_machines, breakdowns, title="Gantt Chart", save_path=None):
    fig, ax = plt.subplots(figsize=(10, 6))

    # 1) Plot breakdown-urile
    for m in range(num_machines):
        if m in breakdowns:
            for (bd_start, bd_end) in breakdowns[m]:
                if bd_start <= ms:
                    bd_duration = bd_end - bd_start
                    ax.barh(m, bd_duration, left=bd_start, height=0.8, color='red',
                            alpha=0.3, edgecolor=None)

    # 2) Plot operațiile
    colors = plt.cm.get_cmap('tab10', 10)
    for (job_id, op_idx, machine_id, start, end) in schedule:
        duration = end - start
        ax.barh(machine_id, duration, left=start, color=colors(job_id % 10),
                edgecolor='black', height=0.6)
        ax.text(start + duration / 2, machine_id, f"J{job_id}-O{op_idx}",
                ha="center", va="center", color="white", fontsize=7)

    ax.set_xlabel("Time")
    ax.set_ylabel("Machine")
    ax.set_yticks(range(num_machines))
    ax.set_title(title)

    breakdown_patch = mpatches.Patch(color='red', alpha=0.3, label='Breakdown')
    ax.legend(handles=[breakdown_patch])
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150)
        plt.close()
    else:
        plt.show()
