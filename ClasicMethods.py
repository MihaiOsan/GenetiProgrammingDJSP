import os
import copy
import random
from typing import List, Dict, Tuple, Any
import matplotlib.pyplot as plt
import time

import matplotlib.patches as mpatches

###############################################################################
# 1) Citire instanță FJSP (dinamic)
###############################################################################

def read_dynamic_fjsp_instance(file_path: str):
    """Parsează un fișier DFJSP dinamic și întoarce: num_jobs, num_machines, jobs, events."""
    with open(file_path, "r") as f:
        lines = f.readlines()

    num_jobs, num_machines = map(int, lines[0].split())
    jobs: List[List[List[Tuple[int, int]]]] = []
    events: Dict[str, Any] = {"breakdowns": {}, "added_jobs": [], "cancelled_jobs": []}

    parsing_jobs = True
    parsing_breakdowns = parsing_added_jobs = parsing_cancelled_jobs = False

    for line in lines[1:]:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("Dynamic Events"):
            parsing_jobs = False
            continue
        if "Machine Breakdowns" in line:
            parsing_breakdowns, parsing_added_jobs, parsing_cancelled_jobs = True, False, False
            continue
        if "Added Jobs" in line:
            parsing_breakdowns, parsing_added_jobs, parsing_cancelled_jobs = False, True, False
            continue
        if "Cancelled Jobs" in line:
            parsing_breakdowns, parsing_added_jobs, parsing_cancelled_jobs = False, False, True
            continue

        if parsing_jobs:
            data = list(map(int, line.split()))
            num_ops, idx = data[0], 1
            job_ops: List = []
            for _ in range(num_ops):
                num_alts = data[idx]; idx += 1
                alts = []
                for _ in range(num_alts):
                    m, p = data[idx], data[idx+1]
                    idx += 2
                    alts.append((m, p))
                job_ops.append(alts)
            jobs.append(job_ops)
        elif parsing_breakdowns:
            m, s_bd, e_bd = map(int, line.split())
            events["breakdowns"].setdefault(m, []).append((s_bd, e_bd))
        elif parsing_added_jobs:
            t_part, j_part = line.split(":")
            t_add = int(t_part)
            vals = list(map(int, j_part.split()))
            num_ops, idx = vals[0], 1
            new_job = []
            for _ in range(num_ops):
                num_alts = vals[idx]; idx += 1
                alts = []
                for _ in range(num_alts):
                    m, p = vals[idx], vals[idx+1]
                    idx += 2
                    alts.append((m, p))
                new_job.append(alts)
            events["added_jobs"].append((t_add, new_job))
        elif parsing_cancelled_jobs:
            t_c, j_id = map(int, line.split())
            events["cancelled_jobs"].append((t_c, j_id))

    return num_jobs, num_machines, jobs, events

###############################################################################
# 2) Metrici
###############################################################################

def metric_makespan(schedule: List[Tuple[int, int, int, int, int]]) -> int:
    return max(f for *_rest, f in schedule) if schedule else 0


def metric_job_completion_times(schedule: List[Tuple[int, int, int, int, int]], n_jobs: int) -> List[int]:
    comp = [0]*n_jobs
    for j, *_r, f in schedule:
        comp[j] = max(comp[j], f)
    return comp


def metric_earliest_completion_time(schedule: List[Tuple[int, int, int, int, int]], n_jobs: int) -> int:
    comps = metric_job_completion_times(schedule, n_jobs)
    return min(comps) if comps else 0


def metric_append(store: Dict[str, List[float]], rule: str, value: float):
    store.setdefault(rule, []).append(value)


def metric_average(store: Dict[str, List[float]]) -> Dict[str, float]:
    return {r: (sum(v)/len(v) if v else 0.0) for r, v in store.items()}

###############################################################################
# 3) Regulile de prioritizare / tie‑break
###############################################################################

def remaining_processing_time(jobs, j: int, cur_op: int) -> int:
    return sum(min(p for _m, p in op) for op in jobs[j][cur_op:])


def compute_priority(rule: str,
                     job_id: int,
                     op_idx: int,
                     machine: int,
                     ptime: int,
                     cur_time: int,
                     *,
                     jobs,
                     job_progress,
                     arrival_times,
                     due_dates,
                     machine_loads: Dict[int, int]) -> float:
    """Întoarce prioritatea numerică (mai mic = mai prioritar)."""
    if rule == "SPT":
        return ptime
    if rule == "LPT":
        return -ptime
    if rule == "EDD":
        return due_dates[job_id] - cur_time
    if rule == "FIFO":
        return arrival_times[job_id]
    if rule == "LIFO":
        return -arrival_times[job_id]
    if rule == "SRPT":
        return remaining_processing_time(jobs, job_id, op_idx)
    if rule == "OPR":
        return len(jobs[job_id]) - op_idx
    if rule == "ECT":
        remain_after = remaining_processing_time(jobs, job_id, op_idx+1)
        return cur_time + ptime + remain_after
    if rule == "LLM":
        return machine_loads[machine]
    if rule == "Random":
        return random.random()
    return ptime

###############################################################################
# 4) Simulare incrementală (time += 1) – fără paralelism pe același job
###############################################################################

def schedule_dynamic_no_parallel(jobs: List,
                                 n_machines: int,
                                 events: Dict,
                                 rule: str):
    bds_per_machine = {m: sorted(events["breakdowns"].get(m, []), key=lambda x: x[0]) for m in range(n_machines)}
    job_progress = [0]*len(jobs)
    job_current_machine = [None]*len(jobs)
    due_dates = [100*(j+1) for j in range(len(jobs))]
    active_ops: Dict[int, Tuple] = {m: None for m in range(n_machines)}  # (job, opidx, start, remaining)
    job_earliest_start = [0]*len(jobs)
    arrival_times = [0]*len(jobs)
    schedule: List[Tuple[int,int,int,int,int]] = []
    t = 0

    def in_breakdown(machine: int, time: int) -> bool:
        return any(s <= time < e for s, e in bds_per_machine[machine])

    while any(job_progress[j] < len(jobs[j]) for j in range(len(jobs))):
        # Added jobs
        for t_add, new_job in events["added_jobs"]:
            if t_add == t:
                j_id = len(jobs)
                jobs.append(new_job)
                job_progress.append(0)
                job_current_machine.append(None)
                due_dates.append(100*(j_id+1))
                job_earliest_start.append(t)
                arrival_times.append(t)
        # Cancelled jobs
        for t_c, j_c in events["cancelled_jobs"]:
            if t_c == t and j_c < len(job_progress):
                job_progress[j_c] = len(jobs[j_c])
                if job_current_machine[j_c] is not None:
                    mstop = job_current_machine[j_c]
                    active_ops[mstop] = None
                    job_current_machine[j_c] = None
        # Start breakdown – drop running op
        for m in range(n_machines):
            if any(s == t for s, _ in bds_per_machine[m]) and active_ops[m] is not None:
                j_b, *_ = active_ops[m]
                active_ops[m] = None
                job_current_machine[j_b] = None
        # Advance running operations
        for m in range(n_machines):
            if active_ops[m] is not None and not in_breakdown(m, t):
                jop, opidx, st, rem = active_ops[m]
                rem -= 1
                if rem <= 0:
                    finish = t+1
                    job_progress[jop] += 1
                    job_earliest_start[jop] = finish
                    schedule.append((jop, opidx, m, st, finish))
                    active_ops[m] = None
                    job_current_machine[jop] = None
                else:
                    active_ops[m] = (jop, opidx, st, rem)
        # machine loads for LLM
        machine_loads = {m: (active_ops[m][3] if active_ops[m] is not None else 0) for m in range(n_machines)}
        # Dispatch on idle
        for m in range(n_machines):
            if active_ops[m] is None and not in_breakdown(m, t):
                best: Tuple[float,int,int,int] | None = None
                for j in range(len(jobs)):
                    if job_progress[j] >= len(jobs[j]):
                        continue
                    if job_current_machine[j] is not None or t < job_earliest_start[j]:
                        continue
                    opidx = job_progress[j]
                    for m_alt, pt_alt in jobs[j][opidx]:
                        if m_alt != m:
                            continue
                        pr = compute_priority(rule, j, opidx, m, pt_alt, t,
                                               jobs=jobs,
                                               job_progress=job_progress,
                                               arrival_times=arrival_times,
                                               due_dates=due_dates,
                                               machine_loads=machine_loads)
                        if best is None or pr < best[0]:
                            best = (pr, j, opidx, pt_alt)
                if best is not None:
                    _prio, j_sel, op_sel, pt_sel = best
                    active_ops[m] = (j_sel, op_sel, t, pt_sel)
                    job_current_machine[j_sel] = m
        t += 1
    makespan = metric_makespan(schedule)
    return makespan, schedule

###############################################################################
# 5) Plot Gantt
###############################################################################

def plot_gantt(schedule: List[Tuple[int,int,int,int,int]],
               n_machines: int,
               breakdowns: Dict[int,List[Tuple[int,int]]],
               title: str = "Gantt Chart",
               save_path: str | None = None):
    fig, ax = plt.subplots(figsize=(10,6))
    for m in range(n_machines):
        for s_bd, e_bd in breakdowns.get(m, []):
            ax.barh(m, e_bd-s_bd, left=s_bd, height=0.8, color="red", alpha=0.3)
    cmap = plt.cm.get_cmap("tab10", 10)
    for j, op, mach, s, f in schedule:
        ax.barh(mach, f-s, left=s, color=cmap(j%10), edgecolor="black", height=0.6)
        ax.text(s+(f-s)/2, mach, f"J{j}-O{op}", ha="center", va="center", color="white", fontsize=7)
    ax.set_xlabel("Time"); ax.set_ylabel("Machine"); ax.set_yticks(range(n_machines)); ax.set_title(title)
    ax.legend(handles=[mpatches.Patch(color="red", alpha=0.3, label="Breakdown")])
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=120)
        plt.close()
    else:
        plt.show()

###############################################################################
# 6) MAIN – evaluare reguli
###############################################################################

if __name__ == "__main__":
    INPUT_DIR  = "dfjss_inputs_and_generators/dynamic-FJSP-instances/test/barnes"
    OUTPUT_DIR = "gantt_outputs/classic"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    RULES = ["SPT", "LPT", "FIFO", "LIFO", "SRPT", "OPR", "ECT", "LLM", "Random"]
    ms_store   = {r: [] for r in RULES}
    time_store = {r: [] for r in RULES}   #  NEW: colectăm timpii

    RESULTS_FILE = "rezultate/classic.txt"
    with open(RESULTS_FILE, "w") as fout:
        for fname in os.listdir(INPUT_DIR):
            if not fname.endswith(".txt"):
                continue
            fpath = os.path.join(INPUT_DIR, fname)
            n_jobs, n_mach, jobs, events = read_dynamic_fjsp_instance(fpath)
            fout.write(f"\n=== Instanța: {fname} (jobs={n_jobs}, machines={n_mach}) ===\n")
            print(f"\n=== Instanța: {fname} ===")

            for rule in RULES:
                jb_copy = copy.deepcopy(jobs)
                ev_copy = {
                    "breakdowns": {m: list(bds) for m, bds in events["breakdowns"].items()},
                    "added_jobs":     list(events["added_jobs"]),
                    "cancelled_jobs": list(events["cancelled_jobs"]),
                }

                t0 = time.perf_counter()                 #  NEW: start cronometru
                ms, sched = schedule_dynamic_no_parallel(
                    jb_copy, n_mach, ev_copy, rule
                )
                elapsed = time.perf_counter() - t0       #  NEW: timp scurs

                metric_append(ms_store,   rule, ms)
                metric_append(time_store, rule, elapsed)

                fout.write(f"{rule} => MS={ms}, T={elapsed:.3f}s\n")
                print(      f"{rule} => MS={ms}, T={elapsed:.3f}s")

                # (plot Gantt identic cu înainte)
                plot_gantt(
                    sched, n_mach, ev_copy["breakdowns"],
                    title=f"{fname} - {rule} (MS={ms})",
                    save_path=os.path.join(
                        OUTPUT_DIR, f"{fname}_{rule}.png".replace(".txt", "")
                    )
                )

        # --- medii ---
        fout.write("\n=== Average per rule ===\n")
        print("\n=== Average per rule ===")
        avg_ms   = metric_average(ms_store)
        avg_time = metric_average(time_store)             #  NEW
        for r in RULES:
            fout.write(f"{r}: MS={avg_ms[r]:.2f}, T={avg_time[r]:.3f}s\n")
            print(      f"{r}: MS={avg_ms[r]:.2f}, T={avg_time[r]:.3f}s")

    print(f"\nRezultatele au fost scrise în {RESULTS_FILE}")
    print(f"Graficele Gantt se află în directorul '{OUTPUT_DIR}'")