import os
import copy
import random
from collections import defaultdict
from typing import List, Dict, Tuple, Any
import matplotlib.pyplot as plt
import time

import matplotlib.patches as mpatches

from data_reader import read_dynamic_fjsp_instance_json

###############################################################################
# 0) UTILITARE COMUNE ---------------------------------------------------------
###############################################################################
#  Tuplul din `schedule` are structura (job, op_idx, machine, start, end)
TUPLE_FIELDS = {
    "job": 0,
    "op": 1,
    "machine": 2,
    "start": 3,
    "end": 4,
}

def field(op, name: str):
    """Returnează câmpul `name` dintr‑un op; funcționează atât pentru tuple,
    cât și pentru dict."""
    if isinstance(op, dict):
        return op[name]
    return op[TUPLE_FIELDS[name]]

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

def calc_machine_idle_time(sched: List) -> Tuple[float, float]:
    """Returnează (idle_total, idle_avg_per_machine)."""
    ops_by_m = defaultdict(list)
    for op in sched:
        ops_by_m[field(op, "machine")].append((field(op, "start"), field(op, "end")))

    idle_total = 0.0
    for ops in ops_by_m.values():
        ops.sort(key=lambda x: x[0])
        prev_end = 0.0
        for st, en in ops:
            idle_total += max(0.0, st - prev_end)
            prev_end = en
        makespan_m = max(e for _, e in ops)
        idle_total += max(0.0, makespan_m - prev_end)
    idle_avg = idle_total / len(ops_by_m) if ops_by_m else 0.0
    return idle_total, idle_avg


def calc_job_waiting_time(sched: List) -> Tuple[float, float]:
    """Returnează (wait_total, wait_avg_per_job)."""
    ops_by_j = defaultdict(list)
    for op in sched:
        ops_by_j[field(op, "job")].append((field(op, "start"), field(op, "end")))

    wait_total = 0.0
    for ops in ops_by_j.values():
        ops.sort(key=lambda x: x[0])
        prev_end = 0.0
        for st, en in ops:
            wait_total += max(0.0, st - prev_end)
            prev_end = en
    wait_avg = wait_total / len(ops_by_j) if ops_by_j else 0.0
    return wait_total, wait_avg


###############################################################################
# 3) Regulile de prioritizare / tie‑break
###############################################################################

def remaining_processing_time(jobs_list_arg: List[List[List[Tuple[int, int]]]],
                              job_idx_arg: int,
                              current_op_idx_arg: int) -> float:
    total_remaining_time = 0.0
    if 0 <= job_idx_arg < len(jobs_list_arg):
        job_ops_list = jobs_list_arg[job_idx_arg]
        if 0 <= current_op_idx_arg < len(job_ops_list):
            for op_list_idx in range(current_op_idx_arg, len(job_ops_list)):
                operation_alternatives = job_ops_list[op_list_idx]
                if operation_alternatives:
                    total_remaining_time += min(float(p_time) for _m, p_time in operation_alternatives)
    return total_remaining_time


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
                     machine_loads: Dict[int, int]) -> float:
    """Întoarce prioritatea numerică (mai mic = mai prioritar)."""
    if rule == "SPT":
        return ptime
    if rule == "LPT":
        return -ptime
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




def schedule_dynamic_no_parallel(
        initial_jobs_ops: List[List[List[Tuple[int, int]]]],
        n_machines: int,
        events: Dict[str, Any],
        rule: str,
        max_simulation_time: float = 200000.0
) -> Tuple[float, List[Tuple[int, int, int, float, float]]]:
    """
    Simulare incrementală cu gestionarea evenimentelor dinamice și ETPC.
    NU foloseste due_dates.
    """

    etpc_map: Dict[Tuple[int, int], List[Tuple[int, int, float]]] = defaultdict(list)
    min_start_due_to_etpc: Dict[Tuple[int, int], float] = defaultdict(float)

    for constr in events.get('etpc_constraints', []):
        try:
            fj, fo = int(constr['fore_job']), int(constr['fore_op_idx'])
            hj, ho = int(constr['hind_job']), int(constr['hind_op_idx'])
            tl = float(constr['time_lapse']);
            tl = max(0.0, tl)
            etpc_map[(fj, fo)].append((hj, ho, tl))
        except (KeyError, ValueError, TypeError) as e:
            print(f"Warning: Skipping invalid ETPC constraint {constr}: {e}")

    current_jobs_sim: List[List[List[Tuple[int, int]]]] = copy.deepcopy(initial_jobs_ops)
    bds_events = events.get("breakdowns", {})
    bds_per_machine: Dict[int, List[Tuple[float, float]]] = {
        m: sorted([(float(s), float(e)) for s, e in bds_events.get(m, [])], key=lambda x: x[0])
        for m in range(n_machines)
    }

    job_progress = [0] * len(current_jobs_sim)
    job_current_machine = [None] * len(current_jobs_sim)

    arrival_times: List[float] = []
    all_props = events.get('all_jobs_properties', [])

    for i in range(len(current_jobs_sim)):
        props_found = next((p for p in all_props if p.get('is_initial') and p.get('initial_job_idx') == i), None)
        current_arrival_time = 0.0
        if props_found:
            current_arrival_time = float(props_found.get('parsed_arrival_time', 0.0))
        arrival_times.append(current_arrival_time)

    job_earliest_start: List[float] = [arr_time for arr_time in arrival_times]
    effective_op_ready_time: Dict[Tuple[int, int], float] = {}

    for j_init_idx in range(len(current_jobs_sim)):
        if current_jobs_sim[j_init_idx]:
            etpc_min = min_start_due_to_etpc.get((j_init_idx, 0), 0.0)
            # Pentru joburile initiale, arrival_times[j_init_idx] este timpul de sosire (0.0 de obicei)
            effective_op_ready_time[(j_init_idx, 0)] = max(arrival_times[j_init_idx], etpc_min)

    active_ops: Dict[int, Tuple[int, int, float, float] | None] = {m: None for m in range(n_machines)}
    schedule: List[Tuple[int, int, int, float, float]] = []
    t: float = 0.0

    dynamic_event_list = []
    for t_add, new_job_ops in events.get("added_jobs", []):
        dynamic_event_list.append({'time': float(t_add), 'type': 'add', 'data': new_job_ops})
    for t_c, j_c_idx in events.get("cancelled_jobs", []):
        dynamic_event_list.append({'time': float(t_c), 'type': 'cancel', 'data': j_c_idx})
    dynamic_event_list.sort(key=lambda ev: ev['time'])
    current_dynamic_event_idx = 0

    while t < max_simulation_time:
        num_active_uncompleted_jobs = 0
        for j_idx_loop in range(len(current_jobs_sim)):
            if j_idx_loop < len(job_progress) and job_progress[j_idx_loop] < len(current_jobs_sim[j_idx_loop]):
                num_active_uncompleted_jobs += 1
        if num_active_uncompleted_jobs == 0 and current_dynamic_event_idx >= len(dynamic_event_list):
            break

        while current_dynamic_event_idx < len(dynamic_event_list) and \
                dynamic_event_list[current_dynamic_event_idx]['time'] <= t + 1e-9:
            event = dynamic_event_list[current_dynamic_event_idx]
            if abs(event['time'] - t) > 1e-9 and event['time'] < t:
                current_dynamic_event_idx += 1;
                continue
            current_dynamic_event_idx += 1
            if event['type'] == 'add':
                new_job_ops_data = copy.deepcopy(event['data'])
                new_j_id = len(current_jobs_sim)
                current_jobs_sim.append(new_job_ops_data)
                job_progress.append(0)
                job_current_machine.append(None)
                arrival_time_new_job = event['time']
                arrival_times.append(arrival_time_new_job)  # Adaugam la lista de arrival_times
                job_earliest_start.append(arrival_time_new_job)
                if new_job_ops_data:  # Daca jobul adaugat are operatii
                    etpc_min_new = min_start_due_to_etpc.get((new_j_id, 0), 0.0)
                    effective_op_ready_time[(new_j_id, 0)] = max(arrival_time_new_job, etpc_min_new)
            elif event['type'] == 'cancel':
                j_c = event['data']
                if j_c < len(job_progress) and job_progress[j_c] < len(current_jobs_sim[j_c]):
                    print(f"Time {t:.2f}: Job {j_c} cancelled.")
                    job_progress[j_c] = len(current_jobs_sim[j_c])
                    if job_current_machine[j_c] is not None:
                        active_ops[job_current_machine[j_c]] = None
                        job_current_machine[j_c] = None

        for m_bd_check in range(n_machines):
            is_breaking_down_now = any(s_bd <= t < e_bd for s_bd, e_bd in bds_per_machine.get(m_bd_check, []))
            if is_breaking_down_now and active_ops[m_bd_check] is not None:
                j_b, op_b, st_b, _rem_b = active_ops[m_bd_check]
                #print(f"Time {t:.2f}: Machine {m_bd_check} breakdown. Op J{j_b}-O{op_b} interrupted.")
                active_ops[m_bd_check] = None
                job_current_machine[j_b] = None
                job_earliest_start[j_b] = t
                etpc_min_interrupted = min_start_due_to_etpc.get((j_b, op_b), 0.0)
                effective_op_ready_time[(j_b, op_b)] = max(t, etpc_min_interrupted)

        for m_adv in range(n_machines):
            if active_ops[m_adv] is not None and not any(
                    s_bd <= t < e_bd for s_bd, e_bd in bds_per_machine.get(m_adv, [])):
                jop_adv, opidx_adv, st_adv, rem_adv = active_ops[m_adv]
                rem_adv -= 1.0
                if rem_adv < 1e-9:
                    finish_time = t + 1.0
                    job_progress[jop_adv] += 1
                    job_earliest_start[jop_adv] = finish_time
                    schedule.append((jop_adv, opidx_adv, m_adv, st_adv, finish_time))
                    active_ops[m_adv] = None
                    job_current_machine[jop_adv] = None

                    if (jop_adv, opidx_adv) in etpc_map:
                        for j_h, o_h, lapse in etpc_map[(jop_adv, opidx_adv)]:
                            new_min_start_for_hind = finish_time + lapse
                            current_min_etpc = min_start_due_to_etpc.get((j_h, o_h), 0.0)
                            min_start_due_to_etpc[(j_h, o_h)] = max(current_min_etpc, new_min_start_for_hind)

                            # Actualizam effective_op_ready_time pentru operatia hind afectata
                            # Daca operatia hind exista (jobul j_h a fost adaugat si op o_h e valida)
                            if j_h < len(job_earliest_start) and \
                                    ((o_h == 0) or \
                                     (o_h > 0 and (
                                     j_h, o_h - 1) in effective_op_ready_time)):  # Verificam daca pred din job e ready

                                base_ready_for_hind = job_earliest_start[j_h] if o_h == 0 else float('-inf')
                                if o_h > 0:
                                    # Cautam timpul de final al operatiei (j_h, o_h-1) daca a fost programata
                                    found_pred_in_schedule = False
                                    for sj, so, _, _, se_sched in schedule:
                                        if sj == j_h and so == o_h - 1:
                                            base_ready_for_hind = se_sched
                                            found_pred_in_schedule = True
                                            break
                                    # Daca predecesorul nu s-a terminat inca, nu putem seta effective_ready_time final
                                    # Se va calcula cand devine candidat
                                    if not found_pred_in_schedule: continue

                                effective_op_ready_time[(j_h, o_h)] = max(base_ready_for_hind,
                                                                          min_start_due_to_etpc.get((j_h, o_h), 0.0))
                else:
                    active_ops[m_adv] = (jop_adv, opidx_adv, st_adv, rem_adv)

        machine_loads = {m_load: (active_ops[m_load][3] if active_ops[m_load] is not None else 0.0)
                         for m_load in range(n_machines)}

        for m_dispatch in range(n_machines):
            if active_ops[m_dispatch] is None and not any(
                    s_bd <= t < e_bd for s_bd, e_bd in bds_per_machine.get(m_dispatch, [])):
                best_candidate_dispatch: Tuple[float, int, int, float] | None = None

                for j_cand in range(len(current_jobs_sim)):
                    if not (j_cand < len(job_progress) and job_progress[j_cand] < len(current_jobs_sim[j_cand])):
                        continue
                    if job_current_machine[j_cand] is not None:
                        continue

                    opidx_cand = job_progress[j_cand]

                    # Asiguram ca effective_op_ready_time este calculat/actualizat pentru candidati
                    base_time_cand = job_earliest_start[j_cand] if j_cand < len(job_earliest_start) else t
                    etpc_min_cand = min_start_due_to_etpc.get((j_cand, opidx_cand), 0.0)
                    current_effective_op_earliest_start = max(base_time_cand, etpc_min_cand)
                    effective_op_ready_time[(j_cand, opidx_cand)] = current_effective_op_earliest_start

                    if t < current_effective_op_earliest_start - 1e-9:
                        continue

                    for m_alt, pt_alt_float in current_jobs_sim[j_cand][opidx_cand]:
                        if m_alt == m_dispatch:
                            pt_alt = float(pt_alt_float)
                            if pt_alt < 1e-9: continue

                            current_arrival_time_cand = arrival_times[j_cand] if j_cand < len(arrival_times) else 0.0

                            # Apelam compute_priority fara due_dates
                            pr = compute_priority(rule, j_cand, opidx_cand, m_dispatch, pt_alt, t,
                                                  jobs=current_jobs_sim, job_progress=job_progress,
                                                  arrival_times=arrival_times,  # due_dates a fost scos
                                                  machine_loads=machine_loads)

                            if best_candidate_dispatch is None or pr < best_candidate_dispatch[0]:
                                best_candidate_dispatch = (pr, j_cand, opidx_cand, pt_alt)
                            break

                if best_candidate_dispatch is not None:
                    _prio_sel, j_sel, op_sel, pt_sel = best_candidate_dispatch
                    active_ops[m_dispatch] = (j_sel, op_sel, t, pt_sel)
                    job_current_machine[j_sel] = m_dispatch

        t += 1.0

    makespan = max(op_tuple[TUPLE_FIELDS["end"]] for op_tuple in schedule) if schedule else t
    if t >= max_simulation_time - 1e-9 and any(
            job_progress[j] < len(current_jobs_sim[j]) for j in range(len(current_jobs_sim)) if
            j < len(job_progress)):  # Verificam si len(job_progress)
        makespan = max(makespan, max_simulation_time)

    return makespan, schedule

###############################################################################
# 5) Plot Gantt
###############################################################################

def plot_gantt(ms, schedule: List[Tuple[int,int,int,int,int]],
               n_machines: int,
               breakdowns: Dict[int,List[Tuple[int,int]]],
               title: str = "Gantt Chart",
               save_path: str | None = None):
    fig, ax = plt.subplots(figsize=(10,6))
    for m in range(n_machines):
        for s_bd, e_bd in breakdowns.get(m, []):
            if s_bd < ms:
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
    INPUT_DIR  = "dfjss_inputs_and_generators/dynamic-FJSP-instances/test_sets"
    OUTPUT_DIR = "gantt_outputs/classic"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    RULES = ["SPT", "LPT", "FIFO", "LIFO", "SRPT", "OPR", "ECT", "LLM", "Random"]
    ms_store    = {r: [] for r in RULES}
    time_store  = {r: [] for r in RULES}
    idle_store  = {r: [] for r in RULES}   #  NEW
    wait_store  = {r: [] for r in RULES}   #  NEW

    RESULTS_FILE = "rezultate/classic.txt"
    with open(RESULTS_FILE, "w") as fout:
        for fname in os.listdir(INPUT_DIR):
            fpath = os.path.join(INPUT_DIR, fname)
            if fname.endswith(".json"):
                n_jobs, n_mach, jobs, events = read_dynamic_fjsp_instance_json(fpath)
            else:
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

                t0 = time.perf_counter()
                ms, sched = schedule_dynamic_no_parallel(jb_copy, n_mach, ev_copy, rule)
                elapsed = time.perf_counter() - t0

                # --- METRICE SUPLIMENTARE ---------------------------------
                _idle_total, idle_avg = calc_machine_idle_time(sched)
                _wait_total, wait_avg = calc_job_waiting_time(sched)
                # -----------------------------------------------------------

                metric_append(ms_store,   rule, ms)
                metric_append(time_store, rule, elapsed)
                metric_append(idle_store, rule, idle_avg)
                metric_append(wait_store, rule, wait_avg)

                fout.write(f"{rule} => MS={ms}, Idle_avg={idle_avg:.2f}, Wait_avg={wait_avg:.2f}, T={elapsed:.3f}s\n")
                print(      f"{rule} => MS={ms}, Idle_avg={idle_avg:.2f}, Wait_avg={wait_avg:.2f}, T={elapsed:.3f}s")

                plot_gantt(ms,
                    sched, n_mach, ev_copy["breakdowns"],
                    title=f"{fname} - {rule} (MS={ms})",
                    save_path=os.path.join(OUTPUT_DIR, f"{fname}_{rule}.png".replace(".txt", ""))
                )

        # --- MEDII PE REGULĂ ---------------------------------------------
        fout.write("\n=== Average per rule ===\n")
        print("\n=== Average per rule ===")
        avg_ms   = metric_average(ms_store)
        avg_time = metric_average(time_store)
        avg_idle = metric_average(idle_store)
        avg_wait = metric_average(wait_store)
        for r in RULES:
            fout.write(f"{r}: MS={avg_ms[r]:.2f}, Idle={avg_idle[r]:.2f}, Wait={avg_wait[r]:.2f}, T={avg_time[r]:.3f}s\n")
            print(      f"{r}: MS={avg_ms[r]:.2f}, Idle={avg_idle[r]:.2f}, Wait={avg_wait[r]:.2f}, T={avg_time[r]:.3f}s")

    print(f"\nRezultatele au fost scrise în {RESULTS_FILE}")
    print(f"Graficele Gantt se află în directorul '{OUTPUT_DIR}'")