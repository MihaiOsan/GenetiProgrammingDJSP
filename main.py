from __future__ import annotations

import os
import copy
import time
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple

from deap import gp

from data_reader import load_instances_from_directory
from evaluator    import evaluate_individual
from gantt_plot   import plot_gantt
from gp_setup     import create_toolbox
from evaluator    import run_genetic_program  # dacă numele e diferit, ajustează
from simple_tree import simplify_individual, tree_str, infix_str

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
TRAIN_DIR = Path("dfjss_inputs_and_generators/dynamic-FJSP-instances/training_sets")
TEST_DIR  = Path("dfjss_inputs_and_generators/dynamic-FJSP-instances/test_sets")
POP_SIZE  = 60
N_GENERATIONS = 5
N_WORKERS = 5         # trece la create_toolbox(np=N_WORKERS)
MAX_HOF   = 5         # câți păstrăm în Hall-of-Fame

RESULTS_FILE = "rezultate/genetic.txt"
GANTT_DIR    = Path("gantt_outputs/genetic")
GANTT_DIR.mkdir(exist_ok=True)

# ----------------------------------------------------------
#  Ordinea câmpurilor într-un tuplu din `schedule`
TUPLE_FIELDS = {
    "job":     0,
    "op":      1,
    "machine": 2,
    "start":   3,
    "end":     4,
}

def field(op, name: str):
    """Returnează câmpul `name` dintr-un op (dict sau tuple)."""
    if isinstance(op, dict):
        return op[name]
    return op[TUPLE_FIELDS[name]]
# ----------------------------------------------------------


# ---------------------------------------------------------------------------
#  UTILITARE METRICE
# ---------------------------------------------------------------------------
def calc_machine_idle_time(sched: List) -> Tuple[float, float]:
    ops_by_m = defaultdict(list)
    for op in sched:
        ops_by_m[field(op, "machine")].append(
            (field(op, "start"), field(op, "end"))
        )

    idle_total = 0.0
    for ops in ops_by_m.values():
        ops.sort(key=lambda x: x[0])          # sortăm după start
        prev_end = 0.0
        for st, en in ops:
            idle_total += max(0.0, st - prev_end)
            prev_end = en
        makespan_m = max(e for _, e in ops)   # ultimul end pe mașină
        idle_total += max(0.0, makespan_m - prev_end)

    idle_avg = idle_total / len(ops_by_m) if ops_by_m else 0.0
    return idle_total, idle_avg


def calc_job_waiting_time(sched: List) -> Tuple[float, float]:
    ops_by_j = defaultdict(list)
    for op in sched:
        ops_by_j[field(op, "job")].append(
            (field(op, "start"), field(op, "end"))
        )

    wait_total = 0.0
    for ops in ops_by_j.values():
        ops.sort(key=lambda x: x[0])
        prev_end = 0.0
        for st, en in ops:
            wait_total += max(0.0, st - prev_end)
            prev_end = en

    wait_avg = wait_total / len(ops_by_j) if ops_by_j else 0.0
    return wait_total, wait_avg


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    global_start = time.time()

    # 1) Load instances
    train_insts = load_instances_from_directory(TRAIN_DIR)
    test_insts  = load_instances_from_directory(TEST_DIR)

    # 2) Toolbox
    toolbox = create_toolbox(np=N_WORKERS)

    # 3) GP training ⇒ Hall-of-Fame (top 5)
    print("\n=== GP TRAINING ===")
    hof = run_genetic_program(
        train_insts,
        toolbox,
        ngen=N_GENERATIONS,
        pop_size=POP_SIZE,
        halloffame=MAX_HOF,
    )
    best_5: List = list(hof)[:MAX_HOF]

    print("Top 5 indivizi (fitness):")
    for idx, ind in enumerate(best_5, 1):
        fit_val = ind.fitness.values[0] if ind.fitness.valid else float("inf")
        print(f"  {idx}: {fit_val:.4f}  ->  {ind}")

    # 4) Test each individual
    with open(RESULTS_FILE, "w", encoding="utf-8") as outf:
        for rank, ind in enumerate(best_5, 1):
            ind_fit = ind.fitness.values[0] if ind.fitness.valid else float("inf")
            simp_ind = simplify_individual(ind, toolbox.pset)
            outf.write(f"\n=== Individual {rank} ===\n")
            outf.write(f"Fitness_train: {ind_fit:.4f}\n")
            outf.write(f"Formula: {infix_str(simp_ind)}\n")
            outf.write(f"Tree: \n{tree_str(simp_ind)}\n\n")


            print(f"\n=== Testing Individual {rank}/{MAX_HOF} (fitness={ind_fit:.4f}) ===")
            print(f"Expr: {infix_str(simp_ind)}\n")
            print(f"Expr: \n{tree_str(simp_ind)}\n\n")

            sum_ms = 0.0
            sum_idle = 0.0
            sum_wait = 0.0
            time_vals: List[float] = []

            for jobs, nm, ev, fname in test_insts:
                jb_cp = copy.deepcopy(jobs)
                ev_cp = {
                    "breakdowns": {m: list(bd) for m, bd in ev["breakdowns"].items()},
                    "added_jobs":  list(ev["added_jobs"]),
                    "cancelled_jobs": list(ev["cancelled_jobs"]),
                }

                t0 = time.perf_counter()
                ms, sched = evaluate_individual(ind, jb_cp, nm, ev_cp, toolbox)
                elapsed = time.perf_counter() - t0

                # --- metrice suplimentare -----------------------------------
                idle_total, idle_avg = calc_machine_idle_time(sched)
                wait_total, wait_avg = calc_job_waiting_time(sched)
                # -------------------------------------------------------------

                sum_ms   += ms
                sum_idle += idle_avg
                sum_wait += wait_avg
                time_vals.append(elapsed)

                outf.write(f"{fname}: "
                           f"MS={ms}, "
                           f"Idle_avg={idle_avg:.2f}, "
                           f"Wait_avg={wait_avg:.2f}, "
                           f"T={elapsed:.3f}s\n")

                print(f"  {fname}: "
                      f"MS={ms:<6} | "
                      f"Idle_avg={idle_avg:>7.2f} | "
                      f"Wait_avg={wait_avg:>7.2f} | "
                      f"T={elapsed:.3f}s")

                # Gantt (opţional)
                gantt_name = f"{Path(fname).stem}_ind{rank}.png"
                plot_gantt(ms, sched, nm, ev_cp["breakdowns"],
                           title=f"{fname} – ind{rank} (MS={ms})",
                           save_path=GANTT_DIR / gantt_name)

            n_tests = len(test_insts) or 1
            avg_ms     = sum_ms   / n_tests
            avg_idle   = sum_idle / n_tests
            avg_wait   = sum_wait / n_tests
            avg_time   = sum(time_vals) / n_tests

            outf.write("\n--- MEDIA ---\n")
            outf.write(f"Average_MS   : {avg_ms:.2f}\n"
                       f"Average_Idle : {avg_idle:.2f}\n"
                       f"Average_Wait : {avg_wait:.2f}\n"
                       f"Average_T    : {avg_time:.3f}s\n")

            print("  ------- MEDIA -------")
            print(f"  Average MS    = {avg_ms:.2f}")
            print(f"  Average Idle  = {avg_idle:.2f}")
            print(f"  Average Wait  = {avg_wait:.2f}")
            print(f"  Average T     = {avg_time:.3f}s")

    print(f"\nRezultatele au fost scrise în '{RESULTS_FILE}'.")
    print(f"Durata totală: {time.time() - global_start:.1f}s")

if __name__ == "__main__":
    main()
