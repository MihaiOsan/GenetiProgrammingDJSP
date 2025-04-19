"""Rule‑learning workflow:
1. Antrenează GP pe instanțele din <train_dir>.
2. Extrage cei mai buni 5 indivizi din Hall‑of‑Fame.
3. Evaluează **fiecare** dintre ei pe instanțele de test și raportează rezultatele.

NOTĂ: Presupune că utilitarele existente (load_instances_from_directory,
      run_genetic_program, evaluate_individual etc.) funcționează la fel
      ca în varianta anterioară.
"""

from __future__ import annotations

import os
import copy
import time
from pathlib import Path

from data_reader import load_instances_from_directory
from evaluator    import evaluate_individual
from gantt_plot   import plot_gantt
from gp_setup     import create_toolbox
from evaluator    import run_genetic_program  # dacă numele e diferit, ajustează

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
TRAIN_DIR = Path("dfjss_inputs_and_generators/dynamic-FJSP-instances/train/barnes")
TEST_DIR  = Path("dfjss_inputs_and_generators/dynamic-FJSP-instances/test/barnes")
POP_SIZE  = 5
N_GENERATIONS = 1
N_WORKERS = 3  # trece la create_toolbox(np=N_WORKERS)
MAX_HOF   = 5  # câți păstrăm în Hall‑of‑Fame

RESULTS_FILE = "rezultate/genetic.txt"
GANTT_DIR    = Path("gantt_outputs/genetic")
GANTT_DIR.mkdir(exist_ok=True)

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

    # 3) GP training ⇒ Hall‑of‑Fame (top 5)
    print("\n=== GP TRAINING ===")
    hof = run_genetic_program(
        train_insts,
        toolbox,
        ngen=N_GENERATIONS,
        pop_size=POP_SIZE,
        halloffame=MAX_HOF,
    )
    best_5: List = list(hof)[:MAX_HOF]

    print("Top 5 indivizi (fitness):")
    for idx, ind in enumerate(best_5, 1):
        fit_val = ind.fitness.values[0] if ind.fitness.valid else float("inf")
        print(f"  {idx}: {fit_val:.4f}  ->  {ind}")

    # 4) Test each individual
    with open(RESULTS_FILE, "w", encoding="utf-8") as outf:
        for rank, ind in enumerate(best_5, 1):
            ind_fit = ind.fitness.values[0] if ind.fitness.valid else float("inf")
            rule_str = str(ind)

            outf.write(f"\n=== Individual {rank} ===\n")
            outf.write(f"Fitness_train: {ind_fit:.4f}\n")
            outf.write(f"Expression: {rule_str}\n")

            print(f"\n=== Testing Individual {rank}/{MAX_HOF} (fitness={ind_fit:.4f}) ===")
            print(f"Expr: {rule_str}")

            sum_ms   = 0.0
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

                sum_ms += ms
                time_vals.append(elapsed)

                outf.write(f"{fname}: MS={ms}, T={elapsed:.3f}s\n")
                print(f"  {fname}: MS={ms}, T={elapsed:.3f}s")

                # Gantt (opţional)
                gantt_name = f"{Path(fname).stem}_ind{rank}.png"
                plot_gantt(sched, nm, ev_cp["breakdowns"],
                           title=f"{fname} – ind{rank} (MS={ms})",
                           save_path=GANTT_DIR / gantt_name)

            avg_ms   = sum_ms   / len(test_insts) if test_insts else 0.0
            avg_time = sum(time_vals) / len(time_vals) if time_vals else 0.0

            outf.write(f"Average_MS: {avg_ms:.2f}\nAverage_T : {avg_time:.3f}s\n")
            print(f"  Average MS  = {avg_ms:.2f}")
            print(f"  Average T   = {avg_time:.3f}s")

    print(f"\nRezultatele au fost scrise în '{RESULTS_FILE}'.")
    print(f"Durata totală: {time.time() - global_start:.1f}s")

if __name__ == "__main__":
    main()
