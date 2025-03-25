import os
import copy

from data_reader import load_instances_from_directory
from evaluator import run_genetic_program
from gantt_plot import plot_gantt
from gp_setup import create_toolbox
from scheduler import evaluate_individual


def main():
    input_dir = "dynamic-FJSP-instances/test/barnes"
    all_instances = load_instances_from_directory(input_dir)
    ng =25
    ps =15

    toolbox = create_toolbox(ps)

    # Parametrii exemplificativi
    best_rule = run_genetic_program(all_instances, toolbox, ngen=ng, pop_size=ps)
    print("Best rule (tree):", best_rule)

    # Test pe alte instanțe
    input_dir_test = "dynamic-FJSP-instances/barnes"
    all_instancest = load_instances_from_directory(input_dir_test)

    results_file = "rezultate_instante.txt"
    with open(results_file, "w") as outf:
        sum_makespan = 0.0

        for (jobs, nm, ev, fname) in all_instancest:
            jb_copy = copy.deepcopy(jobs)
            ev_copy = {
                'breakdowns': {m: list(bd) for m, bd in ev['breakdowns'].items()},
                'added_jobs': list(ev['added_jobs']),
                'cancelled_jobs': list(ev['cancelled_jobs'])
            }

            ms, schedule = evaluate_individual(best_rule, jb_copy, nm, ev_copy, toolbox)
            sum_makespan += ms
            outf.write(f"Instanță: {fname}, Makespan: {ms}\n")

            png_name = fname.replace(".txt", "_gantt.png")
            png_path = os.path.join("gantt_outputs", png_name)
            os.makedirs("gantt_outputs", exist_ok=True)
            plot_gantt(schedule, nm, ev_copy['breakdowns'],
                       title=f"Gantt Chart - {fname} (makespan={ms})",
                       save_path=png_path)

        avg_makespan = sum_makespan / len(all_instancest) if all_instancest else 0.0
        outf.write(f"\nMedia makespan: {avg_makespan:.2f}\n")

    print(f"Rezultatele au fost scrise în fișierul: {results_file}")

if __name__ == "__main__":
    main()
