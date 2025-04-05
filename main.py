import os
import copy

from data_reader import load_instances_from_directory
from evaluator import  run_genetic_program
from gantt_plot import plot_gantt
from gp_setup import create_toolbox
from scheduler import evaluate_individual
import time


def main():
    start_time = time.time()

    # ===================
    # 0) Încărcăm seturile pe categorii (small, medium, large)
    # ===================
    very_small_insts  = load_instances_from_directory("/Users/mihaiosan/PycharmProjects/Dizertatie/dynamic-FJSP-instances/barnes")
    '''small_insts = load_instances_from_directory("/Users/mihaiosan/PycharmProjects/Dizertatie/inputGeneration/dynamic-FJSP-instances/training_small")
    medium_insts = load_instances_from_directory("/Users/mihaiosan/PycharmProjects/Dizertatie/inputGeneration/dynamic-FJSP-instances/training_medium")
    large_insts  = load_instances_from_directory("/Users/mihaiosan/PycharmProjects/Dizertatie/inputGeneration/dynamic-FJSP-instances/training_large")
    very_large_insts = load_instances_from_directory("/Users/mihaiosan/PycharmProjects/Dizertatie/inputGeneration/dynamic-FJSP-instances/training_very_large")
'''
    # Dimensiunea populației, threadurile etc.
    pop_size = 5
    toolbox = create_toolbox()

    # ===================
    # 1) Etapa 1: Antrenament pe small
    # ===================
    print("\n=== Stage 1: very_small_insts ===")

    pop = toolbox.population(n=pop_size)
    # sub-eșantionare 50%, 30 generații
    hof_very_small = run_genetic_program(
        very_small_insts,
        toolbox,
        ngen=5,
        pop_size=pop_size
    )
    '''
    print(f"Stage 1 hall of fame {best_10_very_small}")

    # ===================
    # 2) Etapa 2: small (pornește cu best 10 + random)
    # ===================
    print("\n=== Stage 2: small_insts ===")
    new_pop = best_10_very_small + [toolbox.individual() for _ in range(pop_size - 10)]
    # reinițializez fitness
    for ind in new_pop:
        ind.fitness.values = (999999999,)

    hof_small = run_genetic_program_subsample(
        small_insts,
        toolbox,
        ngen=40,
        pop_size=pop_size,
        subset_rate=0.4,
        pop_init=new_pop,
        halloffame=5
    )
    best_5_small = list(hof_small)
    print(f"Stage 2 hall of fame {best_5_small}")

    # ===================
    # 2) Etapa 3: medium (pornește cu best 5 + random)
    # ===================
    print("\n=== Stage 3: medium_insts ===")
    new_pop = best_5_small + [toolbox.individual() for _ in range(pop_size - 5)]
    # reinițializez fitness
    for ind in new_pop:
        ind.fitness.values = (999999999,)

    hof_medium = run_genetic_program_subsample(
        medium_insts,
        toolbox,
        ngen=40,
        pop_size=pop_size,
        subset_rate=0.4,
        pop_init=new_pop,
        halloffame=5
    )

    best_5_medium = list(hof_medium)
    print(f"Stage 3 hall of fame {best_5_medium}")

    # ===================
    # 3) Etapa 4: large (pornește cu best 5 + random)
    # ===================
    print("\n===Stage 4: large_insts ===")
    new_pop2 = best_5_medium + [toolbox.individual() for _ in range(pop_size - 5)]
    for ind in new_pop2:
        ind.fitness.values = (999999999,)

    hof_large = run_genetic_program_subsample(
        large_insts,
        toolbox,
        ngen=34,
        pop_size=pop_size,
        subset_rate=0.35,
        pop_init=new_pop2
    )
    '''
    best_final = hof_very_small
    print("\nFinal best rule (tree):", best_final)

    # ===================
    # 4) TESTARE finală pe alt set de instanțe
    # ===================
    # ex. "dynamic-FJSP-instances/test/barnes" la fel ca exemplul vechi
    input_dir_test = "/Users/mihaiosan/PycharmProjects/Dizertatie/dynamic-FJSP-instances/test/barnes"
    test_insts = load_instances_from_directory(input_dir_test)

    results_file = "rezultate_instante.txt"
    sum_makespan = 0.0

    with open(results_file, "w") as outf:
        for (jobs, nm, ev, fname) in test_insts:
            jb_copy = copy.deepcopy(jobs)
            ev_copy = {
                'breakdowns': {m: list(bd) for m, bd in ev['breakdowns'].items()},
                'added_jobs': list(ev['added_jobs']),
                'cancelled_jobs': list(ev['cancelled_jobs'])
            }

            ms, schedule = evaluate_individual(best_final, jb_copy, nm, ev_copy, toolbox)
            sum_makespan += ms

            outf.write(f"Instanță: {fname}, Makespan: {ms}\n")
            print(f"Test - {fname}, Makespan={ms}")

            # generăm Gantt
            png_name = fname.replace(".txt", "_gantt.png")
            png_path = os.path.join("gantt_outputs", png_name)
            os.makedirs("gantt_outputs", exist_ok=True)
            plot_gantt(
                schedule, nm, ev_copy['breakdowns'],
                title=f"Gantt Chart - {fname} (makespan={ms})",
                save_path=png_path
            )

        # media
        avg_makespan = sum_makespan / len(test_insts) if test_insts else 0.0
        outf.write(f"\nMedia makespan: {avg_makespan:.2f}\n")
        print(f"Test - {fname}, Media makespan={avg_makespan:.2f}")

    print(f"\nRezultatele au fost scrise în fișierul: {results_file}")
    print(f"Timp Necesar: {time.time() - start_time:.2f} secunde")


if __name__ == "__main__":
    main()
