import copy
from deap import tools, algorithms, gp
import random as rd

from scheduler import evaluate_individual

def multi_instance_fitness(individual, instances, toolbox):
    """
    Calculează fitness-ul pentru un individ,
    ca media makespan-ului pe o listă de instanțe.
    """

    print("   Evaluating individual " + str(individual))
    total_makespan = 0.0
    for (jobs, num_machines, events, _) in instances:
        jb_copy = copy.deepcopy(jobs)
        ev_copy = {
            'breakdowns': {m: list(bd) for m, bd in events['breakdowns'].items()},
            'added_jobs': list(events['added_jobs']),
            'cancelled_jobs': list(events['cancelled_jobs'])
        }

        ms, _ = evaluate_individual(individual, jb_copy, num_machines, ev_copy, toolbox)
        total_makespan += ms
    return (total_makespan / len(instances),)

def run_genetic_program(instances, toolbox, ngen=10, pop_size=20):
    """
    Rulează GP-ul pe instanțele date.
    `toolbox` trebuie să fie deja configurat cu operatorii DEAP.
    """
    # Adăugăm evaluarea și ceilalți operatori
    print("Running genetic program...")
    toolbox.register("evaluate", multi_instance_fitness, instances=instances, toolbox=toolbox)
    toolbox.register("select", tools.selTournament, tournsize=3)
    toolbox.register("mate", gp.cxOnePoint)
    toolbox.register("mutate", gp.mutUniform, expr=toolbox.expr, pset=toolbox.pset)

    # Inițializăm populația
    pop = toolbox.population(n=pop_size)
    hof = tools.HallOfFame(1)

    algorithms.eaSimple(pop, toolbox, cxpb=0.5, mutpb=0.3, ngen=ngen,
                        halloffame=hof, verbose=True)

    return hof[0]

'''def run_genetic_program_subsample(instances, toolbox,
                                     ngen=10, pop_size=20,
                                     chunk_size=5,
                                     subset_rate=0.5,
                                     cxpb=0.5, mutpb=0.3,
                                     halloffame=15, pop_init=None):
    """
    Împarte evoluția în segmente de `chunk_size` generații.
    La începutul fiecărui segment, alege un subset nou de instanțe
    (proporție subset_rate) și actualizează `toolbox.evaluate`.
    Apoi apelează `eaSimple` pentru chunk_size generații.
    Repetă până acoperim toți cei ngen.
    """
    # 1) Iniț. populația
    if pop_init is None:
        pop = toolbox.population(n=pop_size)
    else:
        pop = pop_init
    hof = tools.HallOfFame(halloffame)

    # 2) Stabilim câte segmente
    num_chunks = (ngen + chunk_size - 1) // chunk_size  # rotunjire "în sus"

    # Generații deja efectuate
    gens_done = 0

    for chunk_idx in range(num_chunks):
        # A) Verificăm câte generații facem în chunk-ul curent
        gens_left = ngen - gens_done
        gens_here = min(chunk_size, gens_left)
        if gens_here <= 0:
            break

        print(f"=== Chunk {chunk_idx}, generații {gens_here} ===")

        # B) Alegem subsetul pentru chunk-ul curent
        if subset_rate < 1.0:
            k = max(1, int(len(instances)*subset_rate))
            chosen_insts = rd.sample(instances, k)
        else:
            chosen_insts = instances

        # C) Definim evaluate care folosește chosen_insts
        def evaluate_sub(ind):
            return multi_instance_fitness(ind, chosen_insts, toolbox)

        # D) Înregistrăm evaluate, select, mate, mutate (dacă nu era deja)
        toolbox.register("evaluate", evaluate_sub)
        toolbox.register("select", tools.selTournament, tournsize=3)
        toolbox.register("mate", gp.cxOnePoint)
        toolbox.register("mutate", gp.mutUniform, expr=toolbox.expr, pset=toolbox.pset)

        # E) Apelăm eaSimple pentru chunk_size generații
        #    dar dacă gens_here < chunk_size (ultimul chunk), facem doar gens_here
        pop, log = algorithms.eaSimple(
            pop, toolbox,
            cxpb=cxpb, mutpb=mutpb,
            ngen=gens_here,
            halloffame=hof,
            verbose=True
        )

        gens_done += gens_here
        if gens_done >= ngen:
            break

    # 3) Return final: best individ (sau tot hof)
    return hof


'''
