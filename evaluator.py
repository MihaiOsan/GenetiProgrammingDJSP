import copy
from deap import tools, algorithms, gp

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

    algorithms.eaSimple(pop, toolbox, cxpb=0.7, mutpb=0.3, ngen=ngen,
                        halloffame=hof, verbose=True)

    return hof[0]
