import os
import operator
import random
from deap import base, creator, gp, tools, algorithms

def read_instance_with_events(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    num_machines, num_jobs = 0, 0
    jobs = []
    events = {
        'breakdowns': {},
        'added_jobs': [],
        'cancelled_jobs': []
    }

    dynamic_section = False
    added_jobs_section = False

    for line in lines:
        line = line.strip()

        # Identifică secțiunile fișierului
        if line.startswith("# Dynamic Events"):
            dynamic_section = True
        elif line.startswith("# Machine Breakdowns"):
            dynamic_section = True
            added_jobs_section = False
        elif line.startswith("# Added Jobs"):
            dynamic_section = True
            added_jobs_section = True
        elif line.startswith("# Cancelled Jobs"):
            dynamic_section = True
            added_jobs_section = False

        # Citirea numărului de mașini și joburi
        elif not dynamic_section and line and not line.startswith("#"):
            if num_machines == 0 and num_jobs == 0:
                num_machines, num_jobs = map(int, line.split())
            else:
                operations = list(map(int, line.split()))
                job = [(operations[i], operations[i + 1]) for i in range(0, len(operations), 2)]
                jobs.append(job)

        # Citirea defectărilor
        elif dynamic_section and not added_jobs_section and line and not line.startswith("#"):
            parts = line.split()
            if len(parts) == 3:  # Verificăm că există exact 3 valori
                machine, start, end = map(int, parts)
                if machine not in events['breakdowns']:
                    events['breakdowns'][machine] = []
                events['breakdowns'][machine].append((start, end))
            else:
                print(f"Linie ignorată în defectări: {line}")  # Mesaj pentru debugging

        # Citirea joburilor noi
        elif dynamic_section and added_jobs_section and line and not line.startswith("#"):
            time, job_data = line.split(":")
            time = int(time)
            job = [(int(job_data.split()[i]), int(job_data.split()[i + 1])) for i in range(0, len(job_data.split()), 2)]
            events['added_jobs'].append((time, job))

        # Citirea joburilor anulate
        elif dynamic_section and not added_jobs_section and line and not line.startswith("#"):
            parts = line.split()
            if len(parts) == 2:  # Verificăm că există exact 2 valori
                cancel_time, job_id = map(int, parts)
                events['cancelled_jobs'].append((cancel_time, job_id))
            else:
                print(f"Linie ignorată în joburi anulate: {line}")  # Mesaj pentru debugging

    return num_machines, num_jobs, jobs, events

def read_all_instances(directory_path):
    instances = []
    files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith(".txt")]
    for file_path in files:
        num_machines, num_jobs, jobs, events = read_instance_with_events(file_path)
        instances.append((num_machines, num_jobs, jobs, events))
    return instances


pset = gp.PrimitiveSet("MAIN", 2)  # Intrări: job și timpul curent
pset.addPrimitive(operator.add, 2)
pset.addPrimitive(operator.sub, 2)
pset.addPrimitive(operator.mul, 2)
pset.addPrimitive(operator.truediv, 2)
pset.addPrimitive(max, 2)
pset.addPrimitive(min, 2)
pset.addTerminal(1)
pset.addTerminal(0)

creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", gp.PrimitiveTree, fitness=creator.FitnessMin)

toolbox = base.Toolbox()
toolbox.register("expr", gp.genFull, pset=pset, min_=1, max_=3)
toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.expr)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)

toolbox.register("compile", gp.compile, pset=pset)


def eval_multi_instance_makespan(individual, instances):
    dispatch_func = toolbox.compile(expr=individual)

    total_makespan = 0
    for num_machines, num_jobs, jobs, events in instances:
        machine_availability = [0] * num_machines
        job_completion = [0] * len(jobs)
        current_jobs = jobs.copy()

        added_jobs = sorted(events['added_jobs'], key=lambda x: x[0])
        cancelled_jobs = {event[1]: event[0] for event in events['cancelled_jobs']}
        breakdowns = events['breakdowns']

        time = 0
        while current_jobs or added_jobs:
            while added_jobs and added_jobs[0][0] <= time:
                _, job = added_jobs.pop(0)
                current_jobs.append(job)

            current_jobs = [job for i, job in enumerate(current_jobs) if i not in cancelled_jobs or time < cancelled_jobs[i]]

            for job_id, job in enumerate(current_jobs):
                current_time = 0
                for machine, process_time in job:
                    breakdowns_for_machine = breakdowns.get(machine, [])
                    for start, end in breakdowns_for_machine:
                        if start <= time < end:
                            time = end

                    start_time = max(machine_availability[machine], current_time)
                    end_time = start_time + process_time
                    machine_availability[machine] = end_time
                    current_time = end_time
                job_completion[job_id] = current_time

            time += 1

        makespan = max(job_completion) if job_completion else 0
        total_makespan += makespan

    return total_makespan / len(instances),

# Operatorii genetici
toolbox.register("evaluate", eval_multi_instance_makespan, instances=None)
toolbox.register("mate", gp.cxOnePoint)
toolbox.register("mutate", gp.mutUniform, expr=toolbox.expr, pset=pset)
toolbox.register("select", tools.selTournament, tournsize=3)

# Funcția principală
def genetic_programming_for_all_instances(directory_path, num_generations=40, population_size=100):
    instances = read_all_instances(directory_path)
    toolbox.register("evaluate", eval_multi_instance_makespan, instances=instances)

    population = toolbox.population(n=population_size)
    algorithms.eaSimple(
        population, toolbox,
        cxpb=0.7, mutpb=0.2, ngen=num_generations,
        stats=None, halloffame=None, verbose=True
    )

    best_individual = tools.selBest(population, k=1)[0]
    return best_individual

# Rularea pentru toate instanțele din director
directory_path = "dynamic_JSP_instances"
best_individual = genetic_programming_for_all_instances(directory_path)

print("Regula de dispatch generală:", best_individual)
