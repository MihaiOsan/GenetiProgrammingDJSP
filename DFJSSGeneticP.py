import os
import random
from deap import base, creator, tools, gp, algorithms
import operator
import numpy as np


def read_dynamic_fjsp_instance(file_path):
    """Reads a dynamic FJSP instance including events."""
    with open(file_path, 'r') as f:
        lines = f.readlines()

    num_jobs, num_machines = map(int, lines[0].split())
    jobs = []
    dynamic_events = {"breakdowns": {}, "added_jobs": [], "cancelled_jobs": []}

    parsing_jobs = True
    parsing_breakdowns = False
    parsing_added_jobs = False
    parsing_cancelled_jobs = False

    for line in lines[1:]:
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("Dynamic Events"):
            parsing_jobs = False
            continue
        if "Machine Breakdowns" in line:
            parsing_breakdowns = True
            parsing_added_jobs = False
            parsing_cancelled_jobs = False
            continue
        if "Added Jobs" in line:
            parsing_breakdowns = False
            parsing_added_jobs = True
            parsing_cancelled_jobs = False
            continue
        if "Cancelled Jobs" in line:
            parsing_breakdowns = False
            parsing_added_jobs = False
            parsing_cancelled_jobs = True
            continue

        if parsing_jobs:
            try:
                data = list(map(int, line.split()))
                num_operations = data[0]
                job = []
                idx = 1
                for _ in range(num_operations):
                    num_machines_op = data[idx]
                    idx += 1

                    if idx + 2 * num_machines_op > len(data):
                        raise ValueError(
                            f"Malformed job operation data in file {file_path}:\n {line}"
                        )

                    machines = [
                        (data[idx + 2 * i], data[idx + 2 * i + 1])
                        for i in range(num_machines_op)
                    ]
                    job.append(machines)
                    idx += 2 * num_machines_op
                jobs.append(job)
            except Exception as e:
                raise ValueError(
                    f"Error parsing job data in file {file_path}:\n {line}\nError: {str(e)}"
                )
        elif parsing_breakdowns:
            try:
                machine, start, end = map(int, line.split())
                if machine not in dynamic_events['breakdowns']:
                    dynamic_events['breakdowns'][machine] = []
                dynamic_events['breakdowns'][machine].append((start, end))
            except ValueError:
                raise ValueError(
                    f"Error parsing breakdown data in file {file_path}:\n {line}"
                )
        elif parsing_added_jobs:
            try:
                time, *job_data = line.split(":")
                time = int(time)
                job_data = list(map(int, job_data[0].split()))
                num_operations = job_data[0]
                job = []
                idx = 1
                for _ in range(num_operations):
                    num_machines_op = job_data[idx]
                    idx += 1

                    if idx + 2 * num_machines_op > len(job_data):
                        raise ValueError(
                            f"Malformed added job data in file {file_path}:\n {line}"
                        )

                    machines = [
                        (job_data[idx + 2 * i], job_data[idx + 2 * i + 1])
                        for i in range(num_machines_op)
                    ]
                    job.append(machines)
                    idx += 2 * num_machines_op
                dynamic_events['added_jobs'].append((time, job))
            except Exception as e:
                raise ValueError(
                    f"Error parsing added job data in file {file_path}:\n {line}\nError: {str(e)}"
                )
        elif parsing_cancelled_jobs:
            try:
                time, job_id = map(int, line.split())
                dynamic_events['cancelled_jobs'].append((time, job_id))
            except ValueError:
                raise ValueError(
                    f"Error parsing cancelled job data in file {file_path}:\n {line}"
                )

    return num_jobs, num_machines, jobs, dynamic_events


def evaluate_fitness(individual, jobs, num_machines, events):
    """Evaluates the average makespan of the machines using a dispatch rule."""
    rule = toolbox.compile(expr=individual)

    machine_times = [0] * num_machines
    job_progress = [0] * len(jobs)
    time = 0

    while any(job_progress[j] < len(jobs[j]) for j in range(len(jobs))):
        # Handle dynamic events
        for machine, breakdowns in events['breakdowns'].items():
            for start, end in breakdowns:
                if start <= time < end:
                    machine_times[machine] = max(machine_times[machine], end)

        for event_time, new_job in events['added_jobs']:
            if event_time == time:
                jobs.append(new_job)
                job_progress.append(0)

        for event_time, cancelled_job in events['cancelled_jobs']:
            if event_time == time and cancelled_job < len(job_progress):
                job_progress[cancelled_job] = len(jobs[cancelled_job])  # Mark as complete

        available_operations = []

        for job_id, progress in enumerate(job_progress):
            if progress < len(jobs[job_id]):
                operation = jobs[job_id][progress]
                available_operations.append((job_id, operation))

        if not available_operations:
            time += 1
            continue

        # Use the rule to select the next operation
        selected_op = min(
            available_operations,
            key=lambda x: rule(x[0], min(machine_times))  # Use scalar metric instead of the full list
        )

        job_id, operation = selected_op
        selected_machine, processing_time = min(operation, key=lambda x: machine_times[x[0]] + x[1])

        machine_times[selected_machine] += processing_time
        job_progress[job_id] += 1

        time += 1

    return sum(machine_times) / num_machines,

def safe_div(left, right):
    """Performs safe division, avoiding division by zero."""
    return left / right if right != 0 else 1

# Define the primitive set for the dispatch rules
pset = gp.PrimitiveSet("MAIN", 2)  # Two inputs: job ID and machine times
pset.addPrimitive(operator.add, 2)
pset.addPrimitive(operator.sub, 2)
pset.addPrimitive(operator.mul, 2)
pset.addPrimitive(safe_div, 2)
pset.addPrimitive(max, 2)
pset.addPrimitive(min, 2)
pset.addPrimitive(operator.neg, 1)
pset.addTerminal(1)
pset.addTerminal(0)

creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", gp.PrimitiveTree, fitness=creator.FitnessMin)

toolbox = base.Toolbox()
toolbox.register("expr", gp.genFull, pset=pset, min_=1, max_=3)
toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.expr)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)
toolbox.register("compile", gp.compile, pset=pset)


def run_genetic_program(input_directory, generations=20, pop_size=50):
    instances = []
    events_list = []

    for root, _, files in os.walk(input_directory):
        for file_name in files:
            if file_name.endswith(".txt") and root.endswith("barnes"):
                file_path = os.path.join(root, file_name)
                num_jobs, num_machines, jobs, events = read_dynamic_fjsp_instance(file_path)
                instances.append((num_jobs, num_machines, jobs))
                events_list.append(events)

    def fitness_function(individual):
        total_makespan = 0
        for (num_jobs, num_machines, jobs), events in zip(instances, events_list):
            total_makespan += evaluate_fitness(individual, jobs, num_machines, events)[0]
        return total_makespan / len(instances),

    toolbox.register("evaluate", fitness_function)
    toolbox.register("mate", gp.cxOnePoint)
    toolbox.register("mutate", gp.mutUniform, expr=toolbox.expr, pset=pset)
    toolbox.register("select", tools.selTournament, tournsize=3)
    population = toolbox.population(n=pop_size)
    hall_of_fame = tools.HallOfFame(1)

    algorithms.eaSimple(
        population, toolbox,
        cxpb=0.7, mutpb=0.2, ngen=generations,
        stats=None, halloffame=hall_of_fame, verbose=True
    )

    return hall_of_fame[0]


# Exemplu de utilizare
input_directory = "dynamic-FJSP-instances"
best_rule = run_genetic_program(input_directory)
print("Cea mai bună regulă de dispatch:", best_rule)
