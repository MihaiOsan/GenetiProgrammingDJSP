import os
import copy
import random
import operator
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from deap import base, creator, tools, gp, algorithms

###############################################################################
# 1) Citirea instanțelor FJSP (cu evenimente dinamice)
###############################################################################
def read_dynamic_fjsp_instance(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    num_jobs, num_machines = map(int, lines[0].split())
    jobs = []
    dynamic_events = {
        "breakdowns": {},
        "added_jobs": [],
        "cancelled_jobs": []
    }

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
            data = list(map(int, line.split()))
            num_operations = data[0]
            job_ops = []
            idx = 1
            for _ in range(num_operations):
                num_alts = data[idx]
                idx += 1
                alt_list = []
                for _ in range(num_alts):
                    m = data[idx]
                    p = data[idx + 1]
                    idx += 2
                    alt_list.append((m, p))
                job_ops.append(alt_list)
            jobs.append(job_ops)
        elif parsing_breakdowns:
            machine, start, end = map(int, line.split())
            if machine not in dynamic_events['breakdowns']:
                dynamic_events['breakdowns'][machine] = []
            dynamic_events['breakdowns'][machine].append((start, end))
        elif parsing_added_jobs:
            time_part, job_part = line.split(":")
            time_part = int(time_part)
            job_part = list(map(int, job_part.split()))
            num_operations = job_part[0]
            j_ops = []
            idx = 1
            for _ in range(num_operations):
                num_alts = job_part[idx]
                idx += 1
                alt_list = []
                for _ in range(num_alts):
                    m = job_part[idx]
                    p = job_part[idx + 1]
                    idx += 2
                    alt_list.append((m, p))
                j_ops.append(alt_list)
            dynamic_events['added_jobs'].append((time_part, j_ops))
        elif parsing_cancelled_jobs:
            time, job_id = map(int, line.split())
            dynamic_events['cancelled_jobs'].append((time, job_id))

    return num_jobs, num_machines, jobs, dynamic_events


###############################################################################
# 2) GP - definiții de bază (terminale, primitive, etc.)
###############################################################################
def protected_div(a, b):
    return a / b if abs(b) > 1e-9 else a

pset = gp.PrimitiveSet("MAIN", 3)  # 3 argumente: PT, RO, MW
pset.renameArguments(ARG0='PT')
pset.renameArguments(ARG1='RO')
pset.renameArguments(ARG2='MW')

pset.addPrimitive(operator.add, 2)
pset.addPrimitive(operator.sub, 2)
pset.addPrimitive(operator.mul, 2)
pset.addPrimitive(protected_div, 2)
pset.addPrimitive(operator.neg, 1)
pset.addPrimitive(min, 2)
pset.addPrimitive(max, 2)

pset.addTerminal(1.0)
pset.addTerminal(0.0)

creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", gp.PrimitiveTree, fitness=creator.FitnessMin)

toolbox = base.Toolbox()
toolbox.register("expr", gp.genFull, pset=pset, min_=1, max_=3)
toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.expr)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)
toolbox.register("compile", gp.compile, pset=pset)


###############################################################################
# 3) Verificăm dacă un breakdown **începe** în [start, end)
###############################################################################
def get_first_breakdown_in_interval(start_t, end_t, breakdowns):
    """
    Returnează (bd_start, bd_end) dacă există un breakdown care începe
    în interiorul (start_t, end_t). Altfel, None.
    """
    for (bd_s, bd_e) in breakdowns:
        if bd_s > start_t and bd_s < end_t:
            return (bd_s, bd_e)
    return None


###############################################################################
# 4) Evaluare individ: breakdown => mașina blocată până la bd_end, job reînceput
###############################################################################
def evaluate_individual(individual, jobs, num_machines, events):
    dispatch_rule = toolbox.compile(expr=individual)

    # sortăm breakdown-urile
    breakdowns_per_machine = {}
    for m in range(num_machines):
        bd = events['breakdowns'].get(m, [])
        bd_sorted = sorted(bd, key=lambda x: x[0])
        breakdowns_per_machine[m] = bd_sorted

    machine_end_time = [0.0] * num_machines
    job_end_time = [0.0] * len(jobs)

    # Operații ready
    ready_ops = []
    for j in range(len(jobs)):
        if jobs[j]:
            ready_ops.append((j, 0))

    schedule = []
    current_time = 0.0

    while ready_ops:
        # 1) Generăm candidați (job_id, op_idx, machine, ptime, priority)
        candidates = []
        for (job_id, op_idx) in ready_ops:
            alt_machines = jobs[job_id][op_idx]
            for (m, ptime) in alt_machines:
                PT = float(ptime)
                total_ops_for_job = len(jobs[job_id])
                RO = float(total_ops_for_job - op_idx)
                MW = max(0.0, machine_end_time[m] - current_time)
                priority = dispatch_rule(PT, RO, MW)
                candidates.append((priority, job_id, op_idx, m, PT))

        # 2) Sortăm după priority (minim)
        candidates.sort(key=lambda x: x[0])
        _, best_j, best_op, best_machine, best_pt = candidates[0]

        # 3) Scoatem toate aparițiile (best_j, best_op) din ready_ops
        to_remove = []
        for r in ready_ops:
            if r[0] == best_j and r[1] == best_op:
                to_remove.append(r)
        for rr in to_remove:
            ready_ops.remove(rr)

        # 4) Programăm
        start_t = max(machine_end_time[best_machine], job_end_time[best_j])
        end_t = start_t + best_pt

        # 5) Verificăm breakdown
        bd_info = get_first_breakdown_in_interval(start_t, end_t, breakdowns_per_machine[best_machine])
        if bd_info is not None:
            # => breakdown începe în [start_t, end_t)
            bd_start, bd_end = bd_info

            # Mașina e blocată până la bd_end (nu doar până la bd_start!)
            machine_end_time[best_machine] = max(machine_end_time[best_machine], bd_end)


            # Jobul nu avansează (pierzând progresul)
            # => job_end_time[best_j] rămâne la fel

            # Nu adăugăm nimic în schedule (job-ul n-a progresat deloc)
            # sau am putea pune un segment "irosit" (opțional)

            # Reintroducem (best_j, best_op) în coada de ready
            ready_ops.append((best_j, best_op))

        else:
            # => niciun breakdown în interval
            machine_end_time[best_machine] = end_t
            job_end_time[best_j] = end_t

            # Salvăm operația în schedule
            schedule.append((best_j, best_op, best_machine, start_t, end_t))

            # Trecem la op următoare
            if best_op + 1 < len(jobs[best_j]):
                ready_ops.append((best_j, best_op + 1))

        current_time = min(machine_end_time)

    makespan = max(machine_end_time)
    return makespan, schedule


###############################################################################
# 5) Fitness multi-instanta (media makespan)
###############################################################################
def multi_instance_fitness(individual, instances):
    total = 0.0
    for (jobs, num_machines, events) in instances:
        jb_copy = copy.deepcopy(jobs)
        ev_copy = {
            'breakdowns': {m: list(bd) for m, bd in events['breakdowns'].items()},
            'added_jobs': list(events['added_jobs']),
            'cancelled_jobs': list(events['cancelled_jobs'])
        }
        ms, _ = evaluate_individual(individual, jb_copy, num_machines, ev_copy)
        total += ms
    return (total / len(instances),)


###############################################################################
# 6) GP simplu
###############################################################################
def run_genetic_program(instances, ngen=10, pop_size=20):
    toolbox.register("evaluate", multi_instance_fitness, instances=instances)
    toolbox.register("select", tools.selTournament, tournsize=3)
    toolbox.register("mate", gp.cxOnePoint)
    toolbox.register("mutate", gp.mutUniform, expr=toolbox.expr, pset=pset)

    pop = toolbox.population(n=pop_size)
    hof = tools.HallOfFame(1)

    algorithms.eaSimple(pop, toolbox, cxpb=0.7, mutpb=0.3, ngen=ngen,
                        halloffame=hof, verbose=True)

    return hof[0]  # cel mai bun individ


###############################################################################
# 7) Plot Gantt (evidențiere breakdown)
###############################################################################
def plot_gantt(schedule, num_machines, breakdowns, title="Gantt Chart"):
    fig, ax = plt.subplots(figsize=(10, 6))

    # 1) Desenăm întâi breakdown-urile ca bare semi-transparente pe fundal
    for m in range(num_machines):
        if m in breakdowns:
            for (bd_start, bd_end) in breakdowns[m]:
                bd_duration = bd_end - bd_start
                ax.barh(
                    m,
                    bd_duration,
                    left=bd_start,
                    height=0.8,
                    color='red',
                    alpha=0.3,
                    edgecolor=None
                )

    # 2) Desenăm operațiile
    colors = plt.cm.get_cmap('tab10', 10)
    for (job_id, op_idx, machine_id, start, end) in schedule:
        duration = end - start
        ax.barh(
            machine_id,
            duration,
            left=start,
            color=colors(job_id % 10),
            edgecolor='black',
            height=0.6
        )
        ax.text(
            start + duration / 2,
            machine_id,
            f"J{job_id}-O{op_idx}",
            ha="center",
            va="center",
            color="white",
            fontsize=7
        )

    ax.set_xlabel("Time")
    ax.set_ylabel("Machine")
    ax.set_yticks(range(num_machines))
    ax.set_title(title)

    breakdown_patch = mpatches.Patch(color='red', alpha=0.3, label='Breakdown')
    ax.legend(handles=[breakdown_patch])

    plt.tight_layout()
    plt.show()


###############################################################################
# 8) Exemplu de utilizare
###############################################################################
if __name__ == "__main__":
    input_dir = "dynamic-FJSP-instances/barnes"
    all_instances = []
    for root, dirs, files in os.walk(input_dir):
        for fname in files:
            if fname.endswith(".txt"):
                fpath = os.path.join(root, fname)
                num_jobs, num_machines, jobs, events = read_dynamic_fjsp_instance(fpath)
                all_instances.append((jobs, num_machines, events))

    # Rulăm GP
    best_rule = run_genetic_program(all_instances, ngen=150, pop_size=50)
    print("Best rule (tree):", best_rule)

    # Testăm pe un fișier
    test_file = os.path.join(input_dir, "mt10c1_dynamic_1.txt")
    if os.path.exists(test_file):
        _, nm, jb, ev = read_dynamic_fjsp_instance(test_file)
        jb_copy = copy.deepcopy(jb)
        ev_copy = {
            'breakdowns': {m: list(bd) for m, bd in ev['breakdowns'].items()},
            'added_jobs': list(ev['added_jobs']),
            'cancelled_jobs': list(ev['cancelled_jobs'])
        }

        ms, schedule = evaluate_individual(best_rule, jb_copy, nm, ev_copy)
        print(f"Makespan pentru {test_file} = {ms}")
        plot_gantt(schedule, nm, ev_copy['breakdowns'], title=f"Gantt Chart Dynamic- {test_file}")
    else:
        print(f"Fișierul {test_file} nu există!")

        # Testăm pe un fișier
    test_file = os.path.join(input_dir, "mt10c1_static.txt")
    if os.path.exists(test_file):
        _, nm, jb, ev = read_dynamic_fjsp_instance(test_file)
        jb_copy = copy.deepcopy(jb)
        ev_copy = {
            'breakdowns': {m: list(bd) for m, bd in ev['breakdowns'].items()},
            'added_jobs': list(ev['added_jobs']),
            'cancelled_jobs': list(ev['cancelled_jobs'])
        }

        ms, schedule = evaluate_individual(best_rule, jb_copy, nm, ev_copy)
        print(f"Makespan pentru {test_file} = {ms}")
        plot_gantt(schedule, nm, ev_copy['breakdowns'], title=f"Gantt Chart Static - {test_file}")
    else:
        print(f"Fișierul {test_file} nu există!")
