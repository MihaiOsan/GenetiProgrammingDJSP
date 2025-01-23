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
# 3) Evaluare individ (simulare discrete-time, dar cu start/end corect salvate)
###############################################################################
def evaluate_individual(individual, jobs, num_machines, events, max_time=999999):
    """
    Varianta "discretă" în care timpul avansează cu 1 unitate, DAR:
      - Reținem momentul real de start în `ms.start_time`.
      - Când operația se termină, salvăm intervalul [start_time, current_time].
    """

    # ------------------------------------------------------------
    # 1) Compilăm regula de dispecerizare din individul GP
    # ------------------------------------------------------------
    dispatch_rule = toolbox.compile(expr=individual)

    # ------------------------------------------------------------
    # 2) Transformăm breakdowns/added_jobs/cancelled_jobs într-o listă de evenimente
    #    de forma (time, type, extra_info...)
    # ------------------------------------------------------------
    event_list = []

    # Breakdown -> evenimente
    for m_id, bd_list in events['breakdowns'].items():
        for (bd_start, bd_end) in bd_list:
            event_list.append((bd_start, "breakdown", m_id, bd_end))

    # Added jobs
    for (add_time, job_ops) in events['added_jobs']:
        event_list.append((add_time, "added_job", job_ops))

    # Cancelled jobs
    for (cancel_time, job_id) in events['cancelled_jobs']:
        event_list.append((cancel_time, "cancel_job", job_id))

    # Sortăm după momentul de start
    event_list.sort(key=lambda e: e[0])
    event_idx = 0
    total_events = len(event_list)

    # ------------------------------------------------------------
    # 3) Inițializăm starea mașinilor
    # ------------------------------------------------------------
    class MachineState:
        def __init__(self):
            self.busy = False      # True dacă procesează ceva
            self.job_id = None
            self.op_idx = None
            self.time_remaining = 0
            self.broken_until = 0  # mașina e defectă până la acest timp
            self.start_time = 0    # momentul la care a început operația curentă

    machines = [MachineState() for _ in range(num_machines)]

    # Timpul la care s-a terminat ultima operație din job (pentru joburile inițiale)
    job_end_time = [0.0] * len(jobs)

    # Operații gata de planificat
    ready_ops = []
    for j in range(len(jobs)):
        if len(jobs[j]) > 0:
            ready_ops.append((j, 0))

    # Joburi anulate
    cancelled_jobs = set()

    # Funcție pentru a adăuga un job nou (apărut la runtime)
    def add_new_job(job_ops):
        new_jid = len(jobs)
        jobs.append(job_ops)
        job_end_time.append(0.0)
        ready_ops.append((new_jid, 0))

    current_time = 0
    completed_ops = 0
    total_ops = sum(len(j) for j in jobs)

    # schedule = list de tuple (job_id, op_idx, machine_id, start_t, end_t)
    schedule = []

    # ------------------------------------------------------------
    # 4) BUCLA PRINCIPALĂ
    # ------------------------------------------------------------
    while current_time < max_time:
        # (A) Activăm evenimentele care au loc la current_time
        while event_idx < total_events and event_list[event_idx][0] == current_time:
            ev_time, ev_type = event_list[event_idx][0], event_list[event_idx][1]

            if ev_type == "breakdown":
                # (ev_time, "breakdown", machine_id, bd_end)
                _, _, m_id, bd_end = event_list[event_idx]
                # Marcăm mașina ca defectă până la bd_end
                machines[m_id].broken_until = bd_end

                # Dacă mașina lucra la ceva, pierdem progresul
                if machines[m_id].busy:
                    j_can = machines[m_id].job_id
                    o_can = machines[m_id].op_idx
                    if j_can not in cancelled_jobs:
                        # Reintroducem acea operație în ready_ops
                        ready_ops.append((j_can, o_can))
                # Eliberăm mașina
                machines[m_id].busy = False
                machines[m_id].job_id = None
                machines[m_id].op_idx = None
                machines[m_id].time_remaining = 0
                machines[m_id].start_time = 0

            elif ev_type == "added_job":
                # (ev_time, "added_job", job_ops)
                _, _, new_job_ops = event_list[event_idx]
                add_new_job(new_job_ops)
                total_ops += len(new_job_ops)

            elif ev_type == "cancel_job":
                # (ev_time, "cancel_job", job_id)
                _, _, job_id = event_list[event_idx]
                cancelled_jobs.add(job_id)
                # Dacă jobul e pe vreo mașină chiar acum, o resetăm
                for ms in machines:
                    if ms.busy and ms.job_id == job_id:
                        ms.busy = False
                        ms.job_id = None
                        ms.op_idx = None
                        ms.time_remaining = 0
                        ms.start_time = 0
                # Scoatem din ready_ops
                ready_ops = [(jj, oo) for (jj, oo) in ready_ops if jj != job_id]

            event_idx += 1

        # (B) Actualizăm starea fiecărei mașini pentru 1 unitate de timp
        for m_id, ms in enumerate(machines):
            # Dacă e defectă, nu face nimic
            if ms.broken_until > current_time:
                continue

            # Dacă e ocupată
            if ms.busy:
                ms.time_remaining -= 1
                if ms.time_remaining <= 0:
                    # Operația s-a terminat
                    jdone = ms.job_id
                    odone = ms.op_idx
                    start_op = ms.start_time  # momentul când a început
                    end_op = current_time     # momentul când s-a terminat

                    # Eliberăm mașina
                    ms.busy = False
                    ms.job_id = None
                    ms.op_idx = None
                    ms.time_remaining = 0
                    ms.start_time = 0

                    completed_ops += 1
                    job_end_time[jdone] = end_op

                    # Salvăm în schedule
                    schedule.append((jdone, odone, m_id, start_op, end_op))

                    # Dacă jobul mai are o operație (și nu e anulat)
                    if odone + 1 < len(jobs[jdone]) and jdone not in cancelled_jobs:
                        ready_ops.append((jdone, odone + 1))

            # Dacă e liberă și nu defectă
            if not ms.busy and ms.broken_until <= current_time:
                # Găsim candidații care pot rula pe mașina m_id
                best_candidate = None
                best_priority = float('inf')
                for (jj, oo) in ready_ops:
                    if jj in cancelled_jobs:
                        continue
                    alt_list = jobs[jj][oo]
                    ptime = None
                    for (mach, pt) in alt_list:
                        if mach == m_id:
                            ptime = pt
                            break
                    if ptime is not None:
                        PT = float(ptime)
                        RO = float(len(jobs[jj]) - oo)
                        MW = 0.0  # mașina e liberă acum
                        priority = dispatch_rule(PT, RO, MW)

                        if priority < best_priority:
                            best_priority = priority
                            best_candidate = (jj, oo, ptime)

                # Alocăm operația dacă s-a găsit
                if best_candidate is not None:
                    (jj, oo, ptime) = best_candidate
                    ms.busy = True
                    ms.job_id = jj
                    ms.op_idx = oo
                    ms.time_remaining = ptime
                    ms.start_time = current_time  # moment real de start

                    # Scoatem (jj, oo) din ready_ops
                    removed_once = False
                    new_ready = []
                    for (rj, ro) in ready_ops:
                        if not removed_once and rj == jj and ro == oo:
                            removed_once = True
                        else:
                            new_ready.append((rj, ro))
                    ready_ops = new_ready

        # (C) Verificăm dacă am terminat tot
        if completed_ops == total_ops:
            break

        # (D) Incrementăm timpul
        current_time += 1

    # ------------------------------------------------------------
    # 5) Makespan (max job_end_time pentru joburile neanulate)
    # ------------------------------------------------------------
    makespan = 0
    for j_id in range(len(jobs)):
        if j_id not in cancelled_jobs:
            if job_end_time[j_id] > makespan:
                makespan = job_end_time[j_id]

    return makespan, schedule


###############################################################################
# 4) Fitness multi-instanta (media makespan)
###############################################################################
def multi_instance_fitness(individual, instances):
    total = 0.0
    for (jobs, num_machines, events, _) in instances:
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
# 5) GP simplu
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
# 6) Plot Gantt (evidențiere breakdown)
###############################################################################
def plot_gantt(schedule, num_machines, breakdowns, title="Gantt Chart", save_path=None):
    fig, ax = plt.subplots(figsize=(10, 6))

    # 1) Desenăm întâi breakdown-urile
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

    if save_path:
        plt.savefig(save_path, dpi=150)
        plt.close()
    else:
        plt.show()


###############################################################################
# 7) Exemplu de utilizare și generarea unui fișier de rezultate
###############################################################################
if __name__ == "__main__":
    input_dir = "dynamic-FJSP-instances/barnes"
    all_instances = []

    # Vom reține și numele fișierului în instanțe, pentru raportare
    for root, dirs, files in os.walk(input_dir):
        for fname in files:
            print(f"processing {fname}")
            if fname.endswith(".txt"):
                fpath = os.path.join(root, fname)
                num_jobs, num_machines, jobs, events = read_dynamic_fjsp_instance(fpath)
                all_instances.append((jobs, num_machines, events, fname))

    # Rulăm GP (atenție, parametrii pop_size=2, ngen=10 sunt exemplificativi)
    best_rule = run_genetic_program(all_instances, ngen=25, pop_size=10)
    print("Best rule (tree):", best_rule)

    input_dir_test = "dynamic-FJSP-instances/test"
    all_instancest = []

    # Vom reține și numele fișierului în instanțe, pentru raportare
    for root, dirs, files in os.walk(input_dir_test):
        for fname in files:
            if fname.endswith(".txt"):
                fpath = os.path.join(root, fname)
                num_jobs, num_machines, jobs, events = read_dynamic_fjsp_instance(fpath)
                all_instancest.append((jobs, num_machines, events, fname))

    # Fișierul de rezultate
    results_file = "rezultate_instante.txt"
    with open(results_file, "w") as outf:
        sum_makespan = 0.0

        # Testăm fiecare instanță cu regula cea mai bună
        for (jobs, nm, ev, fname) in all_instancest:
            jb_copy = copy.deepcopy(jobs)
            ev_copy = {
                'breakdowns': {m: list(bd) for m, bd in ev['breakdowns'].items()},
                'added_jobs': list(ev['added_jobs']),
                'cancelled_jobs': list(ev['cancelled_jobs'])
            }

            ms, schedule = evaluate_individual(best_rule, jb_copy, nm, ev_copy)
            sum_makespan += ms

            # Scriem în fișier
            outf.write(f"Instanță: {fname}, Makespan: {ms}\n")

            # Salvăm graficul Gantt ca PNG (opțional)
            png_name = fname.replace(".txt", "_gantt.png")
            png_path = os.path.join("gantt_outputs", png_name)
            os.makedirs("gantt_outputs", exist_ok=True)
            plot_gantt(schedule, nm, ev_copy['breakdowns'],
                       title=f"Gantt Chart - {fname} (makespan={ms})",
                       save_path=png_path)

        # Media makespan
        avg_makespan = sum_makespan / len(all_instancest) if all_instancest else 0.0
        outf.write(f"\nMedia makespan: {avg_makespan:.2f}\n")

    print(f"Rezultatele au fost scrise în fișierul: {results_file}")
