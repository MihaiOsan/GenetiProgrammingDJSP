import os
import copy
import random
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

###############################################################################
# 1) Citire instanță FJSP (dinamic)
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
                    p = data[idx+1]
                    idx += 2
                    alt_list.append((m, p))
                job_ops.append(alt_list)
            jobs.append(job_ops)
        elif parsing_breakdowns:
            machine, start_bd, end_bd = map(int, line.split())
            if machine not in dynamic_events['breakdowns']:
                dynamic_events['breakdowns'][machine] = []
            dynamic_events['breakdowns'][machine].append((start_bd, end_bd))
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
                    p = job_part[idx+1]
                    idx += 2
                    alt_list.append((m, p))
                j_ops.append(alt_list)
            dynamic_events['added_jobs'].append((time_part, j_ops))
        elif parsing_cancelled_jobs:
            t_cancel, job_id = map(int, line.split())
            dynamic_events['cancelled_jobs'].append((t_cancel, job_id))

    return num_jobs, num_machines, jobs, dynamic_events


###############################################################################
# 2) Regulile clasice (SPT, LPT, EDD, Random)
###############################################################################
def compute_classic_priority(rule_name, job_id, op_idx, m, ptime,
                             time, due_dates):
    """
    Returnează o valoare de 'priority' (mai mic => mai prioritar).
    """
    if rule_name == "SPT":
        return ptime
    elif rule_name == "LPT":
        return -ptime
    elif rule_name == "EDD":
        return (due_dates[job_id] - time)
    elif rule_name == "Random":
        return random.random()
    # fallback: SPT
    return ptime


###############################################################################
# 3) Simulare incrementală (time += 1) cu constrângeri de ordine și breakdown-uri
###############################################################################
def schedule_dynamic_no_parallel(jobs, num_machines, events, rule_name):
    """
    Versiune incrementală: time += 1.
    - Fără rulare paralelă a aceluiași job (o singură operație la un moment dat).
    - Dacă începe un breakdown pe mașină, operația curentă se întrerupe și jobul își pierde progresul.
    - Pentru a ne asigura că operația (i+1) nu începe înainte să se termine (i),
      folosim un vector job_earliest_start[j].
    """
    breakdowns_per_machine = {
        m: sorted(events['breakdowns'].get(m, []), key=lambda x: x[0])
        for m in range(num_machines)
    }

    # job_progress[j] -> indexul operației curente (0-based) care trebuie făcută
    job_progress = [0]*len(jobs)
    # job_current_machine[j] -> ce mașină rulează ACUM jobul j (sau None)
    job_current_machine = [None]*len(jobs)
    # due_dates fictive, doar pentru EDD (se pot folosi altfel)
    due_dates = [100*(j+1) for j in range(len(jobs))]
    # active_ops[m] -> (job, op_index, start_time, remain)
    active_ops = {m: None for m in range(num_machines)}

    # Momentul la care jobul j poate începe următoarea operație
    job_earliest_start = [0]*len(jobs)

    schedule = []
    time = 0

    def machine_in_breakdown(m, t):
        bds = breakdowns_per_machine[m]
        for (bd_s, bd_e) in bds:
            if bd_s <= t < bd_e:
                return True
        return False

    while any(job_progress[j] < len(jobs[j]) for j in range(len(jobs))):
        # 1) Added Jobs la momentul curent (pot apărea joburi noi)
        for (t_add, new_job) in events['added_jobs']:
            if t_add == time:
                j_id = len(jobs)
                jobs.append(new_job)
                job_progress.append(0)
                job_current_machine.append(None)
                due_dates.append(100*(j_id+1))
                job_earliest_start.append(0)

        # 2) Cancelled Jobs la momentul curent
        for (t_c, j_c) in events['cancelled_jobs']:
            if t_c == time and j_c < len(job_progress):
                # Marchez jobul finalizat
                job_progress[j_c] = len(jobs[j_c])
                # Dacă era în execuție, îl opresc
                if job_current_machine[j_c] is not None:
                    m_stop = job_current_machine[j_c]
                    active_ops[m_stop] = None
                    job_current_machine[j_c] = None

        # 3) Verificăm dacă începe un breakdown la momentul curent => întrerupem operația
        for m in range(num_machines):
            for (bd_s, bd_e) in breakdowns_per_machine[m]:
                if bd_s == time:
                    # Dacă mașina m executa ceva => se întrerupe
                    if active_ops[m] is not None:
                        (jop, opidx, st, rem) = active_ops[m]
                        # Jobul jop pierde tot progresul
                        active_ops[m] = None
                        job_current_machine[jop] = None
                    break  # nu mai verificăm alte breakdown-uri care încep la același moment

        # 4) Actualizăm operațiile în curs (mașinile care nu sunt în breakdown)
        for m in range(num_machines):
            if active_ops[m] is not None:
                # Dacă mașina e în breakdown => nu consumăm timp
                if machine_in_breakdown(m, time):
                    continue
                # consumăm 1 unitate de timp din execuția operației
                (jop, opidx, st, rem) = active_ops[m]
                new_rem = rem - 1
                if new_rem <= 0:
                    # S-a terminat la finalul acestei unități
                    finish_t = time + 1
                    job_progress[jop] += 1

                    # Operația s-a încheiat, jobul poate începe următoarea operație doar după finish_t
                    job_earliest_start[jop] = finish_t

                    # Înregistrăm în schedule
                    schedule.append((jop, opidx, m, st, finish_t))
                    # Eliberăm mașina
                    active_ops[m] = None
                    job_current_machine[jop] = None
                else:
                    active_ops[m] = (jop, opidx, st, new_rem)

        # 5) Pe mașinile LIBERE și fără breakdown, încercăm să lansăm o operație
        for m in range(num_machines):
            if active_ops[m] is None and not machine_in_breakdown(m, time):
                # Căutăm jobul care are cea mai bună prioritate pe mașina m
                best_j = None
                best_op = None
                best_pt = None
                best_prio = None

                for j_id in range(len(jobs)):
                    # job neterminat?
                    if job_progress[j_id] < len(jobs[j_id]):
                        # Are deja o operație pe altă mașină?
                        if job_current_machine[j_id] is not None:
                            continue
                        # Verificăm dacă ne aflăm înainte de momentul minim la care jobul poate porni
                        if time < job_earliest_start[j_id]:
                            continue

                        # Operația curentă
                        opidx = job_progress[j_id]
                        alt_ops = jobs[j_id][opidx]  # list[(m_alt, pt)]
                        for (m_alt, pt_alt) in alt_ops:
                            if m_alt == m:
                                # calculăm priority
                                pr = compute_classic_priority(rule_name,
                                                              j_id,
                                                              opidx,
                                                              m_alt,
                                                              pt_alt,
                                                              time,
                                                              due_dates)
                                if best_prio is None or pr < best_prio:
                                    best_prio = pr
                                    best_j = j_id
                                    best_op = opidx
                                    best_pt = pt_alt

                # Dacă am găsit un job => pornim execuția
                if best_j is not None:
                    active_ops[m] = (best_j, best_op, time, best_pt)
                    job_current_machine[best_j] = m

        # Incrementează timpul
        time += 1

    # Calculăm makespan
    makespan = 0
    for (j, opidx, m, s_t, e_t) in schedule:
        if e_t > makespan:
            makespan = e_t

    return makespan, schedule


###############################################################################
# 4) Plot Gantt cu breakdown roșu
###############################################################################
def plot_gantt(schedule, num_machines, breakdowns, title="Gantt Chart"):
    fig, ax = plt.subplots(figsize=(10, 6))

    # Evidențiem zonele de breakdown
    for m in range(num_machines):
        bds = breakdowns.get(m, [])
        for (bd_s, bd_e) in bds:
            dur = bd_e - bd_s
            ax.barh(m, dur, left=bd_s, height=0.8, color='red', alpha=0.3)

    # Desenăm operațiile
    colors = plt.cm.get_cmap('tab10', 10)
    for (job_id, op_idx, machine_id, s_t, f_t) in schedule:
        dur = f_t - s_t
        ax.barh(machine_id, dur, left=s_t,
                color=colors(job_id % 10),
                edgecolor='black',
                height=0.6)
        ax.text(s_t + dur/2, machine_id,
                f"J{job_id}-O{op_idx}",
                ha="center", va="center", color="white", fontsize=7)

    ax.set_xlabel("Time")
    ax.set_ylabel("Machine")
    ax.set_yticks(range(num_machines))
    ax.set_title(title)

    patch_bd = mpatches.Patch(color='red', alpha=0.3, label='Breakdown')
    ax.legend(handles=[patch_bd])

    plt.tight_layout()
    plt.show()


###############################################################################
# 5) MAIN
###############################################################################
if __name__ == "__main__":
    # Exemplu: citim un fișier test (trebuie să existe la calea specificată)
    test_file = "dynamic-FJSP-instances/barnes/mt10c1_dynamic_1.txt"
    if not os.path.exists(test_file):
        print(f"Fișierul {test_file} nu există!")
        exit(1)

    # 1) Citim instanța
    num_jobs, num_machines, jobs, events = read_dynamic_fjsp_instance(test_file)

    # 2) Reguli de test (clasic)
    rules = ["SPT", "LPT", "EDD", "Random"]

    for rule in rules:
        # Copiem structurile (ca să nu le modificăm permanent)
        jb_copy = copy.deepcopy(jobs)
        ev_copy = {
            "breakdowns": {m: list(bds) for m, bds in events["breakdowns"].items()},
            "added_jobs": list(events["added_jobs"]),
            "cancelled_jobs": list(events["cancelled_jobs"])
        }

        # 3) Planificăm
        makespan, schedule = schedule_dynamic_no_parallel(
            jb_copy, num_machines, ev_copy, rule
        )
        print(f"[{rule}] => Makespan = {makespan}")

        # 4) Afișăm Gantt
        plot_gantt(schedule, num_machines, ev_copy["breakdowns"],
                   title=f"{rule} => makespan={makespan}")
