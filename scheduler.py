import copy


class MachineState:
    """
    Clasă simplă pentru reținerea stării unei mașini.
    """

    def __init__(self):
        self.busy = False
        self.job_id = None
        self.op_idx = None
        self.time_remaining = 0
        self.broken_until = 0  # mașina e defectă până la acest timp
        self.start_time = 0  # momentul la care a început operația curentă
        self.idle_since = 0  # momentul când a devenit ultima dată liberă


def evaluate_individual(individual, jobs, num_machines, events, toolbox, max_time=999999):
    """
    Rulează simularea discretă a FJSP (inclusiv evenimente dinamice)
    folosind regula de dispecerizare compilată din `individual` (GP).
    Returnează (makespan, schedule).

    Variabile folosite în dispatch_rule:
    1) PT  (Processing Time)
    2) RO  (Remaining Operations)
    3) MW  (Machine Wait)
    4) TQ  (Time in Queue)
    5) WIP (Work In Progress)
    6) RPT (Remaining Processing Time pentru job)
    7) TUF (Time Until Fixed)
    """

    # ------------------------------------------------------------
    # 0) Funcții ajutătoare
    # ------------------------------------------------------------
    def make_op_ready(j, op, t):
        """
        Pune operația (j, op) în coada ready_ops
        și reține momentul t când a devenit ready.
        """
        ready_ops.append((j, op))
        ready_time[(j, op)] = t

    def compute_rpt(job_id, op_idx):
        """
        Calculează Remaining Processing Time (RPT)
        ca suma timpilor MINIMI de pe operațiile rămase în job,
        de la op_idx (inclusiv) până la sfârșit.
        """
        total = 0
        for k in range(op_idx, len(jobs[job_id])):
            # jobs[job_id][k] = listă de (mașină, timp_proc)
            alt_times = [p for (_, p) in jobs[job_id][k]]
            if alt_times:
                total += min(alt_times)
        return total

    # ------------------------------------------------------------
    # 1) Compilăm regula de dispecerizare din individul GP
    # ------------------------------------------------------------
    dispatch_rule = toolbox.compile(expr=individual)

    # ------------------------------------------------------------
    # 2) Transformăm breakdowns/added_jobs/cancelled_jobs într-o listă de evenimente
    # ------------------------------------------------------------
    event_list = []
    for m_id, bd_list in events['breakdowns'].items():
        for (bd_start, bd_end) in bd_list:
            event_list.append((bd_start, "breakdown", m_id, bd_end))

    for (add_time, job_ops) in events['added_jobs']:
        event_list.append((add_time, "added_job", job_ops))

    for (cancel_time, job_id) in events['cancelled_jobs']:
        event_list.append((cancel_time, "cancel_job", job_id))

    event_list.sort(key=lambda e: e[0])
    event_idx = 0
    total_events = len(event_list)

    # ------------------------------------------------------------
    # 3) Inițializare stări
    # ------------------------------------------------------------
    machines = [MachineState() for _ in range(num_machines)]
    job_end_time = [0.0] * len(jobs)

    # Ținem minte când fiecare operație devine ready (pt. a calcula TQ)
    ready_time = {}

    # Inițial, adăugăm (job, op_idx=0) în coadă
    ready_ops = []
    for j in range(len(jobs)):
        if len(jobs[j]) > 0:
            make_op_ready(j, 0, 0.0)  # devine ready la t=0

    cancelled_jobs = set()

    def add_new_job(job_ops, t):
        new_jid = len(jobs)
        jobs.append(job_ops)
        job_end_time.append(0.0)
        # Operația 0 a jobului nou devine ready la momentul curent t
        make_op_ready(new_jid, 0, t)

    current_time = 0
    completed_ops = 0
    total_ops = sum(len(j) for j in jobs)
    schedule = []

    # ------------------------------------------------------------
    # 4) Bucla principală
    # ------------------------------------------------------------
    while current_time < max_time:
        # (A) Activăm evenimentele care apar la current_time
        while event_idx < total_events and event_list[event_idx][0] == current_time:
            ev_time, ev_type = event_list[event_idx][0], event_list[event_idx][1]

            if ev_type == "breakdown":
                # (ev_time, "breakdown", machine_id, bd_end)
                _, _, m_id, bd_end = event_list[event_idx]
                machines[m_id].broken_until = bd_end
                if machines[m_id].busy:
                    j_can = machines[m_id].job_id
                    o_can = machines[m_id].op_idx
                    if j_can not in cancelled_jobs:
                        # Reintroducem operația în coada de ready_ops
                        make_op_ready(j_can, o_can, current_time)
                # Eliberăm mașina
                machines[m_id].busy = False
                machines[m_id].job_id = None
                machines[m_id].op_idx = None
                machines[m_id].time_remaining = 0
                machines[m_id].start_time = 0
                machines[m_id].idle_since = current_time

            elif ev_type == "added_job":
                # (ev_time, "added_job", job_ops)
                _, _, new_job_ops = event_list[event_idx]
                add_new_job(new_job_ops, current_time)
                total_ops += len(new_job_ops)

            elif ev_type == "cancel_job":
                # (ev_time, "cancel_job", job_id)
                _, _, job_id = event_list[event_idx]
                cancelled_jobs.add(job_id)
                # Eliberăm mașinile care lucrau la jobul respectiv
                for ms in machines:
                    if ms.busy and ms.job_id == job_id:
                        ms.busy = False
                        ms.job_id = None
                        ms.op_idx = None
                        ms.time_remaining = 0
                        ms.start_time = 0
                        ms.idle_since = current_time
                # Scoatem din ready_ops
                ready_ops = [(jj, oo) for (jj, oo) in ready_ops if jj != job_id]

            event_idx += 1

        # (B) Actualizăm starea fiecărei mașini pentru 1 unitate de timp
        for m_id, ms in enumerate(machines):
            # Dacă e defectă până la un moment > current_time, nu face nimic
            if ms.broken_until > current_time:
                continue

            # Dacă e ocupată, consumăm 1 unitate de timp din time_remaining
            if ms.busy:
                ms.time_remaining -= 1
                if ms.time_remaining <= 0:
                    # Operație finalizată
                    jdone = ms.job_id
                    odone = ms.op_idx
                    start_op = ms.start_time
                    end_op = current_time

                    ms.busy = False
                    ms.job_id = None
                    ms.op_idx = None
                    ms.time_remaining = 0
                    ms.start_time = 0
                    ms.idle_since = current_time

                    completed_ops += 1
                    job_end_time[jdone] = end_op
                    schedule.append((jdone, odone, m_id, start_op, end_op))

                    # Dacă jobul mai are operații
                    if odone + 1 < len(jobs[jdone]) and jdone not in cancelled_jobs:
                        make_op_ready(jdone, odone + 1, current_time)

            # Dacă e liberă și nu defectă -> planificăm o nouă operație
            if not ms.busy and ms.broken_until <= current_time:
                # Calculăm TUF (Time Until Fixed) = cât mai durează reparația
                # Dacă mașina nu e în breakdown, e 0
                TUF = max(0, ms.broken_until - current_time)

                best_candidate = None
                best_priority = float('inf')

                # WIP = nr. mașini care sunt busy (o definiție simplă)
                WIP_val = sum(1 for m2 in machines if m2.busy)

                # MW = Machine Wait = cât timp a stat mașina asta liberă
                # (de la ultima oparție finalizată)
                MW_val = current_time - ms.idle_since

                new_ready = []
                removed_indexes = set()  # ca să eliminăm o singură dată pe cei alocați

                for idx, (jj, oo) in enumerate(ready_ops):
                    if jj in cancelled_jobs:
                        continue

                    # Vedem dacă operația (jj, oo) se poate face pe mașina m_id
                    alt_list = jobs[jj][oo]
                    ptime = None
                    for (mach, pt) in alt_list:
                        if mach == m_id:
                            ptime = pt
                            break
                    if ptime is not None:
                        # Calculăm cele 7 variabile
                        PT_val = float(ptime)

                        # RO = nr. de operații rămase DUPĂ aceasta
                        RO_val = float(len(jobs[jj]) - oo - 1)

                        # TQ = current_time - momentul când (jj,oo) a devenit ready
                        # (stocat în ready_time)
                        TQ_val = current_time - ready_time.get((jj, oo), 0)

                        # RPT = remaining processing time (inclusiv op. curentă)
                        RPT_val = compute_rpt(jj, oo)

                        # TUF_val = TUF deja calculat (cât timp până la reparare)
                        TUF_val = TUF

                        # Apelăm dispatch_rule
                        priority = dispatch_rule(
                            PT_val,  # PT
                            RO_val,  # RO
                            MW_val,  # MW
                            TQ_val,  # TQ
                            WIP_val,  # WIP
                            RPT_val,  # RPT
                            TUF_val  # TUF
                        )

                        if priority < best_priority:
                            best_priority = priority
                            best_candidate = (jj, oo, ptime, idx)

                if best_candidate is not None:
                    (jj, oo, ptime, idx_in_ready) = best_candidate
                    # Alocăm operația
                    ms.busy = True
                    ms.job_id = jj
                    ms.op_idx = oo
                    ms.time_remaining = ptime
                    ms.start_time = current_time
                    # Re-setăm idle_since, devine ocupată
                    # (poți lăsa 0 sau current_time, depinde logică)
                    ms.idle_since = 0

                    # Scoatem (jj, oo) din ready_ops o singură dată
                    # (cel mai simplu: construim o listă nouă, omitem idx_in_ready)
                    new_ready_ops = []
                    removed_one = False
                    for i, rop in enumerate(ready_ops):
                        if i == idx_in_ready and not removed_one:
                            removed_one = True
                        else:
                            new_ready_ops.append(rop)
                    ready_ops = new_ready_ops

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
