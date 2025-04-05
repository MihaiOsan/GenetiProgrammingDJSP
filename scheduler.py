class MachineState:
    """
    Clasă simplă pentru reținerea stării unei mașini.
    Include un camp `id` pentru a evita `machines.index(...)`.
    """

    def __init__(self, machine_id):
        self.id = machine_id
        self.busy = False
        self.job_id = None
        self.op_idx = None
        self.time_remaining = 0
        self.broken_until = 0   # mașina e defectă până la acest timp
        self.start_time = 0     # momentul la care a început operația curentă
        self.idle_since = 0     # momentul când a devenit ultima dată liberă


def evaluate_individual(individual, jobs, num_machines, events, toolbox, max_time=999999):
    """
    Rulează simularea discretă a FJSP (inclusiv evenimente dinamice)
    folosind regula de dispecerizare compilată din `individual` (GP).
    Returnează (makespan, schedule).
    """
    # Parametru limită opțional pentru a nu itera la nesfârșit
    MAX_TIME_LIMIT = 200_000

    # ------------------------------------------------------------
    # 1) Compilăm regula de dispecerizare din individul GP
    # ------------------------------------------------------------
    dispatch_rule = toolbox.compile(expr=individual)

    # ------------------------------------------------------------
    # 2) Transformăm breakdowns/added_jobs/cancelled_jobs într-o listă de evenimente
    # ------------------------------------------------------------
    event_list = []
    # Adunăm breakdowns
    for m_id, bd_list in events["breakdowns"].items():
        for (bd_start, bd_end) in bd_list:
            event_list.append((bd_start, "breakdown", m_id, bd_end))

    # Added jobs
    for (add_time, job_ops) in events["added_jobs"]:
        event_list.append((add_time, "added_job", job_ops))

    # Cancelled jobs
    for (cancel_time, job_id) in events["cancelled_jobs"]:
        event_list.append((cancel_time, "cancel_job", job_id))

    # Sortăm după momentul de start
    event_list.sort(key=lambda e: e[0])

    # ------------------------------------------------------------
    # 3) Inițializare stări
    # ------------------------------------------------------------
    # (A) Creăm mașinile cu ID clar
    machines = [MachineState(m) for m in range(num_machines)]

    # (B) Timpul de final pentru fiecare job
    job_end_time = [0.0] * len(jobs)

    # (C) Calculăm lungimea joburilor pentru a nu face len(jobs[j]) de zeci de ori
    len_jobs = [len(job) for job in jobs]

    # (D) Operații pregătite => set, plus un dict pt moment ready_time
    ready_ops = set()
    ready_time = {}
    for j, job in enumerate(jobs):
        if job:  # job nu e gol
            ready_ops.add((j, 0))
            ready_time[(j, 0)] = 0.0

    cancelled_jobs = set()

    # (E) Cache RPT
    rpt_cache = {}

    event_idx = 0
    current_time = 0
    completed_ops = 0
    total_ops = sum(len_j for len_j in len_jobs)  # totalul de operații
    schedule = []

    # ------------------------------------------------------------
    # Funcții ajutătoare
    # ------------------------------------------------------------
    def make_op_ready(j, op, t):
        """Adaugă o operație (j, op) în ready_ops și reține momentul când devine ready."""
        ready_ops.add((j, op))
        ready_time[(j, op)] = t

    def compute_rpt(job_id, op_idx):
        """Calculează Remaining Processing Time (RPT) cu caching."""
        key = (job_id, op_idx)
        if key not in rpt_cache:
            # Suma minimelor de la op_idx până la final
            # (de ex. sum(min(...) for k in range(op_idx, len_jobs[job_id])))
            s = 0
            for k in range(op_idx, len_jobs[job_id]):
                s += min(p for (_, p) in jobs[job_id][k])
            rpt_cache[key] = s
        return rpt_cache[key]

    def add_new_job(job_ops, t):
        """Adaugă un nou job, pune prima operație ca ready, etc."""
        new_jid = len(jobs)
        jobs.append(job_ops)
        job_end_time.append(0.0)
        len_jobs.append(len(job_ops))  # actualizăm array-ul de lungimi
        make_op_ready(new_jid, 0, t)

    # ------------------------------------------------------------
    # 4) Bucla principală
    # ------------------------------------------------------------
    while current_time < max_time:


        # (A) Activăm evenimentele la current_time
        while event_idx < len(event_list) and event_list[event_idx][0] == current_time:
            ev_time, ev_type, *ev_data = event_list[event_idx]

            if ev_type == "breakdown":
                m_id, bd_end = ev_data
                machine = machines[m_id]
                machine.broken_until = bd_end
                if machine.busy:
                    # Operația se întrerupe, reintroducem jobul + op
                    make_op_ready(machine.job_id, machine.op_idx, current_time)
                    machine.busy = False
                    machine.idle_since = current_time

            elif ev_type == "added_job":
                add_new_job(ev_data[0], current_time)

            elif ev_type == "cancel_job":
                job_id = ev_data[0]
                cancelled_jobs.add(job_id)
                for mach in machines:
                    if mach.busy and mach.job_id == job_id:
                        mach.busy = False
                        mach.idle_since = current_time
                ready_ops = {(jj, oo) for (jj, oo) in ready_ops if jj != job_id}

            event_idx += 1

        # (B) Actualizăm starea fiecărei mașini pentru 1 unitate de timp
        for machine in machines:
            # local var
            m_id = machine.id

            # mașina e defectă?
            if machine.broken_until > current_time:
                continue

            # Dacă e ocupată
            if machine.busy:
                machine.time_remaining -= 1
                if machine.time_remaining <= 0:
                    jdone, odone = machine.job_id, machine.op_idx
                    start_op, end_op = machine.start_time, current_time

                    machine.busy = False
                    machine.idle_since = current_time
                    completed_ops += 1
                    job_end_time[jdone] = end_op
                    schedule.append((jdone, odone, m_id, start_op, end_op))

                    # Dacă mai avem operații
                    if odone + 1 < len_jobs[jdone] and jdone not in cancelled_jobs:
                        make_op_ready(jdone, odone + 1, current_time)

            # Dacă e liberă și nu defectă
            if not machine.busy and machine.broken_until <= current_time:
                WIP_val = sum(1 for m2 in machines if m2.busy)
                MW_val = current_time - machine.idle_since
                TUF = max(0, machine.broken_until - current_time)

                best_candidate = None
                best_priority = float('inf')

                for (jj, oo) in ready_ops:
                    if jj in cancelled_jobs:
                        continue
                    alt_list = jobs[jj][oo]
                    ptime = None
                    for (mach, p) in alt_list:
                        if mach == m_id:
                            ptime = p
                            break

                    if ptime is not None:
                        PT_val = ptime
                        RO_val = len_jobs[jj] - oo - 1
                        TQ_val = current_time - ready_time.get((jj, oo), 0.0)
                        RPT_val = compute_rpt(jj, oo)
                        priority = dispatch_rule(
                            PT_val, RO_val, MW_val, TQ_val,
                            WIP_val, RPT_val, TUF
                        )

                        if priority < best_priority:
                            best_priority = priority
                            best_candidate = (jj, oo, ptime)

                # Dacă am un best_candidate, aloc
                if best_candidate is not None:
                    jj, oo, ptime = best_candidate
                    machine.busy = True
                    machine.job_id = jj
                    machine.op_idx = oo
                    machine.time_remaining = ptime
                    machine.start_time = current_time
                    ready_ops.remove((jj, oo))

        # (C) Verificăm dacă am terminat tot
        if completed_ops == total_ops:
            break

        # (D) Incrementăm timpul
        current_time += 1

    # ------------------------------------------------------------
    # 5) Makespan (max job_end_time pt joburile neanulate)
    # ------------------------------------------------------------
    makespan = 0
    for j_id in range(len_jobs.__len__()):
        if j_id not in cancelled_jobs:
            if job_end_time[j_id] > makespan:
                makespan = job_end_time[j_id]

    return makespan, schedule
