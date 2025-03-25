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
        self.broken_until = 0
        self.start_time = 0

def evaluate_individual(individual, jobs, num_machines, events, toolbox, max_time=999999):
    """
    Rulează simularea discretă a FJSP (inclusiv evenimente dinamice)
    folosind regula de dispecerizare compilată din `individual` (GP).
    Returnează (makespan, schedule).
    """
    # 1) Compilăm regula de dispecerizare
    dispatch_rule = toolbox.compile(expr=individual)

    # 2) Transformăm breakdowns/added_jobs/cancelled_jobs într-o listă de evenimente
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

    # 3) Inițializare
    machines = [MachineState() for _ in range(num_machines)]
    job_end_time = [0.0] * len(jobs)
    ready_ops = [(j, 0) for j in range(len(jobs)) if len(jobs[j]) > 0]
    cancelled_jobs = set()

    def add_new_job(job_ops):
        new_jid = len(jobs)
        jobs.append(job_ops)
        job_end_time.append(0.0)
        ready_ops.append((new_jid, 0))

    current_time = 0
    completed_ops = 0
    total_ops = sum(len(j) for j in jobs)
    schedule = []

    # 4) Bucla principală
    while current_time < max_time:
        # (A) Evenimente la current_time
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
                for ms in machines:
                    if ms.busy and ms.job_id == job_id:
                        ms.busy = False
                        ms.job_id = None
                        ms.op_idx = None
                        ms.time_remaining = 0
                        ms.start_time = 0
                ready_ops = [(jj, oo) for (jj, oo) in ready_ops if jj != job_id]

            event_idx += 1

        # (B) Update stare mașini pentru 1 unitate de timp
        for m_id, ms in enumerate(machines):
            if ms.broken_until > current_time:
                # Mașina e defectă
                continue

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

                    completed_ops += 1
                    job_end_time[jdone] = end_op
                    schedule.append((jdone, odone, m_id, start_op, end_op))

                    if odone + 1 < len(jobs[jdone]) and jdone not in cancelled_jobs:
                        ready_ops.append((jdone, odone + 1))

            if not ms.busy and ms.broken_until <= current_time:
                # Selectăm o operație pe care să o planificăm (dacă există)
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
                        MW = 0.0  # mașina e liberă
                        priority = dispatch_rule(PT, RO, MW)
                        if priority < best_priority:
                            best_priority = priority
                            best_candidate = (jj, oo, ptime)

                if best_candidate is not None:
                    (jj, oo, ptime) = best_candidate
                    ms.busy = True
                    ms.job_id = jj
                    ms.op_idx = oo
                    ms.time_remaining = ptime
                    ms.start_time = current_time

                    # Scoatem o singură dată (jj, oo) din ready_ops
                    removed_once = False
                    new_ready = []
                    for (rj, ro) in ready_ops:
                        if not removed_once and rj == jj and ro == oo:
                            removed_once = True
                        else:
                            new_ready.append((rj, ro))
                    ready_ops = new_ready

        # (C) Verificăm finalizare
        if completed_ops == total_ops:
            break

        # (D) Incrementăm timpul
        current_time += 1

    # 5) Makespan
    makespan = max(job_end_time[j] for j in range(len(jobs)) if j not in cancelled_jobs) \
               if cancelled_jobs else max(job_end_time)
    return makespan, schedule
