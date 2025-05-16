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


def evaluate_individual(individual, jobs, num_machines, events, toolbox, max_time=999999.0):
    """
    Rulează simularea discretă a FJSP (inclusiv evenimente dinamice și ETPC)
    folosind regula de dispecerizare compilată din `individual` (GP).
    Returnează (makespan, schedule).

    Presupuneri:
    - `jobs`: Lista inițială de joburi, unde fiecare job este un OpsList
              (List[List[Tuple[int, int]]]). Această listă este extinsă dinamic.
    - `events`: Un dicționar ce conține:
        - `events['breakdowns']`: {m_id: [(start, end), ...]}
        - `events['added_jobs']`: List[Tuple[ArrivalTime, OpsList]]
        - `events['cancelled_jobs']`: List[Tuple[CancelTime, SimJobIdx]]
        - `events['etpc_constraints']`: List[Dict], unde fiecare dicționar este
          {"fore_job": SimJobIdx1, "fore_op_idx": OpIdx1,
           "hind_job": SimJobIdx2, "hind_op_idx": OpIdx2, "time_lapse": N}.
          `SimJobIdx1` și `SimJobIdx2` sunt indecși de simulare.
        - `events['all_jobs_properties']` (opțional, folosit pentru maparea ID-urilor originale
          dacă `etpc_constraints` ar folosi ID-uri originale, dar NU este folosit direct
          în această implementare a ETPC dacă presupunem indecși de simulare în constrângeri).
    - `max_time`: Timpul maxim de simulare.
    """
    MAX_TIME_LIMIT = 200000.0  # Limita de siguranță a timpului de simulare
    dispatch_rule = toolbox.compile(expr=individual)

    # --- Inițializare și Pre-procesare ETPC ---
    etpc_map = {}  # (fore_job_sim_idx, fore_op_idx) -> List[(hind_job_sim_idx, hind_op_idx, time_lapse)]
    if 'etpc_constraints' in events and isinstance(events['etpc_constraints'], list):
        for constr in events['etpc_constraints']:
            try:
                # Validam tipurile si existenta cheilor pentru ETPC
                fj = int(constr['fore_job'])
                fo = int(constr['fore_op_idx'])
                hj = int(constr['hind_job'])
                ho = int(constr['hind_op_idx'])
                tl = float(constr['time_lapse'])  # Permitem float pentru lapse
                if tl < 0: tl = 0.0  # Timpul de decalaj nu poate fi negativ

                fore_key = (fj, fo)
                if fore_key not in etpc_map:
                    etpc_map[fore_key] = []
                etpc_map[fore_key].append((hj, ho, tl))
            except (KeyError, ValueError, TypeError) as e_etpc:
                print(f"   Warning: Skipping invalid ETPC constraint {constr}: {e_etpc}")

    min_start_due_to_etpc = {}
    job_internal_pred_finish_time = {}
    effective_ready_time = {}

    # --- Transformarea evenimentelor standard intr-o lista sortata ---
    event_list = []
    if "breakdowns" in events and isinstance(events["breakdowns"], dict):
        for m_id, bd_list in events["breakdowns"].items():
            if isinstance(bd_list, list):
                for item in bd_list:
                    if isinstance(item, tuple) and len(item) == 2:
                        bd_start, bd_end = item
                        event_list.append((float(bd_start), "breakdown", int(m_id), float(bd_end)))

    if "added_jobs" in events and isinstance(events["added_jobs"], list):
        for item in events["added_jobs"]:
            if isinstance(item, tuple) and len(item) == 2:
                add_time, job_ops_list_for_add = item
                # job_ops_list_for_add ar trebui sa fie OpsList
                if isinstance(job_ops_list_for_add, list):
                    event_list.append((float(add_time), "added_job", job_ops_list_for_add))

    if "cancelled_jobs" in events and isinstance(events["cancelled_jobs"], list):
        for item in events["cancelled_jobs"]:
            if isinstance(item, tuple) and len(item) == 2:
                cancel_time, job_id_to_cancel = item
                event_list.append((float(cancel_time), "cancel_job", int(job_id_to_cancel)))

    event_list.sort(key=lambda e: e[0])

    # --- Inițializare stări simulare ---
    machines = [MachineState(m) for m in range(num_machines)]

    # `jobs` este lista initiala de OpsList-uri, va fi extinsa.
    # Cream copii pentru a nu modifica lista originala din `instances`
    current_jobs_sim = [list(op_list) for op_list in jobs]  # Copie superficiala a OpsList-urilor
    job_end_time = [0.0] * len(current_jobs_sim)
    len_jobs = [len(job_op_l) for job_op_l in current_jobs_sim]

    ready_ops = set()

    for j_init_idx, job_op_list_init in enumerate(current_jobs_sim):
        if job_op_list_init:
            job_internal_pred_finish_time[(j_init_idx, 0)] = 0.0
            etpc_min_for_first_op = min_start_due_to_etpc.get((j_init_idx, 0), 0.0)
            effective_ready_time[(j_init_idx, 0)] = max(0.0, etpc_min_for_first_op)
            # Adaugam in ready_ops doar daca poate incepe la current_time (0) sau imediat dupa
            if effective_ready_time[(j_init_idx, 0)] <= 1e-9:  # Practic, la t=0
                ready_ops.add((j_init_idx, 0))
            # Daca ETPC impune un start mai tarziu, va fi adaugat cand current_time atinge effective_ready_time
            # Nu, mai bine il adaugam in ready_ops oricum, iar eligibilitatea se verifica in bucla de alocare

    cancelled_jobs_set = set()
    rpt_cache = {}
    event_idx = 0
    current_time = 0.0
    completed_ops = 0
    total_ops = sum(len_j for len_j in len_jobs)
    schedule = []

    # --- Funcții ajutătoare ---
    def make_op_ready(j_sim_idx, op_sim_idx, internal_pred_finish_time_val):
        job_internal_pred_finish_time[(j_sim_idx, op_sim_idx)] = float(internal_pred_finish_time_val)
        etpc_min_val = min_start_due_to_etpc.get((j_sim_idx, op_sim_idx), 0.0)
        actual_ready_time = max(float(internal_pred_finish_time_val), float(etpc_min_val))
        effective_ready_time[(j_sim_idx, op_sim_idx)] = actual_ready_time
        ready_ops.add((j_sim_idx, op_sim_idx))

    def compute_rpt(job_sim_idx, op_sim_idx):
        key = (job_sim_idx, op_sim_idx)
        if key not in rpt_cache:
            s = 0.0
            if 0 <= job_sim_idx < len(len_jobs):
                for k_op_idx in range(op_sim_idx, len_jobs[job_sim_idx]):
                    # current_jobs_sim[job_sim_idx][k_op_idx] este lista de alternative [(m,p), ...]
                    if current_jobs_sim[job_sim_idx][k_op_idx]:
                        s += min(float(p_time) for (_, p_time) in current_jobs_sim[job_sim_idx][k_op_idx])
            rpt_cache[key] = s
        return rpt_cache.get(key, 0.0)

    def add_new_job(new_job_ops_list_param, arrival_time_param):
        nonlocal total_ops
        new_sim_job_id_val = len(current_jobs_sim)
        current_jobs_sim.append(new_job_ops_list_param)
        job_end_time.append(0.0)
        num_new_ops_val = len(new_job_ops_list_param)
        len_jobs.append(num_new_ops_val)
        total_ops += num_new_ops_val
        if num_new_ops_val > 0:
            make_op_ready(new_sim_job_id_val, 0, float(arrival_time_param))

    # --- Bucla principală de simulare ---
    while current_time < float(max_time):
        if current_time > MAX_TIME_LIMIT:
            print(
                f"   Warning: Simulation time limit ({MAX_TIME_LIMIT:.2f}) reached. Makespan: {current_time:.2f}. Aborting.")
            break

        # (A) Activăm evenimentele la current_time
        while event_idx < len(event_list) and event_list[event_idx][0] <= current_time:
            ev_time, ev_type, *ev_data = event_list[event_idx]  # Extragem evenimentul curent

            if abs(ev_time - current_time) < 1e-9:  # Procesam doar evenimente exact la current_time
                event_idx += 1  # Consumam evenimentul DOAR daca e procesat
                if ev_type == "breakdown":
                    m_id, bd_end = ev_data
                    machine = machines[m_id]
                    machine.broken_until = max(machine.broken_until, bd_end)
                    if machine.busy and machine.start_time < machine.broken_until:
                        #print(f"   Time {current_time:.2f}: M{m_id} breakdown (until {bd_end:.2f}) interrupts J{machine.job_id} Op{machine.op_idx}")
                        make_op_ready(machine.job_id, machine.op_idx, current_time)
                        machine.busy = False;
                        machine.job_id = None;
                        machine.op_idx = None
                        machine.time_remaining = 0.0;
                        machine.start_time = 0.0
                        machine.idle_since = current_time
                elif ev_type == "added_job":
                    add_new_job(ev_data[0], current_time)
                elif ev_type == "cancel_job":
                    job_id_to_cancel = ev_data[0]
                    if job_id_to_cancel not in cancelled_jobs_set:
                        cancelled_jobs_set.add(job_id_to_cancel)
                        for mach_cancel in machines:
                            if mach_cancel.busy and mach_cancel.job_id == job_id_to_cancel:
                                mach_cancel.busy = False;
                                mach_cancel.job_id = None;
                                mach_cancel.op_idx = None
                                mach_cancel.time_remaining = 0.0;
                                mach_cancel.start_time = 0.0
                                mach_cancel.idle_since = current_time
                        ready_ops = {(jj, oo) for (jj, oo) in ready_ops if jj != job_id_to_cancel}
                        ops_done_for_cancelled = 0
                        for sched_entry in schedule:
                            if sched_entry[0] == job_id_to_cancel: ops_done_for_cancelled += 1
                        if 0 <= job_id_to_cancel < len(len_jobs):
                            total_ops_of_cancelled_job = len_jobs[job_id_to_cancel]
                            ops_not_done_and_will_not_be = total_ops_of_cancelled_job - ops_done_for_cancelled
                            if ops_not_done_and_will_not_be > 0: total_ops -= ops_not_done_and_will_not_be
            elif ev_time < current_time:  # Eveniment din trecut, skip si consuma
                print(f"   Warning: Skipping past event at time {ev_time:.2f} (current_time is {current_time:.2f})")
                event_idx += 1
            else:  # ev_time > current_time
                break  # Oprim procesarea evenimentelor pentru acest pas de timp

        # (B) Actualizăm starea mașinilor și finalizăm operații
        for machine in machines:
            m_id = machine.id
            if machine.broken_until > current_time + 1e-9: continue  # Daca e inca defecta in intervalul curent
            if abs(machine.broken_until - current_time) < 1e-9 and machine.broken_until != 0:
                # A fost defecta PANA ACUM (current_time), devine disponibila de la current_time
                machine.broken_until = 0.0
                if not machine.busy: machine.idle_since = current_time

            if machine.busy:
                machine.time_remaining -= 1.0
                if machine.time_remaining < 1e-9:  # Aproape de zero
                    jdone, odone = machine.job_id, machine.op_idx
                    start_op_time, end_op_time = machine.start_time, current_time + 1.0

                    machine.busy = False;
                    machine.job_id = None;
                    machine.op_idx = None
                    machine.time_remaining = 0.0;
                    machine.start_time = 0.0
                    machine.idle_since = end_op_time

                    completed_ops += 1
                    # Asiguram ca jdone este un index valid pentru job_end_time
                    if 0 <= jdone < len(job_end_time):
                        job_end_time[jdone] = end_op_time
                    else:  # Jobul a fost adaugat si job_end_time nu a fost extins corect - eroare de logica
                        print(f"   ERROR: jdone index {jdone} out of bounds for job_end_time (len {len(job_end_time)})")

                    schedule.append((jdone, odone, m_id, start_op_time, end_op_time))
                    # print(f"   Time {end_op_time:.2f}: J{jdone} Op{odone} END on M{m_id}. Comp: {completed_ops}/{total_ops}")

                    if (jdone, odone) in etpc_map:
                        for j_h_etpc, o_h_etpc, lapse_val_etpc in etpc_map[(jdone, odone)]:
                            new_min_start_for_hind_etpc = end_op_time + lapse_val_etpc
                            current_etpc_min_for_hind = min_start_due_to_etpc.get((j_h_etpc, o_h_etpc), 0.0)
                            min_start_due_to_etpc[(j_h_etpc, o_h_etpc)] = max(current_etpc_min_for_hind,
                                                                              new_min_start_for_hind_etpc)

                            if (j_h_etpc, o_h_etpc) in job_internal_pred_finish_time:
                                base_jprd_time_etpc = job_internal_pred_finish_time[(j_h_etpc, o_h_etpc)]
                                effective_ready_time[(j_h_etpc, o_h_etpc)] = max(base_jprd_time_etpc,
                                                                                 min_start_due_to_etpc[
                                                                                     (j_h_etpc, o_h_etpc)])

                    if odone + 1 < len_jobs[jdone] and jdone not in cancelled_jobs_set:
                        make_op_ready(jdone, odone + 1, end_op_time)

        # (C) Alocăm operații noi pe mașinile libere
        for machine in machines:
            m_id = machine.id
            if not machine.busy and machine.broken_until <= current_time + 1e-9:  # Daca e libera si nu e defecta (sau devine disponibila exact acum)
                WIP_val = sum(1 for m2_wip in machines if m2_wip.busy)
                MW_val = (current_time + 1.0) - machine.idle_since  # Cat timp va fi stat idle pana la startul urm op

                best_candidate_op_alloc = None
                best_priority_val_alloc = float('inf')

                current_ready_ops_list_alloc = list(ready_ops)
                for (jj_alloc, oo_alloc) in current_ready_ops_list_alloc:
                    if jj_alloc in cancelled_jobs_set:
                        if (jj_alloc, oo_alloc) in ready_ops: ready_ops.remove((jj_alloc, oo_alloc))
                        continue

                    op_effective_ready_t_alloc = effective_ready_time.get((jj_alloc, oo_alloc), float('inf'))
                    # Poate incepe la current_time + 1.0 (adica in intervalul [current_time, current_time+1))
                    if op_effective_ready_t_alloc > current_time + 1.0 - 1e-9:
                        continue

                    if not (0 <= jj_alloc < len(current_jobs_sim) and 0 <= oo_alloc < len_jobs[jj_alloc]):
                        # print(f"   Warning: Invalid job/op index ({jj_alloc},{oo_alloc}) in ready_ops. Skipping.")
                        if (jj_alloc, oo_alloc) in ready_ops: ready_ops.remove((jj_alloc, oo_alloc))
                        continue

                    current_op_alternatives_alloc = current_jobs_sim[jj_alloc][oo_alloc]
                    ptime_on_this_machine_alloc = None
                    for (mach_alt_idx_alloc, p_val_alloc) in current_op_alternatives_alloc:
                        if mach_alt_idx_alloc == m_id:
                            ptime_on_this_machine_alloc = float(p_val_alloc);
                            break

                    if ptime_on_this_machine_alloc is not None and ptime_on_this_machine_alloc > 1e-9:
                        PT_val = ptime_on_this_machine_alloc
                        RO_val = len_jobs[jj_alloc] - oo_alloc - 1.0
                        TQ_val = max(0.0, (current_time + 1.0) - op_effective_ready_t_alloc)
                        RPT_val = compute_rpt(jj_alloc, oo_alloc)

                        try:
                            priority = dispatch_rule(PT=PT_val, RO=RO_val, MW=MW_val, TQ=TQ_val, WIP=WIP_val,
                                                     RPT=RPT_val)
                        except Exception as e_dispatch:
                            priority = float('inf')

                        if priority < best_priority_val_alloc:
                            best_priority_val_alloc = priority
                            best_candidate_op_alloc = (jj_alloc, oo_alloc, ptime_on_this_machine_alloc)

                if best_candidate_op_alloc is not None:
                    jj_sel, oo_sel, ptime_sel = best_candidate_op_alloc
                    # print(f"   Time {current_time + 1.0:.2f}: Assign J{jj_sel} Op{oo_sel} (PT={ptime_sel:.2f}) to M{m_id} (Pri={best_priority_val_alloc:.2f})")
                    machine.busy = True
                    machine.job_id = jj_sel
                    machine.op_idx = oo_sel
                    machine.time_remaining = float(ptime_sel)
                    machine.start_time = current_time + 1.0
                    if (jj_sel, oo_sel) in ready_ops: ready_ops.remove((jj_sel, oo_sel))

        # (D) Verificăm condiția de terminare
        all_jobs_truly_completed = False  # Incepem cu fals
        if completed_ops >= total_ops:  # Conditie necesara, dar nu suficienta
            all_jobs_truly_completed = True  # Presupunem ca e adevarat si invalidam daca gasim un job neterminat
            for j_check_idx_final in range(len(len_jobs)):
                if j_check_idx_final not in cancelled_jobs_set:
                    if len_jobs[j_check_idx_final] > 0:
                        last_op_of_job_final = len_jobs[j_check_idx_final] - 1
                        # Verificam daca ultima operatie a fost programata
                        is_j_finished_in_schedule_final = False
                        for sched_j_f, sched_o_f, _, _, _ in schedule:
                            if sched_j_f == j_check_idx_final and sched_o_f == last_op_of_job_final:
                                is_j_finished_in_schedule_final = True;
                                break
                        if not is_j_finished_in_schedule_final:
                            all_jobs_truly_completed = False;
                            break

        if all_jobs_truly_completed and not ready_ops:
            # print(f"--- Simulation finished at time {current_time + 1.0:.2f} (all ops done and no ready ops) ---")
            break

        # (E) Incrementăm timpul
        current_time += 1.0

    # --- Calcul Makespan ---
    makespan = 0.0
    valid_job_existed_and_not_cancelled = False
    for j_id_final_mk in range(len(len_jobs)):  # len(len_jobs) poate fi mai mare decat len(job_end_time) initial
        if j_id_final_mk not in cancelled_jobs_set and len_jobs[j_id_final_mk] > 0:
            valid_job_existed_and_not_cancelled = True
            if j_id_final_mk < len(job_end_time):  # Asiguram ca accesam un index valid
                makespan = max(makespan, float(job_end_time[j_id_final_mk]))
            # Daca un job a fost adaugat dar nu a inceput/terminat, job_end_time[j_id_final_mk] va fi 0.0
            # Daca un job adaugat nu are nicio operatie finalizata, makespan nu va fi afectat de el direct
            # decat daca e singurul job si nu se intampla nimic.

    if not schedule and valid_job_existed_and_not_cancelled:  # Nimic programat desi existau joburi valide
        if current_time >= MAX_TIME_LIMIT - 1e-9:
            makespan = float(MAX_TIME_LIMIT)
        else:
            makespan = float(max_time)
        # print(f"   Warning: No operations scheduled, but valid jobs existed. Makespan set to {makespan:.2f}")

    if makespan == 0.0 and valid_job_existed_and_not_cancelled:
        if current_time >= MAX_TIME_LIMIT - 1e-9:
            # print(f"   Warning: Makespan is 0 but MAX_TIME_LIMIT was hit. Setting makespan to {MAX_TIME_LIMIT:.2f}")
            makespan = float(MAX_TIME_LIMIT)
        else:  # S-a terminat normal, dar makespan e 0 (poate toate joburile aveau timp 0?)
            # print(f"   Warning: Makespan is 0.0 but valid jobs existed. Sim time: {current_time:.2f}.")
            # Daca s-a terminat (completed_ops == total_ops) si ready_ops e goala,
            # un makespan de 0 e posibil daca toate timpii de procesare sunt 0.
            # Daca schedule e gol, inseamna ca nu s-a facut nimic.
            # Daca current_time e mic, e si mai suspect.
            pass

    # print(f"Final Makespan: {makespan:.2f}. Total Ops Completed: {completed_ops}. Target Ops (adjusted for cancels): {total_ops}.")
    return makespan, schedule