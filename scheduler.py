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
    # (A) Creăm mașinile
    machines = [MachineState(m) for m in range(num_machines)]

    # (B) Timpul de final pentru fiecare job
    job_end_time = [0.0] * len(jobs)

    # (C) Calculăm lungimea joburilor
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
    # Calculul initial ramane - pentru joburile de start
    total_ops = sum(len_j for len_j in len_jobs)
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
            s = 0
            # Verificam daca job_id este valid inainte de a accesa len_jobs si jobs
            if 0 <= job_id < len(len_jobs):
                for k in range(op_idx, len_jobs[job_id]):
                    # Tratare posibilă eroare dacă jobs[job_id][k] e goală
                    if jobs[job_id][k]:
                        s += min(p for (_, p) in jobs[job_id][k])
                    else:
                        # Poate loga un warning sau ignora? Depinde de cum sunt definite joburile.
                        # Presupunem ca o operatie are mereu alternative.
                        pass
            rpt_cache[key] = s
        return rpt_cache.get(key, 0)  # Returneaza 0 daca key nu exista (ex. job anulat?)

    def add_new_job(job_ops, t):
        """Adaugă un nou job, pune prima operație ca ready, etc."""
        # --- MODIFICARE: Declarăm nonlocal pentru a modifica total_ops din scope-ul exterior ---
        nonlocal total_ops

        new_jid = len(jobs)
        jobs.append(job_ops)
        job_end_time.append(0.0)
        num_new_ops = len(job_ops)  # Numarul de operatii pt noul job
        len_jobs.append(num_new_ops)  # actualizăm array-ul de lungimi

        # --- MODIFICARE: Actualizăm numărul total de operații așteptate ---
        total_ops += num_new_ops

        # Punem prima operatie ca ready doar daca jobul are operatii
        if num_new_ops > 0:
            make_op_ready(new_jid, 0, t)

    # ------------------------------------------------------------
    # 4) Bucla principală
    # ------------------------------------------------------------
    while current_time < max_time:  # Folosim max_time, nu MAX_TIME_LIMIT aici

        # --- Limitare opțională a timpului de simulare ---
        if current_time > MAX_TIME_LIMIT:
            print(f"   Warning: Simulation time limit ({MAX_TIME_LIMIT}) reached. Aborting.")
            # Putem returna un makespan infinit sau o valoare foarte mare
            # pentru a penaliza soluțiile care nu termină rapid.
            # Sau putem calcula makespan-ul curent. Alegem a doua varianta.
            break  # Iese din bucla while

        # (A) Activăm evenimentele la current_time
        while event_idx < len(event_list) and event_list[event_idx][
            0] <= current_time:  # Folosim <= pentru a prinde evenimente la t=0
            ev_time, ev_type, *ev_data = event_list[event_idx]

            # Procesam doar evenimentele EXACT la current_time
            if ev_time == current_time:
                if ev_type == "breakdown":
                    m_id, bd_end = ev_data
                    machine = machines[m_id]
                    # Setam broken_until doar daca noua valoare e mai mare decat cea existenta
                    # pentru a gestiona suprapuneri (desi sortarea ar trebui sa ajute)
                    machine.broken_until = max(machine.broken_until, bd_end)

                    if machine.busy:
                        # Verificam daca operatia curenta e afectata (a inceput inainte sau la current_time)
                        # Daca e afectata, o intrerupem.
                        # (Logica actuala o intrerupe oricum, ceea ce e probabil OK - presupune ca breakdown-ul e instant)
                        make_op_ready(machine.job_id, machine.op_idx, current_time)
                        # Resetam si cache-ul RPT pentru jobul afectat? Depinde de politica. Nu facem asta acum.
                        machine.busy = False
                        machine.job_id = None
                        machine.op_idx = None
                        machine.time_remaining = 0
                        machine.start_time = 0
                        machine.idle_since = current_time  # Devine idle acum

                elif ev_type == "added_job":
                    add_new_job(ev_data[0], current_time)

                elif ev_type == "cancel_job":
                    job_id = ev_data[0]
                    if job_id not in cancelled_jobs:  # Anulam doar o data
                        cancelled_jobs.add(job_id)
                        # Oprim operatia daca ruleaza pe vreo masina
                        for mach in machines:
                            if mach.busy and mach.job_id == job_id:
                                mach.busy = False
                                mach.job_id = None
                                mach.op_idx = None
                                mach.time_remaining = 0
                                mach.start_time = 0
                                mach.idle_since = current_time
                        # Scoatem operatiile jobului anulat din coada ready
                        initial_ready_ops_count = len(ready_ops)
                        ready_ops = {(jj, oo) for (jj, oo) in ready_ops if jj != job_id}
                        removed_count = initial_ready_ops_count - len(ready_ops)
                        ops_remaining_for_cancelled_job = 0
                        current_op_idx = -1
                        # Gasim ultima operatie finalizata sau cea in curs (daca a fost intrerupta)
                        for j, o, m, s, e in schedule:
                            if j == job_id and o > current_op_idx:
                                current_op_idx = o
                        # Daca jobul rula, op_idx de pe masina e relevant
                        for mach in machines:
                            if mach.job_id == job_id:  # Ar trebui sa fie None acum, dar verificam istoric
                                # Acest caz nu ar trebui sa apara daca am facut reset mai sus
                                pass
                        # Daca nu a rulat nicio operatie, current_op_idx ramane -1

                        # Numarul de operatii ramase = total - (indexul ultimei finalizate + 1)
                        if 0 <= job_id < len(len_jobs):  # Verificare existenta job
                            total_ops_for_job = len_jobs[job_id]
                            ops_remaining_for_cancelled_job = total_ops_for_job - (current_op_idx + 1)

                            # Scadem operatiile ramase din total_ops
                            if ops_remaining_for_cancelled_job > 0:
                                total_ops -= ops_remaining_for_cancelled_job
                                # Trebuie sa ajustam si completed_ops? Nu, completed_ops numara ce s-a terminat efectiv.
                                # Dar trebuie sa avem grija la conditia de oprire.
                                # Daca un job e anulat si operatiile lui sunt scoase din total_ops,
                                # completed_ops va ajunge la noul total_ops cand celelalte joburi se termina.

            # Trecem la urmatorul eveniment din lista, indiferent daca a fost procesat sau nu la acest 'current_time'
            event_idx += 1

        # (B) Actualizăm starea fiecărei mașini
        for machine in machines:
            m_id = machine.id

            # Mașina e defectă sau devine defectă ACUM?
            # Verificam broken_until > current_time (strict mai mare)
            if machine.broken_until > current_time:
                # Daca era busy, ar fi trebuit sa fie oprita la evenimentul breakdown
                # Daca devine idle acum si e broken, ramane idle pana la broken_until
                continue  # Trecem la urmatoarea masina

            # Daca a fost defecta si ACUM nu mai e (broken_until == current_time)
            if machine.broken_until == current_time:
                machine.broken_until = 0  # Resetam starea broken
                machine.idle_since = current_time  # Devine disponibila acum

            # Dacă e ocupată și nu e defectă
            if machine.busy:
                machine.time_remaining -= 1
                if machine.time_remaining <= 0:
                    # Operatie finalizata
                    jdone, odone = machine.job_id, machine.op_idx
                    start_op, end_op = machine.start_time, current_time + 1  # Timpul include unitatea curenta

                    machine.busy = False
                    machine.job_id = None
                    machine.op_idx = None
                    machine.time_remaining = 0
                    machine.start_time = 0
                    machine.idle_since = current_time + 1  # Devine idle la INCEPUTUL urmatorului pas de timp

                    completed_ops += 1
                    job_end_time[jdone] = end_op
                    schedule.append((jdone, odone, m_id, start_op, end_op))

                    # Adăugăm operația următoare în ready_ops, dacă există și jobul nu e anulat
                    if odone + 1 < len_jobs[jdone] and jdone not in cancelled_jobs:
                        make_op_ready(jdone, odone + 1, end_op)  # Devine ready cand operatia anterioara se termina

            # Dacă e liberă și nu e defectă (verificăm broken_until <= current_time)
            if not machine.busy and machine.broken_until <= current_time:
                # Cautam cea mai buna operatie de alocat din ready_ops
                WIP_val = sum(1 for m2 in machines if m2.busy)  # Nr masini ocupate ACUM
                MW_val = (current_time + 1) - machine.idle_since  # Cat timp a stat idle pana la STARTUL urmatorului pas

                best_candidate = None
                best_priority = float('inf')

                # Iteram prin operatiile gata de executie
                ops_to_consider = list(ready_ops)  # Cream o copie pt iterare sigura
                for (jj, oo) in ops_to_consider:
                    # Skip daca jobul a fost anulat intre timp
                    if jj in cancelled_jobs:
                        ready_ops.remove((jj, oo))  # Curatam coada
                        continue

                    # Verificam daca operatia poate rula pe masina curenta (m_id)
                    alt_list = jobs[jj][oo]
                    ptime = None
                    for (mach_alt, p) in alt_list:
                        if mach_alt == m_id:
                            ptime = p
                            break  # Am gasit masina

                    if ptime is not None and ptime > 0:  # Daca masina e o alternativa valida si timpul > 0
                        PT_val = ptime
                        RO_val = len_jobs[jj] - oo - 1
                        TQ_val = (current_time + 1) - ready_time.get((jj, oo),
                                                                     0.0)  # Timpul de asteptare pana la urmatorul pas
                        RPT_val = compute_rpt(jj, oo)

                        # Apelam regula de dispecerizare compilata
                        try:
                            priority = dispatch_rule(
                                PT=PT_val, RO=RO_val, MW=MW_val, TQ=TQ_val,
                                WIP=WIP_val, RPT=RPT_val
                            )
                        except Exception as e:
                            # Prindem erori posibile din regula GP (ex: impartire la zero)
                            priority = float('inf')  # Penalizam aceasta optiune

                        # Actualizam candidatul cel mai bun
                        if priority < best_priority:
                            best_priority = priority
                            best_candidate = (jj, oo, ptime)
                        # TODO: Adaugare logica pentru tie-breaking

                # Alocăm cea mai bună operație găsită, dacă există
                if best_candidate is not None:
                    jj, oo, ptime = best_candidate
                    machine.busy = True
                    machine.job_id = jj
                    machine.op_idx = oo
                    machine.time_remaining = ptime  # Va incepe sa scada de la pasul urmator
                    machine.start_time = current_time + 1  # Operatia incepe la inceputul urmatorului pas de timp
                    # Scoatem operatia alocata din coada ready
                    ready_ops.remove((jj, oo))

        # (C) Verificăm dacă am terminat tot (folosind total_ops actualizat)
        # Conditia trebuie verificata DUPA ce s-au procesat finalizarea operatiilor din pasul curent
        # Verificam daca completed_ops a atins noul total_ops SI daca nu mai sunt operatii in coada ready
        # (ar putea exista operatii adaugate dinamic care nu au fost inca procesate)
        # O conditie mai sigura: toate joburile neanulate au fost completate
        all_done = True
        if completed_ops < total_ops:  # Verificare rapida
            all_done = False
        else:
            # Verificare mai amanuntita daca completed == total
            for j_idx in range(len(len_jobs)):
                if j_idx not in cancelled_jobs:
                    # Verificam daca ultima operatie a jobului a fost finalizata
                    last_op_idx = len_jobs[j_idx] - 1
                    is_job_j_finished = False
                    for schedule_entry in schedule:
                        if schedule_entry[0] == j_idx and schedule_entry[1] == last_op_idx:
                            is_job_j_finished = True
                            break
                    if not is_job_j_finished:
                        # Chiar daca completed==total, e posibil ca un job anulat
                        # sa fi redus total_ops si un job valid sa nu fie gata.
                        # Acest caz nu ar trebui sa apara cu logica de anulare corecta.
                        all_done = False
                        # print(f"Debug: Job {j_idx} not finished although completed_ops {completed_ops} == total_ops {total_ops}")
                        break  # Ajunge un job neterminat

        if all_done and not ready_ops:  # Adaugam si verificarea ca nu mai sunt operatii in asteptare
            break  # Am terminat

        # (D) Incrementăm timpul doar dacă nu am terminat
        current_time += 1

        # --- Debug Info (Optional) ---
        # if current_time % 100 == 0:
        #     print(f"Time: {current_time}, Completed: {completed_ops}/{total_ops}, Ready: {len(ready_ops)}")
        #     for m in machines:
        #         state = "Busy" if m.busy else ("Broken" if m.broken_until > current_time else "Idle")
        #         print(f"  M{m.id}: {state} (Job: {m.job_id}, Op: {m.op_idx}, Rem: {m.time_remaining}, BrkUntil: {m.broken_until})")

    # ------------------------------------------------------------
    # 5) Makespan (max job_end_time pt joburile neanulate)
    # ------------------------------------------------------------
    makespan = 0
    if not schedule and not cancelled_jobs and total_ops > 0:
        # Cazul in care simularea a atins limita de timp fara a programa nimic
        print("   Warning: Simulation limit reached before any operation completed or limit was too low.")
        # Returnam o valoare foarte mare sau limita atinsa?
        makespan = max_time  # Sau MAX_TIME_LIMIT
    else:
        for j_id in range(len(job_end_time)):  # Iteram prin lista originala si extinsa de joburi
            # Verificam indexul si daca jobul nu e anulat
            if j_id < len(len_jobs) and j_id not in cancelled_jobs:
                # Folosim timpul final din job_end_time, care e actualizat cand operatia se termina
                makespan = max(makespan, job_end_time[j_id])

    if makespan == 0 and any(j not in cancelled_jobs for j in range(len(len_jobs))):
        # Daca makespan e 0 dar existau joburi valide, inseamna ca simularea s-a oprit prematur
        # sau a atins limita MAX_TIME_LIMIT inainte de a finaliza ceva.
        # Returnam limita ca indicatie de esec?
        if current_time >= MAX_TIME_LIMIT:
            print(f"   Warning: Returning makespan=MAX_TIME_LIMIT ({MAX_TIME_LIMIT}) due to hitting simulation limit.")
            makespan = MAX_TIME_LIMIT
        else:
            # Ar putea fi cazul in care toate joburile au fost anulate inainte de a incepe?
            # Sau toate joburile aveau PT=0?
            print(f"   Warning: Makespan is 0, check simulation logic or input data. Current time: {current_time}")

    print(f"    Final Makespan: {makespan}. Total Ops Completed: {completed_ops}. Target Ops: {total_ops}.")
    return makespan, schedule