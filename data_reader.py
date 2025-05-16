import os
import json
import math # Needed for rounding arrival/start times if they are floats
import pprint

# ... (read_dynamic_fjsp_instance_txt rămâne la fel) ...
def read_dynamic_fjsp_instance_txt(file_path):
    """
    Reads a FJSP instance file in the original .txt format with dynamic events.
    Returns a tuple: (num_initial_jobs, num_machines, initial_jobs, dynamic_events).
    Returns (None, None, None, None) on error.
    """
    print(f"   Reading dynamic FJSP instance (.txt) from: {file_path}")
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"   Error: File not found at {file_path}")
        return None, None, None, None
    except IOError as e:
        print(f"   Error reading file {file_path}: {e}")
        return None, None, None, None

    if not lines:
        print(f"   Error: File {file_path} is empty.")
        return None, None, None, None

    # Initialize line index for error reporting outside the loop if header fails
    i = 1

    try:
        # --- Parse header ---
        header = lines[0].split()
        if len(header) < 2:
            raise ValueError("Header line must contain at least num_jobs and num_machines.")
        num_jobs_initial_spec, num_machines = map(int, header[:2])
        if num_jobs_initial_spec < 0 or num_machines <= 0:
             raise ValueError(f"Invalid number of jobs ({num_jobs_initial_spec}) or machines ({num_machines}) in header.")

        initial_jobs = []
        dynamic_events = {
            "breakdowns": {},  # Store as {machine_id: [(start1, end1), (start2, end2), ...]}
            "added_jobs": [],  # Store as [(arrival_time, job_ops_list), ...]
            "cancelled_jobs": [] # Store as [(cancel_time, initial_job_index), ...]
             # Vom adauga chei noi aici mai tarziu, dar initializam dictionarul standard
        }

        parsing_jobs = True
        parsing_breakdowns = False
        parsing_added_jobs = False
        parsing_cancelled_jobs = False
        job_counter = 0 # Count initial jobs parsed

        # --- Parse file content line by line ---
        for i, line in enumerate(lines[1:], start=2): # start=2 for line number in error messages
            line = line.strip()
            if not line or line.startswith("#"): # Skip empty lines and comments
                continue

            # --- Switch parsing mode based on headers ---
            if line.startswith("Dynamic Events"):
                parsing_jobs = False
                # Check if we parsed the expected number of initial jobs
                if job_counter != num_jobs_initial_spec:
                    print(f"   Warning: Expected {num_jobs_initial_spec} initial jobs based on header, but found {job_counter} job definition lines before 'Dynamic Events' in {file_path}.")
                    # Update the number of initial jobs to what was actually found
                    num_jobs_initial_spec = job_counter
                continue # Move to the next line after finding the header

            # Headers for dynamic event sections
            # Using "in" allows for optional text like "(x y z): Machine..."
            if "Machine Breakdowns" in line:
                parsing_jobs = False; parsing_breakdowns = True; parsing_added_jobs = False; parsing_cancelled_jobs = False
                continue
            if "Added Jobs" in line:
                parsing_jobs = False; parsing_breakdowns = False; parsing_added_jobs = True; parsing_cancelled_jobs = False
                continue
            if "Cancelled Jobs" in line:
                parsing_jobs = False; parsing_breakdowns = False; parsing_added_jobs = False; parsing_cancelled_jobs = True
                continue

            # --- Parse data based on current mode ---
            if parsing_jobs:
                if job_counter >= num_jobs_initial_spec:
                    # This line should belong to dynamic events or is an error
                    print(f"   Warning: Unexpected line while parsing initial jobs (line {i}): '{line}'. Expected 'Dynamic Events' or end of job definitions. Treating as potential dynamic event or error.")
                    parsing_jobs = False # Stop parsing jobs definitively
                    # Re-evaluate line for dynamic events (fall through)
                else:
                    # Parse an initial job line
                    parts = list(map(int, line.split()))
                    if not parts: raise ValueError(f"Empty job definition line {i}.")

                    num_operations = parts[0]
                    if num_operations <= 0: raise ValueError(f"Job must have at least one operation (line {i}).")
                    job_ops = []
                    idx = 1
                    current_op_index = 0
                    while current_op_index < num_operations:
                        if idx >= len(parts): raise ValueError(f"Incomplete job definition: missing number of alternatives for operation {current_op_index} (line {i}).")
                        num_alts = parts[idx]
                        if num_alts <= 0: raise ValueError(f"Operation must have at least one alternative machine (line {i}, op {current_op_index}).")
                        idx += 1
                        alt_list = []
                        for _ in range(num_alts):
                            if idx + 1 >= len(parts): raise ValueError(f"Incomplete job definition: missing machine/processing time pair (line {i}, op {current_op_index}).")
                            m = parts[idx]
                            p = parts[idx + 1]
                            if not (0 <= m < num_machines):
                                raise ValueError(f"Invalid machine index {m} (must be 0-{num_machines-1}) (line {i}, op {current_op_index}).")
                            if p < 0: raise ValueError(f"Processing time cannot be negative ({p}) (line {i}, op {current_op_index}).")
                            idx += 2
                            alt_list.append((m, p))
                        job_ops.append(alt_list)
                        current_op_index += 1
                    # Optional check: Make sure we consumed all parts for this job line
                    if idx != len(parts):
                        print(f"   Warning: Extra data found on job definition line {i}: {parts[idx:]}")

                    initial_jobs.append(job_ops)
                    job_counter += 1
                    continue # Move to next line after successfully parsing a job

            # --- Parse Dynamic Events ---
            # Ensure we are not in parsing_jobs mode anymore if we reach here without 'continue'
            if parsing_jobs:
                 # This case might happen if num_jobs_initial_spec was 0 in the header
                 # and we encounter non-comment/non-header lines before 'Dynamic Events'
                 print(f"   Warning: Found data line {i} ('{line}') while still expecting initial jobs (count {job_counter}/{num_jobs_initial_spec}). Switching to dynamic event parsing.")
                 parsing_jobs = False # Force exit from parsing jobs

            if parsing_breakdowns:
                parts = list(map(int, line.split()))
                if len(parts) != 3: raise ValueError(f"Invalid format for Machine Breakdown (expected 3 integers): '{line}' (line {i}).")
                machine, start, end = parts
                if not (0 <= machine < num_machines): raise ValueError(f"Invalid machine index {machine} in breakdown (line {i}).")
                if start < 0 or end < start: raise ValueError(f"Invalid breakdown interval [{start}, {end}] (line {i}).")
                # Use setdefault to initialize the list if the machine key doesn't exist
                dynamic_events['breakdowns'].setdefault(machine, []).append((start, end))

            elif parsing_added_jobs:
                # Format: time: num_ops num_alts m p m p ... num_alts m p ...
                try:
                    time_part_str, job_part_str = line.split(":", 1)
                    arrival_time = int(time_part_str.strip())
                    if arrival_time < 0: raise ValueError("Added job arrival time cannot be negative.")

                    job_part = list(map(int, job_part_str.split()))
                    if not job_part: raise ValueError("Empty job data for added job.")

                    num_operations = job_part[0]
                    if num_operations <= 0: raise ValueError("Added job must have at least one operation.")
                    added_job_ops = []
                    idx = 1
                    current_op_index = 0
                    while current_op_index < num_operations:
                         if idx >= len(job_part): raise ValueError("Incomplete added job: missing number of alternatives.")
                         num_alts = job_part[idx]
                         if num_alts <= 0: raise ValueError("Added job operation must have at least one alternative.")
                         idx += 1
                         alt_list = []
                         for _ in range(num_alts):
                             if idx + 1 >= len(job_part): raise ValueError("Incomplete added job: missing machine/processing time pair.")
                             m = job_part[idx]
                             p = job_part[idx + 1]
                             if not (0 <= m < num_machines): raise ValueError(f"Invalid machine index {m} in added job.")
                             if p < 0: raise ValueError("Processing time cannot be negative in added job.")
                             idx += 2
                             alt_list.append((m, p))
                         added_job_ops.append(alt_list)
                         current_op_index += 1
                    if idx != len(job_part):
                         print(f"   Warning: Extra data found on added job line {i}: {job_part[idx:]}")

                    dynamic_events['added_jobs'].append((arrival_time, added_job_ops))
                except (ValueError, IndexError) as e:
                    raise ValueError(f"Invalid format or data for Added Job on line {i}: '{line}'. Error: {e}")

            elif parsing_cancelled_jobs:
                parts = list(map(int, line.split()))
                if len(parts) != 2: raise ValueError(f"Invalid format for Cancelled Job (expected 2 integers): '{line}' (line {i}).")
                cancel_time, job_id_to_cancel = parts
                if cancel_time < 0: raise ValueError(f"Cancellation time cannot be negative ({cancel_time}) (line {i}).")
                # job_id_to_cancel refers to the index in the *initial* jobs list
                # Use the (potentially updated) number of initial jobs found so far
                current_num_initial = num_jobs_initial_spec if not parsing_jobs else job_counter
                if not (0 <= job_id_to_cancel < current_num_initial):
                     raise ValueError(f"Invalid initial job index {job_id_to_cancel} for cancellation (valid indices 0 to {current_num_initial-1}) (line {i}).")
                dynamic_events['cancelled_jobs'].append((cancel_time, job_id_to_cancel))

            # Added an else block to catch lines that don't match any known section after 'Dynamic Events'
            elif not parsing_jobs: # Only if we are past initial jobs section
                 print(f"   Warning: Skipping unrecognized line {i} in dynamic events section: '{line}'")


        # Final check if we are still expecting jobs after reading the whole file
        if parsing_jobs and job_counter != num_jobs_initial_spec:
             print(f"   Warning: Reached end of file {file_path}, expected {num_jobs_initial_spec} initial jobs based on header, but only found {job_counter}.")
             num_jobs_initial_spec = job_counter # Update to actual count

        # Sort breakdowns by start time for each machine (optional, but can be useful)
        for machine in dynamic_events['breakdowns']:
            dynamic_events['breakdowns'][machine].sort(key=lambda x: x[0])

        # Sort added jobs by arrival time (optional)
        dynamic_events['added_jobs'].sort(key=lambda x: x[0])

        # Sort cancelled jobs by time (optional)
        dynamic_events['cancelled_jobs'].sort(key=lambda x: x[0])


        # Return the number of *initial* jobs found, machines, list of initial jobs, and events
        return num_jobs_initial_spec, num_machines, initial_jobs, dynamic_events

    except (ValueError, IndexError) as e:
        print(f"   Error parsing file {file_path} near line {i}: {e}")
        return None, None, None, None
    except Exception as e: # Catch any other unexpected errors during parsing
        print(f"   An unexpected error occurred while parsing {file_path}: {e}")
        return None, None, None, None


def read_dynamic_fjsp_instance_json(file_path):
    """
    Citește fișierul de instanță FJSP (.json) cu evenimente dinamice.
    Include citirea 'weight', 'due_date' pentru joburi și a 'etpc_constraints'.
    Aceste informații suplimentare sunt stocate în dicționarul 'dynamic_events'.
    Returnează tuple (num_initial_jobs, num_machines, initial_jobs, dynamic_events).
    Returnează (None, None, None, None) la eroare.
    """
    print(f"   Reading dynamic FJSP instance (.json) from: {file_path}")
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"   Error: File not found at {file_path}")
        return None, None, None, None
    except json.JSONDecodeError as e:
        print(f"   Error decoding JSON from {file_path}: {e}")
        return None, None, None, None
    except IOError as e:
        print(f"   Error reading file {file_path}: {e}")
        return None, None, None, None

    try:
        # --- Extragem datele principale ---
        all_job_definitions = data.get('jobs', []) # Folosim [] ca default daca 'jobs' lipseste
        if not isinstance(all_job_definitions, list):
             raise ValueError("Invalid 'jobs' field (must be a list or missing) in JSON.")

        # --- MODIFICARE: Citim 'machine_breakdowns' direct ---
        machine_breakdowns_data = data.get('machine_breakdowns', {})
        if not isinstance(machine_breakdowns_data, dict):
            print("   Warning: 'machine_breakdowns' field found but is not a dictionary. Ignoring breakdowns.")
            machine_breakdowns_data = {}

        # --- MODIFICARE: Citim 'etpc_constraints' ---
        etpc_constraints = data.get('etpc_constraints', [])
        if not isinstance(etpc_constraints, list):
            print("   Warning: 'etpc_constraints' field found but is not a list. Ignoring.")
            etpc_constraints = []
        # Putem adauga validari suplimentare pentru continutul listei etpc_constraints daca e necesar

        # --- Determinăm numărul de mașini ---
        max_machine_index = -1
        # Verificăm în joburi
        for i, job_def in enumerate(all_job_definitions):
            if not isinstance(job_def, dict): continue
            operations = job_def.get("operations", [])
            if not isinstance(operations, list): continue
            for op_idx, op in enumerate(operations):
                 if not isinstance(op, dict): continue
                 candidate_machines = op.get("candidate_machines", {})
                 if not isinstance(candidate_machines, dict): continue
                 for m_str in candidate_machines.keys():
                      try:
                          m_idx = int(m_str)
                          if m_idx < 0: raise ValueError("Machine index cannot be negative.")
                          max_machine_index = max(max_machine_index, m_idx)
                      except (ValueError, TypeError):
                           job_id_info = job_def.get('id', f"at index {i}")
                           raise ValueError(f"Invalid machine key '{m_str}' in job '{job_id_info}', operation index {op_idx}.")

        # Verificăm în machine_breakdowns_data
        for m_str in machine_breakdowns_data.keys():
            try:
                m_idx = int(m_str)
                if m_idx < 0: raise ValueError("Machine index in breakdowns cannot be negative.")
                max_machine_index = max(max_machine_index, m_idx)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid machine key '{m_str}' in 'machine_breakdowns'.")

        # Calculăm numărul de mașini
        if max_machine_index == -1:
             if all_job_definitions or machine_breakdowns_data: # Daca exista joburi sau breakdowns definite
                  # Ar trebui sa existe cel putin o masina daca exista operatii/breakdowns
                  # Poate ridicam eroare sau avertizam si setam la 1?
                  # Sa ridicam eroare pentru a fi siguri.
                  raise ValueError("Could not find any valid machine indices to determine machine count, although jobs or breakdowns exist.")
             else: # Nici joburi, nici breakdowns
                  print("   Warning: No jobs or machine breakdowns found. Setting number of machines to 0.")
                  num_machines = 0
        else:
            num_machines = max_machine_index + 1
        print(f"      Derived number of machines: {num_machines}")

        # --- Inițializăm structurile de date returnate ---
        initial_jobs = [] # Va contine doar lista de operatii: List[List[List[Tuple[int, int]]]]
        job_properties = [] # MODIFICARE: Lista pentru proprietatile joburilor initiale
        dynamic_events = {
            "breakdowns": {},
            "added_jobs": [],
            "cancelled_jobs": [],
            # --- MODIFICARE: Adăugăm cheile noi aici ---
            "etpc_constraints": etpc_constraints, # Stocăm lista citită
            "job_properties": job_properties # Vom popula această listă mai jos
        }
        initial_job_index = 0
        initial_job_id_map = {}

        # --- Procesăm definițiile de joburi ---
        for i, job_def in enumerate(all_job_definitions):
             job_id_info = job_def.get('id', f"at index {i}")
             if not isinstance(job_def, dict):
                 print(f"   Warning: Skipping invalid job definition at index {i} (not a dictionary).")
                 continue

             # Extragem timpul de sosire
             arrival_time_raw = job_def.get("arrival_time", 0)
             try:
                 arrival_time = math.ceil(arrival_time_raw) if isinstance(arrival_time_raw, (int, float)) else math.ceil(float(arrival_time_raw))
                 if arrival_time < 0: arrival_time = 0
             except (TypeError, ValueError):
                  print(f"   Warning: Invalid arrival_time '{arrival_time_raw}' for job '{job_id_info}'. Using 0.")
                  arrival_time = 0

             # Extragem operațiile
             operations = job_def.get("operations")
             if not isinstance(operations, list) or not operations:
                  print(f"   Warning: Skipping job '{job_id_info}' due to missing or empty 'operations'.")
                  continue

             # Parsăm operațiile în formatul intern (listă de liste de tuple)
             current_job_ops = []
             valid_ops = True
             for op_idx, op in enumerate(operations):
                 # ... (validările și parsarea operațiilor rămân la fel ca înainte) ...
                 if not isinstance(op, dict):
                    valid_ops = False; raise ValueError(f"Invalid op format (job '{job_id_info}', op {op_idx}).")
                 candidate_machines = op.get("candidate_machines")
                 if not isinstance(candidate_machines, dict) or not candidate_machines:
                    valid_ops = False; raise ValueError(f"Missing/empty candidates (job '{job_id_info}', op {op_idx}).")

                 alt_list = []
                 for m_str, p_time_raw in candidate_machines.items():
                    try:
                        m = int(m_str)
                        p = int(p_time_raw) # Asumam int
                        if num_machines > 0 and not (0 <= m < num_machines):
                             raise ValueError(f"Machine index {m} out of bounds (0-{num_machines-1}).")
                        elif num_machines == 0: raise ValueError("Machine index found, but num_machines is 0.")
                        if p < 0: raise ValueError(f"Processing time cannot be negative ({p}).")
                        alt_list.append((m, p))
                    except (ValueError, TypeError) as e:
                         valid_ops = False; raise ValueError(f"Invalid machine/time ('{m_str}':'{p_time_raw}') in job '{job_id_info}', op {op_idx}: {e}")
                 if not alt_list:
                     valid_ops = False; raise ValueError(f"Op must have alternatives (job '{job_id_info}', op {op_idx}).")
                 alt_list.sort(key=lambda x: x[0])
                 current_job_ops.append(alt_list)

             if not valid_ops:
                 # Daca au fost erori la parsarea operatiilor, sarim jobul
                 # (desi exceptiile probabil au oprit deja executia)
                 print(f"   Error during operation parsing for job '{job_id_info}'. Skipping.")
                 continue


             # --- Clasificăm jobul și extragem proprietățile ---
             if arrival_time > 0:
                 # Job adăugat dinamic - stocăm doar operațiile
                 dynamic_events['added_jobs'].append((arrival_time, current_job_ops))
                 # Nota: 'weight' și 'due_date' pentru joburile adăugate dinamic nu sunt
                 # stocate în această implementare pentru a păstra structura simplă
                 # a 'added_jobs' ca List[Tuple[int, List[...]]].
                 # Ar necesita o modificare a structurii interne a 'added_jobs' sau
                 # un mecanism separat de stocare.
             else:
                 # Job inițial - stocăm operațiile în `initial_jobs`
                 initial_jobs.append(current_job_ops)
             original_id = job_def.get('id') # ID-ul original din JSON

                 # --- MODIFICARE: Extragem weight și due_date ---
             weight = job_def.get('weight', 1) # Default weight = 1
             due_date = job_def.get('due_date', float('inf')) # Default due_date = infinit
             try: # Validam/convertim tipurile
                weight = float(weight) if isinstance(weight, (int, float, str)) else 1.0
                due_date = float(due_date) if isinstance(due_date, (int, float, str)) else float('inf')
             except (ValueError, TypeError):
                print(f"   Warning: Invalid weight ('{job_def.get('weight')}') or due_date ('{job_def.get('due_date')}') for job '{job_id_info}'. Using defaults.")
                weight = 1.0
                due_date = float('inf')

            # Stocăm proprietățile jobului inițial
             job_props = {
                     'id': original_id, # ID-ul original (poate fi None)
                     'index': initial_job_index, # Indexul intern (0, 1, ...)
                     'weight': weight,
                     'due_date': due_date
            }
             job_properties.append(job_props) # Adaugam la lista

                 # Mapăm ID-ul original la indexul intern (dacă ID-ul există)
             if original_id is not None:
                if original_id in initial_job_id_map:
                    print(f"   Warning: Duplicate original job ID '{original_id}' found.")
                initial_job_id_map[original_id] = initial_job_index

             initial_job_index += 1 # Incrementăm indexul pentru următorul job inițial

        # --- Procesăm breakdowns (folosind noua logică) ---
        for m_str, bd_list in machine_breakdowns_data.items():
             try:
                 machine_id = int(m_str)
                 if num_machines > 0 and not (0 <= machine_id < num_machines):
                      print(f"   Warning: Machine index {machine_id} from 'machine_breakdowns' out of bounds. Skipping.")
                      continue
                 elif num_machines == 0: continue # Skip if no machines

                 if not isinstance(bd_list, list):
                     print(f"   Warning: Breakdowns for machine {machine_id} is not a list. Skipping.")
                     continue

                 machine_breakdowns = []
                 for bd_idx, bd in enumerate(bd_list):
                      # ... (logica de parsare a fiecărui breakdown 'bd' rămâne aceeași) ...
                      if not isinstance(bd, dict): continue
                      start_raw = bd.get("start_time")
                      duration_raw = bd.get("duration", bd.get("repair_time"))
                      try:
                           start = math.ceil(start_raw) if isinstance(start_raw, (int, float)) else -1
                           duration = math.ceil(duration_raw) if isinstance(duration_raw, (int, float)) else -1
                           if start < 0 or duration < 0: raise ValueError("Times must be non-negative.")
                           end = start + duration
                           machine_breakdowns.append((start, end))
                      except (TypeError, ValueError) as e:
                           print(f"   Warning: Skipping invalid breakdown data {bd} for machine {machine_id}: {e}")

                 if machine_breakdowns:
                      machine_breakdowns.sort(key=lambda x: x[0])
                      dynamic_events['breakdowns'][machine_id] = machine_breakdowns
             except (ValueError, TypeError) as e:
                  print(f"   Warning: Invalid machine key '{m_str}' in 'machine_breakdowns': {e}")


        # --- Procesăm alte evenimente dinamice ('added_jobs', 'cancelled_jobs' din cheia 'dynamic_events') ---
        # Logica existentă aici rămâne în mare parte neschimbată,
        # deoarece citește din data.get('dynamic_events', {}).
        # Joburile adăugate aici *nu* vor avea 'weight'/'due_date' stocate (vezi nota de mai sus).
        json_dynamic_events = data.get('dynamic_events', {})
        if isinstance(json_dynamic_events, dict):
            # ... (Parsarea cancelled_jobs din json_dynamic_events ramane la fel) ...
            cancelled_jobs_json = json_dynamic_events.get('cancelled_jobs', [])
            if isinstance(cancelled_jobs_json, list):
                for idx, cj in enumerate(cancelled_jobs_json):
                     try:
                         # ... (validari si extragere cancel_time, job_id_to_cancel_orig) ...
                         if not isinstance(cj, dict): raise ValueError("Entry not dict")
                         cancel_time_raw = cj['time']
                         cancel_time = math.ceil(cancel_time_raw) if isinstance(cancel_time_raw, (int,float)) else -1
                         if cancel_time < 0: raise ValueError("Cancel time negative")
                         job_id_to_cancel_orig = cj.get('job_id', cj.get('job_id_to_cancel'))
                         if job_id_to_cancel_orig is None: raise KeyError("Missing job id")

                         if job_id_to_cancel_orig in initial_job_id_map:
                             initial_job_idx = initial_job_id_map[job_id_to_cancel_orig]
                         else:
                             try:
                                 potential_idx = int(job_id_to_cancel_orig)
                                 if 0 <= potential_idx < len(initial_jobs): initial_job_idx = potential_idx
                                 else: raise ValueError(f"ID '{job_id_to_cancel_orig}' not found/invalid index.")
                             except: raise ValueError(f"Cannot find initial job ID '{job_id_to_cancel_orig}'.")

                         dynamic_events['cancelled_jobs'].append((cancel_time, initial_job_idx))
                     except (KeyError, ValueError, TypeError) as e:
                          print(f"   Warning: Skipping invalid cancelled job entry {cj} at index {idx}: {e}")

            # ... (Parsarea added_jobs din json_dynamic_events ramane la fel) ...
            added_jobs_json = json_dynamic_events.get('added_jobs', [])
            if isinstance(added_jobs_json, list):
                 for idx, aj in enumerate(added_jobs_json):
                      try:
                          # ... (validari, extragere arrival_time, parsare operatii in added_job_ops) ...
                           if not isinstance(aj, dict): raise ValueError("Entry not dict.")
                           arrival_time_raw = aj.get('arrival_time')
                           if arrival_time_raw is None: raise KeyError("Missing 'arrival_time'.")
                           arrival_time = math.ceil(arrival_time_raw) if isinstance(arrival_time_raw, (int, float)) else -1
                           if arrival_time <= 0: raise ValueError("Dyn added job arrival time must be > 0.")
                           operations = aj.get("operations")
                           if not isinstance(operations, list) or not operations: raise ValueError("Missing/empty ops.")

                           added_job_ops = []
                           # ... (loop pt parsare operatii similar cu cel principal) ...
                           for op_idx, op in enumerate(operations):
                                if not isinstance(op, dict): raise ValueError(f"Invalid op format (dyn job idx {idx}, op {op_idx}).")
                                candidate_machines = op.get("candidate_machines")
                                if not isinstance(candidate_machines, dict) or not candidate_machines: raise ValueError("Missing candidates.")
                                alt_list = []
                                for m_str, p_time_raw in candidate_machines.items():
                                     try:
                                         m = int(m_str)
                                         p = int(p_time_raw)
                                         if num_machines > 0 and not (0 <= m < num_machines): raise ValueError(f"Machine index {m} out of bounds.")
                                         elif num_machines == 0: raise ValueError("Machine index found, but num_machines is 0.")
                                         if p < 0: raise ValueError(f"Proc time negative ({p}).")
                                         alt_list.append((m, p))
                                     except (ValueError, TypeError) as e: raise ValueError(f"Invalid machine/time: {e}")
                                if not alt_list: raise ValueError("Op must have alternatives.")
                                alt_list.sort(key=lambda x: x[0])
                                added_job_ops.append(alt_list)
                           # Adaugam la lista principala de added_jobs
                           dynamic_events['added_jobs'].append((arrival_time, added_job_ops))
                      except (KeyError, ValueError, TypeError) as e:
                           print(f"   Warning: Skipping invalid dynamic added job entry {aj} at index {idx}: {e}")


        # --- Finalizăm și sortăm ---
        dynamic_events['added_jobs'].sort(key=lambda x: x[0])
        dynamic_events['cancelled_jobs'].sort(key=lambda x: x[0])
        # Nota: job_properties este deja sortat implicit după indexul joburilor inițiale

        num_initial_jobs = len(initial_jobs) # Numărul efectiv de joburi inițiale parsate
        if num_machines < 0: raise ValueError("Internal Error: Num machines negative.")
        if num_machines == 0 and (initial_jobs or dynamic_events['added_jobs'] or dynamic_events['breakdowns']):
            raise ValueError("Internal Error: Num machines is 0, but jobs/breakdowns exist.")

        # Returnăm structura originală (4 elemente), dar dynamic_events conține acum datele extra
        return num_initial_jobs, num_machines, initial_jobs, dynamic_events

    except (ValueError, IndexError, KeyError, TypeError) as e:
        print(f"   Error processing JSON data structure in {file_path}: {e}")
        return None, None, None, None
    except Exception as e:
        print(f"   An unexpected error occurred while processing JSON data from {file_path}: {e}")
        return None, None, None, None


# --- Modified loading function ---
def load_instances_from_directory(input_dir):
    """
    Walks through `input_dir` and loads all .txt and .json FJSP instances.
    Prints the parsed data for each file.
    Returns a list of tuples: (initial_jobs, num_machines, dynamic_events, filename).
    `dynamic_events` now contains additional keys if loaded from JSON.
    Skips files that cause parsing errors.
    """
    print(f"Reading dynamic FJSP instances from directory: {input_dir}")
    all_instances = []
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory '{input_dir}' not found or is not a directory.")
        return []

    for root, dirs, files in os.walk(input_dir):
        files.sort()
        for fname in files:
            fpath = os.path.join(root, fname)
            instance_data = None
            parsed_tuple = None

            print(f"\n--- Processing file: {fname} ---")

            if fname.endswith(".txt"):
                try:
                    parsed_tuple = read_dynamic_fjsp_instance_txt(fpath)
                    if parsed_tuple[0] is not None:
                        num_jobs, num_machines, jobs, events = parsed_tuple
                        print(f"--- Parsed data from {fname} (.txt): ---")
                        pprint.pprint(parsed_tuple)
                        print("----------------------------------------")
                        # Adăugăm chei goale/default la 'events' pentru consistență în validare? Opțional.
                        events.setdefault('etpc_constraints', [])
                        events.setdefault('job_properties', [])
                        instance_data = (jobs, num_machines, events, fname)
                    else:
                        print(f"   Skipping file {fname} due to parsing errors.")
                except Exception as e:
                     print(f"   Critical error processing {fname}: {e}. Skipping.")

            elif fname.endswith(".json"):
                try:
                    parsed_tuple = read_dynamic_fjsp_instance_json(fpath)
                    if parsed_tuple[0] is not None:
                        num_jobs, num_machines, jobs, events = parsed_tuple
                        print(f"--- Parsed data from {fname} (.json): ---")
                        # Afișăm doar o parte din dynamic_events pentru claritate? Sau tot? Afișăm tot.
                        pprint.pprint(parsed_tuple)
                        print("-----------------------------------------")
                        # events conține deja cheile noi din parsarea JSON
                        instance_data = (jobs, num_machines, events, fname)
                    else:
                        print(f"   Skipping file {fname} due to parsing errors.")
                except Exception as e:
                     print(f"   Critical error processing {fname}: {e}. Skipping.")

            # Append data if successfully parsed
            if instance_data:
                 # --- MODIFICARE: Actualizăm validarea pentru a include cheile noi ---
                 if isinstance(instance_data[0], list) and \
                    isinstance(instance_data[1], int) and instance_data[1] >= 0 and \
                    isinstance(instance_data[2], dict) and \
                    'breakdowns' in instance_data[2] and \
                    'added_jobs' in instance_data[2] and \
                    'cancelled_jobs' in instance_data[2] and \
                    'etpc_constraints' in instance_data[2] and \
                    'job_properties' in instance_data[2]: # Verificam si cheile noi
                     all_instances.append(instance_data)
                     # Afișăm confirmarea cu informații despre datele extra
                     extra_info = ""
                     if instance_data[2]['etpc_constraints']:
                         extra_info += f", {len(instance_data[2]['etpc_constraints'])} ETPC constraints"
                     if instance_data[2]['job_properties']:
                         extra_info += f", {len(instance_data[2]['job_properties'])} initial job properties"

                 else:
                      print(f"   Skipping file {fname} due to inconsistent data structure returned by parser or validation failure (check for new keys in events dict).")


    print(f"Done reading dynamic FJSP instances. Stored {len(all_instances)} valid instances.")
    return all_instances

