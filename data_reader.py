import os
import json
import math # Needed for rounding arrival/start times if they are floats
import pprint

# --- Function for reading the original .txt format (with added error handling) ---
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

# --- Function for reading the JSON format ---
# ... (codul funcției read_dynamic_fjsp_instance_json rămâne la fel) ...
def read_dynamic_fjsp_instance_json(file_path):
    """
    Citește fișierul de instanță FJSP (.json) cu evenimente dinamice.
    Numărul de mașini este dedus din indexul maxim al mașinii utilizate
    în 'jobs'->'operations'->'candidate_machines' ȘI 'machine_properties'.
    Joburile cu 'arrival_time' > 0 sunt tratate ca 'added_jobs'.
    Breakdowns sunt citite din 'machine_properties'.
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
        # --- Extragem joburile și machine_properties (dacă există) ---
        all_job_definitions = data.get('jobs')
        if not isinstance(all_job_definitions, list): # Poate fi goală, dar trebuie să fie listă
             raise ValueError("Missing or invalid 'jobs' field (must be a list) in JSON.")

        machine_properties = data.get('machine_breakdowns', {}) # Folosim {} dacă lipsește
        if not isinstance(machine_properties, dict):
            print("   Warning: 'machine_properties' field is not a dictionary. Ignoring it for machine count and breakdowns.")
            machine_properties = {}

        # --- Determinăm numărul de mașini găsind indexul maxim + 1 ---
        max_machine_index = -1

        # Verificăm în joburi
        if not all_job_definitions and not machine_properties:
             # If both are empty/missing, we might still proceed if num_machines can be inferred elsewhere
             # or default to 0/1, but it's safer to raise an error or warn. Let's warn and default to 1?
             # Or better, raise error if *no* machine index is found *anywhere*.
             pass # We'll check max_machine_index later

        for i, job_def in enumerate(all_job_definitions):
            if not isinstance(job_def, dict): continue # Ignorăm joburile invalide
            operations = job_def.get("operations", [])
            if not isinstance(operations, list): continue # Ignorăm operațiile invalide

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
                           # Provide more context in error
                           job_id_info = job_def.get('id', f"at index {i}")
                           raise ValueError(f"Invalid machine key '{m_str}' in job '{job_id_info}', operation index {op_idx}.")


        # Verificăm în machine_properties
        for m_str in machine_properties.keys():
            try:
                m_idx = int(m_str)
                if m_idx < 0: raise ValueError("Machine index in properties cannot be negative.")
                max_machine_index = max(max_machine_index, m_idx)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid machine key '{m_str}' in 'machine_properties'.")

        # Calculăm numărul de mașini
        if max_machine_index == -1:
             # This means no machine indices were found anywhere.
             # If jobs or properties existed but were empty or malformed, this can happen.
             # If both jobs and properties were missing/empty initially, this is expected.
             # Let's default to 1 machine ONLY if there are jobs defined (even if they had no machines listed?)
             # It's safer to raise an error if no machines could be inferred at all.
             if all_job_definitions or machine_properties:
                  raise ValueError("Could not find any valid machine indices in jobs or properties to determine machine count.")
             else:
                  # No jobs, no properties - maybe a valid (empty) instance? Let's allow 0 machines.
                  print("   Warning: No jobs or machine properties found. Setting number of machines to 0.")
                  num_machines = 0
        else:
            num_machines = max_machine_index + 1

        # Print derived number of machines only if > 0
        if num_machines > 0:
            print(f"      Derived number of machines (based on max index {max_machine_index}): {num_machines}")
        elif num_machines == 0 and (all_job_definitions or machine_properties):
             # This state should not be reached due to the error raised above
             pass


        # --- Continuăm parsarea ca înainte, folosind num_machines derivat ---
        initial_jobs = []
        dynamic_events = { "breakdowns": {}, "added_jobs": [], "cancelled_jobs": [] }
        initial_job_index = 0
        initial_job_id_map = {} # Map original job 'id' to initial_job_index

        for i, job_def in enumerate(all_job_definitions):
             job_id_info = job_def.get('id', f"at index {i}") # For better error messages
            # --- Re-verificăm tipul job_def aici pentru claritate ---
             if not isinstance(job_def, dict):
                 print(f"   Warning: Skipping invalid job definition at index {i} (not a dictionary) during detailed parsing.")
                 continue

             arrival_time_raw = job_def.get("arrival_time", 0)
             try:
                 # Allow float arrival times but ceil them for discrete events
                 if isinstance(arrival_time_raw, (int, float)):
                      arrival_time = math.ceil(arrival_time_raw)
                 else:
                      # Try converting to float first, then ceil
                      arrival_time = math.ceil(float(arrival_time_raw))

                 if arrival_time < 0:
                     print(f"   Warning: Negative arrival_time {arrival_time_raw} for job '{job_id_info}'. Using 0.")
                     arrival_time = 0
             except (TypeError, ValueError):
                  print(f"   Warning: Invalid arrival_time '{arrival_time_raw}' for job '{job_id_info}'. Using 0.")
                  arrival_time = 0

             operations = job_def.get("operations")
             if not isinstance(operations, list) or not operations:
                  print(f"   Warning: Skipping job '{job_id_info}' due to missing or empty 'operations' list during detailed parsing.")
                  continue

             current_job_ops = []
             for op_idx, op in enumerate(operations):
                 if not isinstance(op, dict):
                     raise ValueError(f"Invalid operation format (not a dict) for job '{job_id_info}', op index {op_idx} during detailed parsing.")

                 candidate_machines = op.get("candidate_machines")
                 if not isinstance(candidate_machines, dict) or not candidate_machines:
                      raise ValueError(f"Missing or empty 'candidate_machines' dict for job '{job_id_info}', op index {op_idx} during detailed parsing.")

                 alt_list = []
                 for m_str, p_time_raw in candidate_machines.items():
                    try:
                        m = int(m_str)
                        # Allow float processing times? Let's assume integer for now based on TXT format.
                        p = int(p_time_raw)

                        # Validate machine index against derived num_machines
                        # Allow num_machines == 0 only if no machines were ever defined
                        if num_machines > 0 and not (0 <= m < num_machines):
                            raise ValueError(f"Machine index {m} out of bounds (0-{num_machines-1}) for job '{job_id_info}', op {op_idx}.")
                        elif num_machines == 0 and m != 0: # Or maybe just raise if num_machines is 0?
                             # If num_machines is 0, no machine index should exist.
                             raise ValueError(f"Machine index {m} found, but determined number of machines is 0. Inconsistent data in job '{job_id_info}', op {op_idx}.")

                        if p < 0:
                            raise ValueError(f"Processing time cannot be negative ({p}) for job '{job_id_info}', op {op_idx}.")
                        alt_list.append((m, p))
                    except (ValueError, TypeError) as e:
                         # Improve error message
                         raise ValueError(f"Invalid machine/time ('{m_str}': '{p_time_raw}') in job '{job_id_info}', op {op_idx}: {e}")


                 if not alt_list:
                      raise ValueError(f"Operation must have at least one valid alternative machine (job '{job_id_info}', op index {op_idx}).")

                 # Sort alternatives by machine index for consistency (optional but good)
                 alt_list.sort(key=lambda x: x[0])
                 current_job_ops.append(alt_list)

             # --- Clasifică jobul ---
             if arrival_time > 0:
                 dynamic_events['added_jobs'].append((arrival_time, current_job_ops))
             else:
                 initial_jobs.append(current_job_ops)
                 original_id = job_def.get('id') # Get the original ID specified in the JSON
                 if original_id is not None:
                      # Check for duplicate original IDs among initial jobs
                      if original_id in initial_job_id_map:
                           print(f"   Warning: Duplicate original job ID '{original_id}' found. Cancellation might be ambiguous if based on ID.")
                      initial_job_id_map[original_id] = initial_job_index
                 initial_job_index += 1

        # --- Procesează penele (folosind machine_properties pre-validat) ---
        for m_str, props in machine_properties.items():
            # Key conversion already validated during num_machines calculation
            machine_id = int(m_str)
            machine_breakdowns = []
            for bd_idx, bd in enumerate(props):

                if not isinstance(bd, dict):
                    print(f"   Warning: Skipping invalid breakdown entry (not a dict) for machine {machine_id} at index {bd_idx}.")
                    continue
                start_raw = bd.get("start_time")
                # Allow 'duration' or 'repair_time'
                duration_raw = bd.get("duration", bd.get("repair_time"))

                try:
                    # Use math.ceil for start time as well? Consistent with arrival.
                    start = math.ceil(start_raw) if isinstance(start_raw, (int, float)) else -1
                    duration = math.ceil(duration_raw) if isinstance(duration_raw, (int, float)) else -1

                    if start < 0 or duration < 0:
                         raise ValueError(f"Start time ({start_raw}) and duration ({duration_raw}) must be non-negative.")

                    end = start + duration # End time is exclusive? Or inclusive? Assume exclusive (interval [start, end))
                                                # Let's stick to the TXT format's apparent convention: end time is the timestamp *when it becomes available again*.
                                                # So, if breakdown is [5, 10] in TXT, it means unavailable at t=5, 6, 7, 8, 9. Available at t=10. Duration = 5.
                                                # JSON: start_time=5, duration/repair_time=5 -> end = 10. Consistent.
                    machine_breakdowns.append((start, end))

                except (TypeError, ValueError) as e:
                         print(f"   Warning: Skipping invalid breakdown data {bd} for machine {machine_id}: {e}")

            if machine_breakdowns:
                     # Sort machine specific breakdowns by start time
                machine_breakdowns.sort(key=lambda x: x[0])
                dynamic_events['breakdowns'][machine_id] = machine_breakdowns

        # --- Procesează alte evenimente dinamice definite în JSON ---
        json_dynamic_events = data.get('dynamic_events', {})
        if isinstance(json_dynamic_events, dict):

            # --- Joburi Anulate (dinamic) ---
            cancelled_jobs_json = json_dynamic_events.get('cancelled_jobs', [])
            if isinstance(cancelled_jobs_json, list):
                for idx, cj in enumerate(cancelled_jobs_json):
                     try:
                         if not isinstance(cj, dict): raise ValueError("Entry is not a dictionary.")

                         cancel_time_raw = cj['time']
                         # Use ceil for consistency
                         cancel_time = math.ceil(cancel_time_raw) if isinstance(cancel_time_raw, (int,float)) else -1
                         if cancel_time < 0: raise ValueError("Cancel time must be non-negative.")

                         # Use 'job_id' for consistency with job definition field name
                         job_id_to_cancel_orig = cj.get('job_id', cj.get('job_id_to_cancel')) # Allow both keys
                         if job_id_to_cancel_orig is None: raise KeyError("Missing 'job_id' or 'job_id_to_cancel' field.")


                         # Try to find the job index based on the original ID from the JSON
                         if job_id_to_cancel_orig in initial_job_id_map:
                             initial_job_idx = initial_job_id_map[job_id_to_cancel_orig]
                         else:
                             # If ID not found, check if it's a valid integer index
                             try:
                                 potential_idx = int(job_id_to_cancel_orig)
                                 if 0 <= potential_idx < len(initial_jobs):
                                     initial_job_idx = potential_idx
                                     # Optional: Warn if cancelling by index when IDs were present?
                                     # print(f"   Warning: Cancelling job by index {initial_job_idx} as ID '{job_id_to_cancel_orig}' was not found in initial job IDs.")
                                 else:
                                     raise ValueError(f"ID '{job_id_to_cancel_orig}' not found in initial jobs, and it's not a valid index (0-{len(initial_jobs)-1}).")
                             except (ValueError, TypeError): # Handle cases where ID is not int-convertible
                                  raise ValueError(f"Cannot find initial job with ID '{job_id_to_cancel_orig}' to cancel.")


                         dynamic_events['cancelled_jobs'].append((cancel_time, initial_job_idx))

                     except (KeyError, ValueError, TypeError) as e:
                          print(f"   Warning: Skipping invalid cancelled job entry {cj} at index {idx}: {e}")

            # --- Joburi Adăugate (dinamic) ---
            # This section handles jobs defined *within* the dynamic_events block,
            # separate from jobs defined in the main 'jobs' list with arrival_time > 0.
            added_jobs_json = json_dynamic_events.get('added_jobs', [])
            if isinstance(added_jobs_json, list):
                 for idx, aj in enumerate(added_jobs_json):
                      try:
                           if not isinstance(aj, dict): raise ValueError("Entry is not a dictionary.")

                           arrival_time_raw = aj.get('arrival_time')
                           if arrival_time_raw is None: raise KeyError("Missing 'arrival_time'.")
                           arrival_time = math.ceil(arrival_time_raw) if isinstance(arrival_time_raw, (int, float)) else -1
                           if arrival_time <= 0: raise ValueError("Dynamically added job arrival time must be positive.") # Should be > 0

                           operations = aj.get("operations")
                           if not isinstance(operations, list) or not operations:
                                raise ValueError("Missing or empty 'operations' list.")

                           added_job_ops = []
                           job_id_info = aj.get('id', f"dynamic event index {idx}") # For errors
                           for op_idx, op in enumerate(operations):
                                if not isinstance(op, dict): raise ValueError(f"Invalid operation format (op index {op_idx}).")
                                candidate_machines = op.get("candidate_machines")
                                if not isinstance(candidate_machines, dict) or not candidate_machines:
                                     raise ValueError(f"Missing/empty 'candidate_machines' (op index {op_idx}).")

                                alt_list = []
                                for m_str, p_time_raw in candidate_machines.items():
                                     try:
                                         m = int(m_str)
                                         p = int(p_time_raw)
                                         if num_machines > 0 and not (0 <= m < num_machines):
                                             raise ValueError(f"Machine index {m} out of bounds (0-{num_machines-1}).")
                                         elif num_machines == 0:
                                             raise ValueError("Machine index found, but num_machines is 0.")
                                         if p < 0: raise ValueError(f"Processing time cannot be negative ({p}).")
                                         alt_list.append((m, p))
                                     except (ValueError, TypeError) as e:
                                          raise ValueError(f"Invalid machine/time ('{m_str}':'{p_time_raw}'): {e}")

                                if not alt_list: raise ValueError(f"Operation must have alternatives (op index {op_idx}).")
                                alt_list.sort(key=lambda x: x[0])
                                added_job_ops.append(alt_list)

                           # Successfully parsed dynamic added job
                           dynamic_events['added_jobs'].append((arrival_time, added_job_ops))

                      except (KeyError, ValueError, TypeError) as e:
                           print(f"   Warning: Skipping invalid dynamic added job entry {aj} at index {idx}: {e}")

        # Sortează evenimentele adăugate (include cele din job list și cele din dynamic events)
        dynamic_events['added_jobs'].sort(key=lambda x: x[0])
        # Sortează evenimentele anulate
        dynamic_events['cancelled_jobs'].sort(key=lambda x: x[0])


        num_initial_jobs = len(initial_jobs)
        # Re-validate num_machines consistency
        if num_machines < 0: # Should not happen if logic above is correct
            raise ValueError("Internal Error: Final derived number of machines is negative.")
        # Allow num_machines == 0 only if the instance was truly empty
        if num_machines == 0 and (initial_jobs or dynamic_events['added_jobs'] or dynamic_events['breakdowns']):
             raise ValueError("Internal Error: Number of machines derived as 0, but jobs or breakdowns requiring machines exist.")


        return num_initial_jobs, num_machines, initial_jobs, dynamic_events

    except (ValueError, IndexError, KeyError, TypeError) as e:
        # Improve error context if possible
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
    Skips files that cause parsing errors.
    """
    print(f"Reading dynamic FJSP instances from directory: {input_dir}")
    all_instances = []
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory '{input_dir}' not found or is not a directory.")
        return []

    for root, dirs, files in os.walk(input_dir):
        # Sort files for consistent processing order (optional)
        files.sort()
        for fname in files:
            fpath = os.path.join(root, fname)
            instance_data = None # Initialize for each file
            parsed_tuple = None # Store the direct output of parser

            print(f"\n--- Processing file: {fname} ---") # Separator

            if fname.endswith(".txt"):
                try:
                    # Call the parser
                    parsed_tuple = read_dynamic_fjsp_instance_txt(fpath)
                    # Check if parsing was successful before unpacking and printing
                    if parsed_tuple[0] is not None: # Check num_jobs
                        num_jobs, num_machines, jobs, events = parsed_tuple
                        print(f"--- Parsed data from {fname} (.txt): ---")
                        pprint.pprint(parsed_tuple) # Pretty print the whole tuple
                        print("----------------------------------------")
                        # Prepare data for the final list (if needed later)
                        instance_data = (jobs, num_machines, events, fname)
                    else:
                        print(f"   Skipping file {fname} due to parsing errors reported by function.")
                except Exception as e: # Catch any unexpected error during the call itself
                     print(f"   Critical error processing {fname}: {e}. Skipping file.")

            elif fname.endswith(".json"):
                try:
                    # Call the parser
                    parsed_tuple = read_dynamic_fjsp_instance_json(fpath)
                     # Check if parsing was successful before unpacking and printing
                    if parsed_tuple[0] is not None: # Check num_jobs
                        num_jobs, num_machines, jobs, events = parsed_tuple
                        print(f"--- Parsed data from {fname} (.json): ---")
                        pprint.pprint(parsed_tuple) # Pretty print the whole tuple
                        print("-----------------------------------------")
                        # Prepare data for the final list (if needed later)
                        instance_data = (jobs, num_machines, events, fname)
                    else:
                        print(f"   Skipping file {fname} due to parsing errors reported by function.")
                except Exception as e: # Catch any unexpected error during the call itself
                     print(f"   Critical error processing {fname}: {e}. Skipping file.")

            # Append data if successfully parsed and structure looks valid
            if instance_data:
                 # Perform a basic validation of the returned structure (optional but recommended)
                 if isinstance(instance_data[0], list) and \
                    isinstance(instance_data[1], int) and instance_data[1] >= 0 and \
                    isinstance(instance_data[2], dict) and \
                    'breakdowns' in instance_data[2] and \
                    'added_jobs' in instance_data[2] and \
                    'cancelled_jobs' in instance_data[2]:
                     all_instances.append(instance_data)
                     print(f"   Successfully validated and stored instance: {fname} ({len(instance_data[0])} initial jobs, {instance_data[1]} machines)")
                 else:
                      print(f"   Skipping file {fname} due to inconsistent data structure returned by parser or validation failure.")
            print(f"--- Finished processing file: {fname} ---\n") # Separator


    print(f"Done reading dynamic FJSP instances. Stored {len(all_instances)} valid instances.")
    return all_instances

# --- Example Usage ---
if __name__ == "__main__":
    # Create a dummy directory and files for testing
    test_dir = "test_instances"
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)




    # 3. Run the loading function
    loaded_data = load_instances_from_directory(test_dir)

    # Optional: Clean up the test directory and files
    # import shutil
    # shutil.rmtree(test_dir)