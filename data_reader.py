import os

def read_dynamic_fjsp_instance(file_path):
    """
    Citește fișierul de instanță FJSP cu evenimente dinamice
    și returnează tuple (num_jobs, num_machines, jobs, dynamic_events).
    """

    print("   Reading dynamic FJSP instance in " + file_path )
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


def load_instances_from_directory(input_dir):
    """
    Parcurge directorul `input_dir` și încarcă toate fișierele .txt
    într-o listă de tuple (jobs, num_machines, events, filename).
    """

    print("Reading dynamic FJSP instances")
    all_instances = []
    for root, dirs, files in os.walk(input_dir):
        for fname in files:
            if fname.endswith(".txt"):
                fpath = os.path.join(root, fname)
                num_jobs, num_machines, jobs, events = read_dynamic_fjsp_instance(fpath)
                all_instances.append((jobs, num_machines, events, fname))

    print("Done reading dynamic FJSP instances")
    return all_instances
