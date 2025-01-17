import os
import random

# Funcția pentru citirea instanței originale
def read_fjsp_instance(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    num_jobs, num_machines = -1, -1
    jobs = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#"):  # Ignoră liniile goale sau comentariile
            if num_jobs == -1 and num_machines == -1:
                num_jobs, num_machines = map(int, line.split())
            else:
                data = list(map(int, line.split()))
                num_operations = data[0]
                job = []
                idx = 1
                for _ in range(num_operations):
                    num_machines_op = data[idx]
                    idx += 1
                    machines = []
                    for _ in range(num_machines_op):
                        machine, time = data[idx], data[idx + 1]
                        machines.append((machine, time))
                        idx += 2
                    job.append(machines)
                jobs.append(job)
    print(file_path)
    print(jobs[0])
    return num_jobs, num_machines, jobs

# Funcția pentru scrierea instanței modificate
def write_fjsp_instance(file_path, num_jobs, num_machines, jobs, events):
    with open(file_path, 'w') as f:
        f.write(f"{num_jobs} {num_machines}\n")
        for job in jobs:
            f.write(f"{len(job)} ")
            for operation in job:
                f.write(f"{len(operation)} " + " ".join(f"{m[0]} {m[1]}" for m in operation) + " ")
            f.write("\n")

        f.write("\nDynamic Events\n")
        if 'breakdowns' in events:
            f.write("Machine Breakdowns (x y z): Machine {x} breakdown from time {y} to {z}\n")
            for machine, intervals in events['breakdowns'].items():
                for start, end in intervals:
                    f.write(f"{machine} {start} {end}\n")
        if 'added_jobs' in events:
            f.write("\nAdded Jobs (first number is the time at which the job is added, then pairs of machines and process time)\n")
            for job_time, new_job in events['added_jobs']:
                f.write(f"{job_time}: {len(new_job)} ")
                for operation in new_job:
                    f.write(f"{len(operation)} " + " ".join(f"{m[0]} {m[1]}" for m in operation) + " ")
                f.write("\n")
        if 'cancelled_jobs' in events:
            f.write("\nCancelled Jobs\n")
            for cancel_time, cancelled_job in events['cancelled_jobs']:
                f.write(f"{cancel_time} {cancelled_job}\n")


def calculate_total_and_average_execution_times_per_machine(num_machines, jobs):
    # Inițializăm o listă pentru fiecare mașină, pentru a colecta timpii de execuție
    machine_times = {m: [] for m in range(num_machines)}

    # Iterăm prin joburi și operații
    for job in jobs:
        for operation in job:
            for machine, time in operation:
                # Adăugăm timpul de execuție al operației pentru mașina curentă
                machine_times[machine].append(time)

    # Calculăm timpul mediu și totalul pentru fiecare mașină
    machine_avg_times = [0.0] * num_machines
    machine_total_times = [0.0] * num_machines

    for machine in range(num_machines):
        if machine_times[machine]:  # Dacă există timpi colectați pentru această mașină
            machine_total_times[machine] = sum(machine_times[machine])
            machine_avg_times[machine] = machine_total_times[machine] / len(machine_times[machine])

    return machine_total_times, machine_avg_times

def calculate_min_max_processing_times_per_machine(num_machines, jobs):
    machine_processing_times = {m: [] for m in range(num_machines)}

    # Adunăm timpii de procesare pentru fiecare mașină
    for job in jobs:
        for operation in job:
            for machine, time in operation:
                machine_processing_times[machine].append(time)

    # Calculăm timpii minim și maxim pentru fiecare mașină
    machine_min_max_times = {}
    all_times = [time for times in machine_processing_times.values() for time in times if times]
    global_min = min(all_times) if all_times else 10
    global_max = max(all_times) if all_times else 100

    for machine, times in machine_processing_times.items():
        if times:  # Dacă există timpi pentru această mașină
            machine_min_max_times[machine] = (min(times), max(times))
        else:  # Dacă nu există operații pentru această mașină
            machine_min_max_times[machine] = (global_min, global_max)

    return machine_min_max_times


# Generare evenimente dinamice
def add_fjsp_dynamic_events(num_machines, num_jobs, jobs, probabilities, max_breakdowns=0.2, max_breakdown_time=0.05, max_added_jobs=0.2):
    events = {
        'breakdowns': {m: [] for m in range(num_machines)},
        'added_jobs': [],
        'cancelled_jobs': []
    }

    machines_total_times, machines_avg_times = calculate_total_and_average_execution_times_per_machine(num_machines, jobs)
    max_total_machine_processing_time = 0
    for machine in range(num_machines):
        total_machine_processing_time = machines_total_times[machine]
        if total_machine_processing_time > max_total_machine_processing_time:
            max_total_machine_processing_time = total_machine_processing_time

        max_duration = max(1, int(total_machine_processing_time * max_breakdown_time))
        avg_processing_time = machines_avg_times[machine]
        total_breakdown_time = 0
        breakdowns_number = int(max_breakdowns * num_jobs)
        last_breakdown_end_time = 0
        true_breakdowns = 0
        for _ in range(breakdowns_number):
            if random.random() < probabilities['breakdown']:
                true_breakdowns += 1

        for i in range(true_breakdowns):
            if last_breakdown_end_time == 0:
                lower_bound = 0
            else:
                lower_bound = last_breakdown_end_time + int(0.1 * num_jobs * avg_processing_time)
            upper_bound = int((i + 1) / breakdowns_number * total_machine_processing_time)

            if lower_bound >= upper_bound:
                continue  # Sărim peste iterația curentă dacă intervalul este invalid

            start = random.randint(lower_bound, upper_bound)
            duration = min(max_duration, random.randint(int(1.75 * avg_processing_time), int(4 * avg_processing_time)))

            if total_breakdown_time >= max_duration:
                break

            end = start + duration
            if machine not in events['breakdowns']:
                events['breakdowns'][machine] = []
            events['breakdowns'][machine].append((start, end))

            last_breakdown_end_time = end


    for job_id in range(num_jobs):
        if random.random() < probabilities['cancel_job']:
            cancel_time = random.randint(0, int(max_total_machine_processing_time * 0.65))
            events['cancelled_jobs'].append((cancel_time, job_id))


    min_machines_per_operation = min(len(operation) for job in jobs for operation in job)
    max_machines_per_operation = max(len(operation) for job in jobs for operation in job)
    min_operations = min(len(job) for job in jobs)
    max_operations = max(len(job) for job in jobs)
    machine_min_max_times = calculate_min_max_processing_times_per_machine(num_machines, jobs)

    # Generare joburi noi
    new_jobs_number = int(max_added_jobs * num_jobs)
    for _ in range(new_jobs_number):
        if random.random() < probabilities['create_job']:
            new_job_time = random.randint(int(0.05*max_total_machine_processing_time), int(0.4*max_total_machine_processing_time))  # Timpul la care jobul este introdus
            num_operations = random.randint(min_operations, max_operations)
            new_job = []

            for _ in range(num_operations):
                # Determinăm numărul de mașini disponibile pentru operație
                num_machines_op = random.randint(min_machines_per_operation, max_machines_per_operation)
                machines = random.sample(range(num_machines), num_machines_op)

                operation = []
                for machine in machines:
                    # Timp de procesare între limitele specifice fiecărei mașini
                    processing_time = random.randint(int(machine_min_max_times[machine][0]), int(machine_min_max_times[machine][1]))
                    operation.append((machine, processing_time))

                new_job.append(operation)

            events['added_jobs'].append((new_job_time, new_job))

    return events

# Funcție recursivă pentru procesarea fișierelor dintr-un director și subdirectoare
def process_fjsp_instances_recursive(input_dir, output_dir, num_variants=3, probabilities=None, max_breakdowns=0.2, max_breakdown_time=0.05, max_added_jobs=0.2):
    if probabilities is None:
        probabilities = {
            'breakdown': 0.2,
            'cancel_job': 0.10,
            'create_job': 0.3
        }

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Parcurgem recursiv directoarele
    for root, _, files in os.walk(input_dir):
        for file_name in files:
            if file_name.endswith(".txt"):
                input_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(root, input_dir)  # Obține calea relativă a subdirectorului
                output_subdir = os.path.join(output_dir, relative_path)

                if not os.path.exists(output_subdir):
                    os.makedirs(output_subdir)

                num_jobs, num_machines, jobs = read_fjsp_instance(input_path)

                for i in range(num_variants):  # Generăm variante dinamice pentru fiecare fișier
                    events = add_fjsp_dynamic_events(
                        num_machines, num_jobs, jobs, probabilities, max_breakdowns, max_breakdown_time, max_added_jobs
                    )
                    output_path = os.path.join(output_subdir, f"{os.path.splitext(file_name)[0]}_dynamic_{i + 1}.txt")
                    write_fjsp_instance(output_path, num_jobs, num_machines, jobs, events)

# Exemplu de utilizare
input_directory = "fjsp-instances-main"  # Directorul cu fișierele originale
output_directory = "dynamic-FJSP-instances"  # Directorul pentru fișierele generate
probabilities = {
    'breakdown': 0.2,
    'cancel_job': 0.1,
    'create_job': 0.3
}

process_fjsp_instances_recursive(input_directory, output_directory, num_variants=1, probabilities=probabilities, max_breakdowns=0.3, max_breakdown_time=0.15, max_added_jobs=0.2)
