import os
import random


# Funcția pentru citirea instanței originale
def read_instance(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    # Găsește prima linie validă cu numărul de mașini și joburi
    num_machines, num_jobs = -1, -1
    jobs = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#"):  # Ignoră liniile goale sau comentariile
            try:
                # Prima linie care conține două numere este numărul de mașini și joburi
                if num_machines == -1 and num_jobs == -1:
                    num_machines, num_jobs = map(int, line.split())
                else:
                    # Procesarea joburilor
                    operations = list(map(int, line.split()))
                    job = [(operations[i], operations[i + 1]) for i in range(0, len(operations), 2)]
                    jobs.append(job)
            except ValueError:
                continue  # Ignoră liniile care nu conțin numere valide

    return num_machines, num_jobs, jobs



# Funcția pentru scrierea instanței modificate
def write_instance(file_path, num_machines, num_jobs, jobs, events):
    with open(file_path, 'w') as f:
        # Scrie partea originală a fișierului
        f.write(f"{num_machines} {num_jobs}\n")
        for job in jobs:
            f.write(" ".join(f"{op[0]} {op[1]}" for op in job) + "\n")

        # Adaugă evenimentele dinamice
        f.write("\n# Dynamic Events\n")
        if 'breakdowns' in events:
            f.write("# Machine Breakdowns (x y z): Machine {x} breakdown from time {y} to {z}\n")
            for machine, intervals in events['breakdowns'].items():
                for start, end in intervals:
                    f.write(f"{machine} {start} {end}\n")
        if 'added_jobs' in events:
            f.write("\n# Added Jobs (first number is the time at which the job is added, then pairs of machines and process time)\n")
            for job_time, new_job in events['added_jobs']:
                f.write(f"{job_time}: " + " ".join(f"{op[0]} {op[1]}" for op in new_job) + "\n")
        if 'cancelled_jobs' in events:
            f.write("\n# Cancelled Jobs\n")
            for cancel_time, cancelled_job in events['cancelled_jobs']:
                f.write(f"{cancel_time} {cancelled_job}\n")


# Generare evenimente dinamice
def add_dynamic_events(num_machines, num_jobs, jobs, probabilities, max_breakdowns=0.2, max_breakdown_time = 0.05, max_added_jobs=0.2):
    events = {
        'breakdowns': {m: [] for m in range(num_machines)},
        'added_jobs': [],
        'cancelled_jobs': []
    }

    max_total_machine_processing_time=0
    # Gestionare breakdowns pentru mașini
    for machine in range(num_machines):
        # Totalul de timp necesar pentru procesarea evenimentelor pe această mașină
        total_machine_processing_time = sum(op[1] for job in jobs for op in job if op[0] == machine)
        if total_machine_processing_time > max_total_machine_processing_time: max_total_machine_processing_time = total_machine_processing_time

        max_duration = max(1, int(total_machine_processing_time * max_breakdown_time))

        avg_processing_time = total_machine_processing_time / num_jobs

        total_breakdown_time = 0
        last_breakdown_end_time = 0
        true_breakdowns = 0
        breakdowns_number = int(max_breakdowns*num_jobs)
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
            duration = min(max_duration, random.randint(int(0.75 * avg_processing_time), int(2 * avg_processing_time)))

            if total_breakdown_time >= max_duration:
                break

            end = start + duration
            if machine not in events['breakdowns']:
                events['breakdowns'][machine] = []
            events['breakdowns'][machine].append((start, end))

            last_breakdown_end_time = end

    # Gestionare anularea unui job
    for job_id in range(len(jobs)):
        if random.random() < probabilities['cancel_job']:
            cancel_time = random.randint(int(0.05*max_total_machine_processing_time), int(0.75*max_total_machine_processing_time))  # Timpul la care jobul este anulat
            events['cancelled_jobs'].append((cancel_time, job_id))



    # Adăugarea unui job nou
    new_jobs_number = int(max_added_jobs*num_jobs)
    min_processing_time = min(op[1] for job in jobs for op in job)
    max_processing_time = max(op[1] for job in jobs for op in job)
    for _ in range(new_jobs_number):
        if random.random() < probabilities['create_job']:
            new_job_time = random.randint(int(0.05*max_total_machine_processing_time), int(0.45*max_total_machine_processing_time))  # Timpul la care jobul este introdus
            machine_order = random.sample(range(num_machines), num_machines)
            new_job = [
                (machine, random.randint(min_processing_time, max_processing_time))
                for machine in machine_order
            ]
            # Adăugăm jobul nou la evenimente
            events['added_jobs'].append((new_job_time, new_job))

    return events


# Procesarea tuturor fișierelor din director
def process_instances(input_dir, output_dir, num_variants=3, probabilities=None, max_breakdowns=0.2, max_breakdown_percent = 0.05, max_added_jobs=0.2):
    if probabilities is None:
        probabilities = {
            'breakdown': 0.2,  # Probabilitate ca o mașină să aibă breakdown
            'cancel_job': 0.3,  # Probabilitate de anulare a unui job
            'create_job': 0.5  # Probabilitate de creare a unui job nou
        }

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for file_name in os.listdir(input_dir):
        input_path = os.path.join(input_dir, file_name)
        if os.path.isfile(input_path):
            num_machines, num_jobs, jobs = read_instance(input_path)

            for i in range(num_variants):  # Generăm 2-3 variante dinamice pentru fiecare fișier
                events = add_dynamic_events(
                    num_machines, num_jobs, jobs, probabilities, max_breakdowns, max_breakdown_percent, max_added_jobs
                )
                output_path = os.path.join(output_dir, f"{os.path.splitext(file_name)[0]}_dynamic_{i + 1}.txt")
                write_instance(output_path, num_machines, num_jobs, jobs, events)


# Exemplu de utilizare
input_directory = "JSP-static-benchmark-master/instances"  # Directorul cu fișierele originale
output_directory = "dynamic_JSP_instances"  # Directorul pentru fișierele generate
probabilities = {
    'breakdown': 0.2,  # Probabilitate ca o mașină să aibă breakdown
    'cancel_job': 0.15,  # Probabilitate de anulare a unui job
    'create_job': 0.3  # Probabilitate de creare a unui job nou
}
process_instances(input_directory, output_directory, num_variants=5, probabilities=probabilities, max_breakdowns=0.2,
                  max_added_jobs=0.2)
