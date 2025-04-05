import concurrent.futures
import operator
import random
from deap import base, creator, tools, gp

def protected_div(a, b):
    return a / b if abs(b) > 1e-9 else a

def create_toolbox(np = 5):
    """
    Creează și returnează un obiect `toolbox` DEAP cu
    definirea primitivelor GP, a tipurilor de date și
    operatorilor de încrucișare/mutare selecție etc.
    """
    print("Create toolbox")
    pset = gp.PrimitiveSet("MAIN", 7)
    pset.renameArguments(ARG0='PT')  # Processing Time
    pset.renameArguments(ARG1='RO')  # Remaining Operations
    pset.renameArguments(ARG2='MW')  # Machine Wait
    pset.renameArguments(ARG3='TQ')  # Time in Queue
    pset.renameArguments(ARG4='WIP')  # Work In Progress
    pset.renameArguments(ARG5='RPT')  # Remaining Processing Time (job-level)
    pset.renameArguments(ARG6='TUF')  # Time Until Fixed (sau Until next breakdown)


    pset.addPrimitive(operator.add, 2)
    pset.addPrimitive(operator.sub, 2)
    pset.addPrimitive(operator.mul, 2)
    pset.addPrimitive(protected_div, 2)
    pset.addPrimitive(operator.neg, 1)
    pset.addPrimitive(min, 2)
    pset.addPrimitive(max, 2)

    pset.addTerminal(1.0)
    pset.addTerminal(0.0)

    creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
    creator.create("Individual", gp.PrimitiveTree, fitness=creator.FitnessMin)

    toolbox = base.Toolbox()
    toolbox.register("expr", gp.genFull, pset=pset, min_=1, max_=3)
    toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.expr)
    toolbox.register("population", tools.initRepeat, list, toolbox.individual)
    toolbox.register("compile", gp.compile, pset=pset)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=np)
    # Pas 2: Să folosim executorul pentru evaluare în paralel
    toolbox.register("map", executor.map)

    toolbox.pset = pset

    # De notat: nu configurăm aici încă 'evaluate', 'select', etc.
    # pentru că le putem seta din alt modul (evaluator.py).
    return toolbox
