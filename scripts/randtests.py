#!/usr/bin/env python3
import random
import sys
from pathlib import Path

from pysat.formula import CNF  # type: ignore

scripts_dir = Path(__file__).parent.resolve()

if str(scripts_dir) not in sys.path:
    sys.path.append(str(scripts_dir))

from pipeline import run_drat_trim, run_solver

CNF_PATH = Path("/tmp/random_cnf.cnf")
EDRAT_PROOF_PATH = Path("/tmp/proof.edrat")
DRAT_PROOF_PATH = Path("/tmp/proof.drat")


def generate_random_sat_instance(num_vars: int, num_clauses: int) -> CNF:
    cnf = CNF()
    for _ in range(num_clauses):
        clause = [
            random.randint(1, num_vars) * (1 if random.random() < 0.5 else -1)
            for _ in range(3)
        ]
        cnf.append(clause)  # type: ignore
    return cnf


def main(*args: str):
    if args:
        [n] = args
        n = int(n)
    else:
        n = 100
    sat_count = 0
    unsat_count = 0
    for _i in range(n):
        num_vars = 50
        num_clauses = 215

        cnf = generate_random_sat_instance(num_vars, num_clauses)
        cnf.to_file(CNF_PATH)  # type: ignore

        if run_solver(CNF_PATH, EDRAT_PROOF_PATH, DRAT_PROOF_PATH):
            sat_count += 1
            print("SAT instance")
        else:
            if not run_drat_trim(CNF_PATH, DRAT_PROOF_PATH):
                print("Failure: drat-trim verification failed")
                print("CNF file:", CNF_PATH)
                sys.exit(1)
            else:
                unsat_count += 1
                print("Success: UNSAT and verified")
    print(f"sat: {sat_count}, unsat: {unsat_count}")


if __name__ == "__main__":
    main(*sys.argv[1:])
