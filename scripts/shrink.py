#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path
from typing import Generator, List, Optional, Tuple

from pysat.formula import CNF  # type: ignore

scripts_dir = Path(__file__).parent.resolve()

if str(scripts_dir) not in sys.path:
    sys.path.append(str(scripts_dir))

from pipeline import run_drat_trim, run_solver

EDRAT_PROOF_PATH = Path("/tmp/proof.edrat")
DRAT_PROOF_PATH = Path("/tmp/proof.drat")
TMP_CNF = Path("/tmp/temp_cnf.cnf")


def read_cnf_file(file_path: Path) -> CNF:
    return CNF(from_file=str(file_path))


def write_cnf_file(file_path: Path, cnf: CNF):
    with file_path.open("w") as file:
        clauses = get_clauses(cnf)
        file.write(f"c Redundant link count {len(find_unique_binary_literals(cnf))}\n")
        file.write(f"p cnf {count_vars(cnf)} {len(clauses)}\n")
        for clause in clauses:
            file.write(" ".join(map(str, clause)) + " 0\n")


def get_clauses(cnf: CNF) -> List[List[int]]:
    return cnf.clauses  # type: ignore


def count_vars(cnf: CNF) -> int:
    return cnf.nv  # type: ignore


last_ubin_index = 0


def shrink_singleton_rules(cnf: CNF) -> Generator[CNF, None, None]:
    clauses = get_clauses(cnf)
    for clause in clauses:
        if len(clause) == 1:
            [x] = clause
            new_cnf = CNF()
            new_cnf.extend([[z for z in clause if z != -x] for clause in clauses if x not in clause])  # type: ignore
            yield remove_unused_vars(new_cnf)


def shrink_redundant_links(cnf: CNF) -> Generator[CNF, None, None]:
    global last_ubin_index
    ubins = find_unique_binary_literals(cnf)
    clauses = get_clauses(cnf)
    if last_ubin_index >= len(ubins):
        last_ubin_index = 0
    start = last_ubin_index
    for i in range(start, len(ubins)):
        last_ubin_index = i
        (x, (ri, y)) = ubins[i]
        new_cnf = CNF()
        new_cnf.extend([[y if z == -x else z for z in clause] for clause in clauses[:ri]])  # type: ignore
        new_cnf.extend([[y if z == -x else z for z in clause] for clause in clauses[ri + 1 :]])  # type: ignore
        yield remove_unused_vars(new_cnf)
    for i in range(start):
        last_ubin_index = i
        (x, (ri, y)) = ubins[i]
        new_cnf = CNF()
        new_cnf.extend([[y if z == -x else z for z in clause] for clause in clauses[:ri]])  # type: ignore
        new_cnf.extend([[y if z == -x else z for z in clause] for clause in clauses[ri + 1 :]])  # type: ignore
        yield remove_unused_vars(new_cnf)


last_clause_index = 0


def shrink_cnf_clauses(cnf: CNF) -> Generator[CNF, None, None]:
    global last_clause_index
    clauses = get_clauses(cnf)
    if last_clause_index >= len(clauses):
        last_clause_index = 0
    start = last_clause_index
    for i in range(start, len(clauses)):
        last_clause_index = i
        new_cnf = CNF()
        new_cnf.extend(clauses[:i] + clauses[i + 1 :])  # type: ignore
        yield remove_unused_vars(new_cnf)
    for i in range(start):
        last_clause_index = i
        new_cnf = CNF()
        new_cnf.extend(clauses[:i] + clauses[i + 1 :])  # type: ignore
        yield remove_unused_vars(new_cnf)


def remove_unused_vars(cnf: CNF) -> CNF:
    used_vars: set[int] = set()
    clauses = get_clauses(cnf)
    for clause in clauses:
        used_vars.update(abs(var) for var in clause)

    # Create a mapping from old vars to new, compacted vars
    remapping = {
        old_var: new_var for new_var, old_var in enumerate(sorted(used_vars), start=1)
    }

    # Apply the mapping to the CNF
    new_clauses: list[list[int]] = []
    for clause in clauses:
        new_clause = [remapping[abs(var)] * (1 if var > 0 else -1) for var in clause]
        new_clauses.append(new_clause)

    new_cnf = CNF()
    new_cnf.extend(new_clauses)  # type: ignore
    return new_cnf


def shrink_cnf_literals(cnf: CNF) -> Generator[CNF, None, None]:
    clauses = get_clauses(cnf)
    for i, clause in enumerate(clauses):
        for j in range(len(clause)):
            new_clause = clause[:j] + clause[j + 1 :]
            if new_clause:
                new_cnf = CNF()
                new_cnf.extend(clauses[:i] + [new_clause] + clauses[i + 1 :])  # type: ignore
                yield remove_unused_vars(new_cnf)


def try_shrinks(cnf: CNF) -> Optional[CNF]:
    for shrunken_cnf in shrink_singleton_rules(cnf):
        if not test_cnf_instance(shrunken_cnf):
            return shrunken_cnf
    for shrunken_cnf in shrink_redundant_links(cnf):
        if not test_cnf_instance(shrunken_cnf):
            return shrunken_cnf
    for shrunken_cnf in shrink_cnf_clauses(cnf):
        if not test_cnf_instance(shrunken_cnf):
            return shrunken_cnf
    for shrunken_cnf in shrink_cnf_literals(cnf):
        if not test_cnf_instance(shrunken_cnf):
            return shrunken_cnf
    return None


def test_cnf_instance(cnf: CNF) -> bool:
    write_cnf_file(TMP_CNF, cnf)
    try:
        result = run_solver(TMP_CNF, EDRAT_PROOF_PATH, DRAT_PROOF_PATH)
    except subprocess.CalledProcessError:
        return False
    if result:
        print("SAT instance found")
        return True
    else:
        if run_drat_trim(TMP_CNF, DRAT_PROOF_PATH):
            print("UNSAT verified, moving to next shrink...")
            return True
        else:
            print("UNSAT not verified, continue shrinking...")
            return False


def find_unique_binary_literals(cnf: CNF) -> list[Tuple[int, Tuple[int, int]]]:
    literal_count: dict[int, int] = {}
    binary_clause_literals: dict[int, Tuple[int, int]] = dict()

    for i, clause in enumerate(get_clauses(cnf)):
        if len(clause) == 2:
            [x, y] = clause
            binary_clause_literals[x] = (i, y)
            binary_clause_literals[y] = (i, x)

        for lit in clause:
            literal_count[lit] = literal_count.get(lit, 0) + 1

    # Filter literals that appear exactly once and only in binary clauses
    unique_binary_literals = [
        (x, y) for (x, y) in binary_clause_literals.items() if literal_count[x] == 1
    ]
    unique_binary_literals.sort()

    return unique_binary_literals


def main(cnf_path: str, shrink_path: str):
    clauses = read_cnf_file(Path(cnf_path))
    while True:
        clauses = try_shrinks(clauses)
        if clauses is None:
            return
        else:
            write_cnf_file(Path(shrink_path), clauses)


if __name__ == "__main__":
    main(*sys.argv[1:])
