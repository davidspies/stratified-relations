#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path
from typing import List


def execute_subprocess(command: List[str], verbose: bool) -> str:
    stdout_lines: List[str] = []
    stderr_option = (
        None if verbose else subprocess.PIPE
    )  # Direct inheritance of stderr in verbose mode

    with subprocess.Popen(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=stderr_option,
    ) as process:
        assert process.stdout  # Ensure stdout is not None for type checkers
        for line in iter(process.stdout.readline, ""):
            if verbose:
                print(line, end="")  # Forward stdout in real-time if verbose
            stdout_lines.append(line)

        process.wait()

        if process.returncode != 0:
            raise subprocess.CalledProcessError(
                process.returncode, command, "".join(stdout_lines)
            )

    return "".join(stdout_lines)


def run_solver(
    cnf_path: Path,
    edrat_proof_path: Path,
    drat_proof_path: Path,
    verbose: bool = False,
) -> bool:
    stdout_content = execute_subprocess(
        [
            "cargo",
            "run",
            "-r",
            "--package",
            "satsolver",
            str(cnf_path),
            "-e",
            str(edrat_proof_path),
        ],
        verbose,
    )

    is_sat = "v SATISFIABLE" in stdout_content

    if not is_sat:
        _ = execute_subprocess(
            [
                "cargo",
                "run",
                "-r",
                "--package",
                "edrat_translator",
                str(cnf_path),
                str(edrat_proof_path),
                str(drat_proof_path),
            ],
            verbose,
        )

    return is_sat


def run_drat_trim(cnf_path: Path, drat_proof_path: Path, verbose: bool = False) -> bool:
    try:
        stdout_content = execute_subprocess(
            ["../drat-trim/drat-trim", str(cnf_path), str(drat_proof_path)],
            verbose,
        )
        return "s VERIFIED" in stdout_content
    except subprocess.CalledProcessError as e:
        # Yup, this can happen.
        if "s VERIFIED" in e.stdout:
            return True
        else:
            raise


def main(cnf: Path, edrat_proof: Path, drat_proof: Path):
    satisfiable = run_solver(cnf, edrat_proof, drat_proof, verbose=True)
    if not satisfiable:
        run_drat_trim(cnf, drat_proof, verbose=True)


if __name__ == "__main__":
    main(*[Path(arg) for arg in sys.argv[1:]])
