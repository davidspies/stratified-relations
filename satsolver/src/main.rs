use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufReader, Write},
};

use sat::{Literal, parse};
use satsolver::Solver;

use clap::Parser;
use std::path::PathBuf;

/// SAT Solver program.
#[derive(Parser)]
struct Opts {
    /// Sets the input CNF file to use
    #[clap()]
    cnf: PathBuf,

    /// Sets the optional EDRAT proof file to write to
    #[clap(short, long)]
    edrat: Option<PathBuf>,
}

fn main() {
    let opts = Opts::parse();
    let rules = Vec::from_iter(parse::rules(BufReader::new(File::open(opts.cnf).unwrap())));
    let proof_output: Box<dyn Write> = match opts.edrat {
        Some(path) => Box::new(File::create(path).unwrap()),
        None => Box::new(io::sink()),
    };
    let Some(solver) = Solver::new(rules.clone(), proof_output) else {
        println!("v UNSATISFIABLE");
        return;
    };
    match solver.solve() {
        Some(solution) => {
            let solution_lits: HashSet<Literal> = solution.iter().copied().collect();
            println!("v SATISFIABLE");
            for lit in solution {
                print!("{} ", lit.0);
            }
            println!("0");
            for &x in &solution_lits {
                assert!(
                    !solution_lits.contains(&!x),
                    "Solution contains both {} and {}",
                    x.0,
                    (!x).0
                );
            }
            for rule in rules {
                assert!(
                    rule.iter().any(|&x| solution_lits.contains(&x)),
                    "No literal in rule {:?}",
                    rule.iter().map(|&x| x.0).collect::<Vec<_>>()
                );
            }
        }
        None => {
            println!("v UNSATISFIABLE");
        }
    }
}
