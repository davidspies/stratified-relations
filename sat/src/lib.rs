use std::{
    fmt::{Debug, Formatter},
    ops::Not,
};

pub mod parse;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct RuleIndex(pub usize);

pub type Level = usize;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Atom(pub usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Sign {
    Neg,
    Pos,
}

impl Atom {
    #[allow(clippy::should_implement_trait)]
    pub fn neg(self) -> Literal {
        Literal(-(self.0 as isize))
    }

    pub fn pos(self) -> Literal {
        Literal(self.0 as isize)
    }

    pub fn with_sign(&self, sign: Sign) -> Literal {
        match sign {
            Sign::Neg => self.neg(),
            Sign::Pos => self.pos(),
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct Literal(pub isize);

impl Debug for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for Literal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Literal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.atom_and_sign().cmp(&other.atom_and_sign())
    }
}

impl Literal {
    pub fn atom(self) -> Atom {
        Atom(self.0.unsigned_abs())
    }

    pub fn sign(&self) -> Sign {
        self.atom_and_sign().1
    }

    pub fn atom_and_sign(&self) -> (Atom, Sign) {
        match self.0.cmp(&0) {
            std::cmp::Ordering::Less => (Atom((-self.0) as usize), Sign::Neg),
            std::cmp::Ordering::Equal => panic!("Invalid Literal 0"),
            std::cmp::Ordering::Greater => (Atom(self.0 as usize), Sign::Pos),
        }
    }
}

impl Not for Literal {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self(-self.0)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum LiteralCause {
    DecisionLiteral(Level),
    Propogated(RuleIndex, Level),
}

impl LiteralCause {
    pub fn level(self) -> Level {
        match self {
            Self::DecisionLiteral(level) => level,
            Self::Propogated(_, level) => level,
        }
    }
}

pub fn format_rule(rule: &[Literal]) -> String {
    rule.iter()
        .map(|x| format!("{} ", x.0))
        .chain([String::from("0")])
        .collect::<String>()
}

pub fn sanitize_rule(mut rule: Vec<Literal>) -> Result<Vec<Literal>, Atom> {
    rule.sort();
    rule.dedup();
    for slice in rule.windows(2) {
        let &[x, y] = slice else { unreachable!() };
        if x == !y {
            return Err(x.atom());
        }
    }
    Ok(rule)
}
