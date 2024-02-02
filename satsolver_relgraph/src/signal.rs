use loopy_relations::InterruptId;

pub const ASSIGNMENT_CONFLICT: InterruptId = 0;
pub const VIOLATED_RULE: InterruptId = 1;
pub const ROOT_CONFLICT: InterruptId = 2;
pub const SINGLETON_DISCOVERED_1: InterruptId = 3;
pub const EQUIVALENCE_DISCOVERED: InterruptId = 4;
pub const SINGLETON_DISCOVERED_2: InterruptId = 5;
pub const BINARY_DISCOVERED: InterruptId = 6;
pub const SELECTION_INVALIDATED: InterruptId = 7;
