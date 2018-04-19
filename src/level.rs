use table::Table;

pub struct LevelHandler {
    // initialize once
    level: u32,
    max_total_size: u64,

    tables: Vec<Table>,
    size: u64,
}

impl LevelHandler {
    pub fn new(level: u32, max_total_size: u64) -> LevelHandler {
        LevelHandler {
            level,
            max_total_size,

            size: 0,
            tables: vec![],
        }
    }
}
