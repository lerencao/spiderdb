pub mod table;

use std::fs;
pub struct Config {
    dir: String,
    value_dir: String,
    sync_write: bool,
    table_loading_mode: u8,
    value_log_loading_mode: u8,
    max_table_size: u64,
}

pub struct DB {}

impl DB {
    fn open(cfg: &Config) {
        let meta = fs::metadata(&cfg.dir);
        fs::create_dir_all(&cfg.dir).unwrap();
        fs::create_dir_all(&cfg.value_dir).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::rc::Weak;
    use std::sync::mpsc;
    #[test]
    fn it_works() {
        mpsc::channel();
        let d = Rc::new(1);
        Rc::clone(d);
        Rc::downgrade(d);
        Weak::new();

        assert_eq!(2 + 2, 4);
    }
}
