use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use datafusion::{
    arrow::array::RecordBatch,
    common::{config::ConfigExtension, extensions_options},
};

extensions_options! {
   /// optd configuration in datafusion.
   pub struct OptdExtensionConfig {
       /// Should try run optd optimizer instead of datafusion default.
       pub optd_enabled: bool, default = true
       /// Should fail on any unsupported features.
       pub optd_strict_mode: bool, default = false
   }
}

impl ConfigExtension for OptdExtensionConfig {
    const PREFIX: &'static str = "optd";
}

/// The optd datafusion extension used to store shared state.
#[derive(Debug)]
pub struct OptdExtension;

pub type MemoRows = HashMap<String, Vec<RecordBatch>>;

static MEMO_PRELOAD_ROWS: OnceLock<Mutex<Option<MemoRows>>> = OnceLock::new();

static USE_PERSISTENT_MEMO: OnceLock<Mutex<Option<bool>>> = OnceLock::new();




fn memo_preload_rows_store() -> &'static Mutex<Option<MemoRows>> {
    MEMO_PRELOAD_ROWS.get_or_init(|| Mutex::new(None))
}

pub fn set_memo_preload_rows(rows: MemoRows) {
    let mut guard = memo_preload_rows_store().lock().unwrap();
    *guard = Some(rows);
}

pub fn take_memo_preload_rows() -> Option<MemoRows> {
    let mut guard = memo_preload_rows_store().lock().unwrap();
    guard.take()
}

pub fn clear_memo_preload_rows() {
    let mut guard = memo_preload_rows_store().lock().unwrap();
    *guard = None;
}

pub fn set_persistent_memo(value: bool) {
    let mut guard = USE_PERSISTENT_MEMO.get_or_init(|| Mutex::new(None)).lock().unwrap();
    *guard = Some(value);
}

pub fn use_persistent_memo() -> bool {
    let guard = USE_PERSISTENT_MEMO.get_or_init(|| Mutex::new(None)).lock().unwrap();
    guard.unwrap_or(false)
}

