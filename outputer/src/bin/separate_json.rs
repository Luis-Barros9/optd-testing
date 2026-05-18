#[path = "../separate_json.rs"]
mod separate_json;

fn main() -> anyhow::Result<()> {
    separate_json::run()
}