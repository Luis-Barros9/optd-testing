use std::{
	fs,
	io::{self, BufRead, BufReader, Read},
	path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(name = "separate_json")]
#[command(about = "Split a memo JSON object into one NDJSON file per table")]
pub struct Cli {
	/// Read the input JSON from a file instead of stdin.
	#[arg(short = 'f', long, value_name = "FILE")]
	pub input_file: Option<PathBuf>,

	/// Output directory for the generated .ndjson files.
	#[arg(short = 'o', long, value_name = "DIR", default_value = "data/memoData")]
	pub output_dir: PathBuf,
}

pub fn run() -> Result<()> {
	let cli = Cli::parse();
	let input = get_input(cli.input_file.as_deref())?;
	split_json_object_into_ndjson_files(input, &cli.output_dir)
}



fn get_input(input_file: Option<&Path>) -> Result<Box<dyn BufRead>> {
	match input_file {
		Some(path) => Ok(Box::new(
			BufReader::new(
				fs::File::open(path)
					.with_context(|| format!("failed to open input file '{}'", path.display()))?,
			),
		)),
		None => Ok(Box::new(BufReader::new(io::stdin()))),
	}
}


pub fn split_json_object_into_ndjson_files(json_input: impl BufRead, output_dir: &Path) -> Result<()> {
	let value: Value = serde_json::from_reader(BomStrippingReader::new(json_input))
		.context("input is not valid JSON")?;
	let Some(object) = value.as_object() else {
		return Err(anyhow::Error::msg(
			"expected a top-level JSON object in the shape {\"Tab1\":[...], \"Tab2\":[...]}",
		));
	};

	fs::create_dir_all(output_dir)
		.with_context(|| format!("failed to create output directory '{}'", output_dir.display()))?;

	let mut tables: Vec<(&str, &Value)> = object.iter().map(|(name, rows)| (name.as_str(), rows)).collect();
	tables.sort_by(|(left_name, _), (right_name, _)| left_name.to_lowercase().cmp(&right_name.to_lowercase()));

	for (table_name, rows_value) in tables {
		let rows = match rows_value.as_array() {
			Some(rows) => rows,
			None => {
				return Err(anyhow::Error::msg(format!("table '{}' must be an array", table_name)));
			}
		};

		let file_name = format!("{}.ndjson", normalize_file_stem(table_name));
		let output_path = output_dir.join(file_name);

		let mut buffer = String::new();
		for row in rows {
			buffer.push_str(&serde_json::to_string(row).context("failed to serialize row as JSON")?);
			buffer.push('\n');
		}

		fs::write(&output_path, buffer)
			.with_context(|| format!("failed to write '{}'", output_path.display()))?;
	}

	Ok(())
}

fn normalize_file_stem(name: &str) -> String {
	let normalized: String = name
		.chars()
		.map(|ch| {
			if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
				ch.to_ascii_lowercase()
			} else {
				'_'
			}
		})
		.collect();

	if normalized.is_empty() {
		"table".to_string()
	} else {
		normalized
	}
}

struct BomStrippingReader<R> {
	inner: R,
	bom_checked: bool,
}

impl<R> BomStrippingReader<R> {
	fn new(inner: R) -> Self {
		Self {
			inner,
			bom_checked: false,
		}
	}
}


impl<R: BufRead> Read for BomStrippingReader<R> {
	fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
		if !self.bom_checked {
			self.bom_checked = true;
			let buffer = self.inner.fill_buf()?;
			if buffer.starts_with(&[0xEF, 0xBB, 0xBF]) {
				self.inner.consume(3);
			}
		}

		self.inner.read(output)
	}
}
