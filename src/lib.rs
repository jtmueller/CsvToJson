use crate::parsing::arg_parse;
use clap::Parser;
use color_eyre::eyre::Result;
use core::str::FromStr;
use csv::{Reader, StringRecord};
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use serde_json::{map::Map, Number, Value};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use wildmatch::WildMatch;

mod parsing;

#[derive(Parser, Debug)]
#[clap(name = "CsvToJson")]
#[clap(version = "0.1")]
#[clap(about = "Converts csv files to json", long_about = None)]
pub struct ApplicationOptions {
    /// Input file(s) to process
    #[clap(long, short, num_args=1..)]
    pub input: Vec<String>,

    /// Output file or directory
    #[clap(long, short)]
    pub output: Option<String>,

    /// Fields to convert to numbers. Simple wildcards are supported, if the name is in quotes.
    #[clap(long, num_args=1.., value_parser=parse_wildmatch)]
    pub numeric_fields: Option<Vec<WildMatch>>,

    /// Auto detect numbers - any value that cannot be parsed as a number will be a string
    #[clap(long)]
    pub auto_numbers: bool,

    /// Pretty print JSON output
    #[clap(long)]
    pub pretty_print: bool,
}

fn parse_wildmatch(pattern: &str) -> Result<WildMatch> {
    Ok(WildMatch::new(pattern))
}

#[derive(Debug)]
pub struct ProcessingUnit {
    input: PathBuf,
    output: PathBuf,
}

pub fn convert_line(
    headers: &[String],
    record: &StringRecord,
    options: &ApplicationOptions,
) -> Result<Value> {
    let mut line = Map::new();

    for (i, header_name) in headers.iter().enumerate() {
        let value = record.get(i).unwrap();
        let json_value = if options.auto_numbers {
            if let Ok(number) = Number::from_str(value) {
                Value::Number(number)
            } else {
                Value::String(value.to_string())
            }
        } else if let Some(numeric_fields) = &options.numeric_fields {
            if numeric_fields.iter().any(|f| f.matches(header_name)) {
                if value.len() == 0 {
                    Value::Null
                } else if let Ok(number) = Number::from_str(value) {
                    Value::Number(number)
                } else {
                    Value::String(value.to_string())
                }
            } else {
                Value::String(value.to_string())
            }
        } else {
            Value::String(value.to_string())
        };
        line.insert(header_name.to_string(), json_value);
    }

    Ok(Value::Object(line))
}

pub fn write_to_file(
    mut rdr: Reader<File>,
    headers: &[String],
    output: &PathBuf,
    options: &ApplicationOptions,
) -> Result<()> {
    let mut file_handler = File::create(output)?;
    file_handler.write_all(b"[")?;
    for (i, record) in rdr.records().filter_map(Result::ok).enumerate() {
        if i > 0 {
            file_handler.write_all(b",\n")?;
        }
        let converted_line_output = convert_line(headers, &record, options)?;
        if options.pretty_print {
            serde_json::to_writer_pretty(&mut file_handler, &converted_line_output)?;
        } else {
            serde_json::to_writer(&mut file_handler, &converted_line_output)?;
        }
    }
    file_handler.write_all(b"]")?;

    Ok(())
}

pub fn write_to_stdout(
    mut rdr: Reader<File>,
    headers: &[String],
    options: &ApplicationOptions,
) -> Result<()> {
    println!("[");
    for (i, record) in rdr.records().filter_map(Result::ok).enumerate() {
        let converted_line_output = convert_line(headers, &record, options)?;
        if i == 0 {
            print!("{}", converted_line_output);
        } else {
            println!(",");
            print!(",{}", converted_line_output);
        }
    }
    println!("]");

    Ok(())
}

fn build_output_path(output: &Option<String>, input: &Path) -> PathBuf {
    let mut output_directory = match output {
        None => PathBuf::new(),
        Some(o) => {
            let x = PathBuf::from(o);
            if x.to_string_lossy().contains(".json") {
                // explicit path, we assume a file - use it directly
                return x;
            } else {
                // base directory for processing
                x
            }
        }
    };

    let elements = input.iter();
    let size = input.iter().count();
    for (index, part) in elements.enumerate() {
        let index = index as i32;
        let size = size as i32 - 1;
        if index < size {
            output_directory.push(part)
        }
    }

    fs::create_dir_all(&output_directory).unwrap();
    let mut last = input.iter().last().unwrap().to_os_string();
    last.push(".json");
    output_directory.push(last);

    output_directory
}

pub fn collect_files(options: &ApplicationOptions) -> Vec<ProcessingUnit> {
    let mut files_to_process = Vec::new();

    for argument in &options.input {
        for entry in glob::glob(argument).unwrap() {
            match entry {
                Ok(input) => {
                    let output = build_output_path(&options.output, &input);

                    let processing_unit = ProcessingUnit { input, output };

                    files_to_process.push(processing_unit)
                }
                // if the path matched but was unreadable,
                // thereby preventing its contents from matching
                Err(e) => eprintln!("{:?}", e),
            }
        }
    }
    files_to_process
}

pub fn convert_data(processing_unit: &ProcessingUnit, options: &ApplicationOptions) -> Result<()> {
    if !Path::exists(Path::new(&processing_unit.input)) {
        panic!("{:?}", &processing_unit.input);
    }

    let mut rdr = Reader::from_path(&processing_unit.input)?;
    let headers: Vec<String> = rdr
        .headers()?
        .iter()
        .map(|s| String::from(s).replace('\"', "\\\""))
        .collect();

    write_to_file(rdr, &headers, &processing_unit.output, options)?;
    Ok(())
}

pub fn run_by_option(options: &ApplicationOptions) -> Result<()> {
    let files = collect_files(options);
    let r: Result<Vec<_>, _> = files
        .par_iter()
        .map(|pu| convert_data(pu, options))
        .collect();
    r.map(|_| ())
}

pub fn run() -> Result<()> {
    let options: ApplicationOptions = arg_parse();
    run_by_option(&options)
}
