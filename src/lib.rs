use crate::parsing::arg_parse;
use clap::Parser;
use color_eyre::eyre::Result;
use csv::{Reader, StringRecord};
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use serde_json::{map::Map, Value};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

mod parsing;

#[derive(Parser)]
#[clap(name = "CsvToJson")]
#[clap(version = "0.1")]
#[clap(about = "Converts csv files to json", long_about = None)]
pub struct ApplicationOptions {
    #[clap(long, short, num_args=1..)]
    pub input: Vec<String>,

    #[clap(long, short)]
    pub output: Option<String>,
}

#[derive(Debug)]
pub struct ProcessingUnit {
    input: PathBuf,
    output: PathBuf,
}

pub fn convert_line(headers: &[String], record: &StringRecord) -> Result<Value> {
    let mut line = Map::new();
    headers.iter().enumerate().for_each(|(i, h)| {
        let value = record.get(i).unwrap().to_string();
        line.insert(h.to_string(), Value::String(value));
    });

    Ok(Value::Object(line))
}

pub fn write_to_file(mut rdr: Reader<File>, headers: &[String], output: &PathBuf) -> Result<()> {
    let mut file_handler = File::create(output)?;
    file_handler.write_all(b"[")?;
    for (i, record) in rdr.records().filter_map(Result::ok).enumerate() {
        if i > 0 {
            file_handler.write_all(b",\n")?;
        }
        let converted_line_output = convert_line(headers, &record)?;
        serde_json::to_writer(&mut file_handler, &converted_line_output)?;
    }
    file_handler.write_all(b"]")?;

    Ok(())
}

pub fn write_to_stdout(mut rdr: Reader<File>, headers: &[String]) -> Result<()> {
    println!("[");
    for (i, record) in rdr.records().filter_map(Result::ok).enumerate() {
        let converted_line_output = convert_line(headers, &record)?;
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

pub fn convert_data(processing_unit: &ProcessingUnit) -> Result<()> {
    if !Path::exists(Path::new(&processing_unit.input)) {
        panic!("{:?}", &processing_unit.input);
    }

    let mut rdr = Reader::from_path(&processing_unit.input)?;
    let headers: Vec<String> = rdr
        .headers()?
        .iter()
        .map(|s| String::from(s).replace('\"', "\\\""))
        .collect();

    write_to_file(rdr, &headers, &processing_unit.output)?;
    Ok(())
}

pub fn run_by_option(options: &ApplicationOptions) -> Result<()> {
    let files = collect_files(options);
    let r: Result<Vec<_>, _> = files.par_iter().map(convert_data).collect();
    r.map(|_| ())
}

pub fn run() -> Result<()> {
    let options: ApplicationOptions = arg_parse();
    run_by_option(&options)
}
