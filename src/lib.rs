use std::collections::HashMap;
use std::error::Error;
use std::ffi::OsStr;
use std::fmt::{Debug, format, Formatter, Pointer};
use std::fs::File;
use std::io::Write;
use std::path::{MAIN_SEPARATOR, Path, PathBuf};
use std::thread;
use crossbeam::channel::{bounded, unbounded};
use crossbeam::select;
use crossbeam_utils::thread::scope;


use csv::{Reader, StringRecord};

pub fn arg_parse(args: Vec<String>) -> ApplicationOptions {
    let input: String = String::from("--input");
    let output: String = String::from("--output");
    let quiet: String = String::from("--quiet");

    let mut options = ApplicationOptions::default();
    // assume only input provided, write to std out
    if args.len() == 2 {
        let input_csv = args[1_usize].clone();
        options.input = input_csv.clone();
        options.output = String::from("");
        return options;
    }
    for (i, a) in args.iter().enumerate() {
        if input.eq(a) {
            let input_csv = args[i + 1_usize].clone();
            options.input = input_csv;
        }

        if output.eq(a) {
            let output_json = args[i + 1_usize].clone();
            options.output = output_json;
        }

        if quiet.eq(a) {
            options.quiet = true;
        }
    }
    return options;
}

pub fn build_json_line(record: HashMap<String, String>, header: StringRecord) -> String {
    let mut line = "{".to_string();
    // consistent key order
    for h in &header {
        let value = (record.get(h).unwrap()).to_string();
        line.push('"');
        line.push_str(&h.replace("\"", "\\\""));
        line.push_str("\":\"");
        line.push_str(&value.replace("\"", "\\\""));
        line.push_str("\",");
    }

    // remove last comma
    let mut a = line[0..line.len() - 1].to_string();
    a.push_str("}\n");
    a
}

pub fn read_data(options: &ApplicationOptions) -> (Vec<HashMap<String, String>>, StringRecord) {
    if !Path::exists(Path::new(&options.input)) {
        panic!("{:?}", &options.input);
    }

    let mut rdr = Reader::from_path(&options.input).unwrap();
    let header = rdr.headers().unwrap().clone();
    let data: Vec<HashMap<String, String>> = rdr
        .records()
        .map(|record| {
            Ok(header
                .iter()
                .map(|e| e.to_string())
                .zip(record?.iter().map(String::from))
                .collect())
        })
        .collect::<Result<_, Box<dyn Error>>>().unwrap();
    (data, header)
}

fn run_to_stdout(data: Vec<HashMap<String, String>>, header: StringRecord) {
    for record in data {
        let line = build_json_line(record, header.clone());
        print!("{}", line)
    }
}

fn run_to_file(data: Vec<HashMap<String, String>>, header: StringRecord, options: ApplicationOptions) {
    let mut file_handler = File::create(&options.output).unwrap();
    for record in data {
        let line = build_json_line(record, header.clone());
        let b = line.as_bytes();
        file_handler.write_all(b).unwrap();
    }
}

fn to_absolute(option: &ApplicationOptions, path: &PathBuf) -> String {
    let last = option.input.split(MAIN_SEPARATOR).last().unwrap();
    let last_with_separator = format!("{}{}", String::from(MAIN_SEPARATOR), String::from(last));
    let prefix = option.input.replace(&last_with_separator, &String::from(""));
    format!(
        "{}/{}.{}",
        prefix,
        path.file_stem().unwrap().to_str().unwrap(),
        path.extension().unwrap().to_str().unwrap(),
    )
}


fn run_files_channel(options: ApplicationOptions) {
    let mut files_to_process = Vec::new();

    // Prepare data for processing
    for entry in glob::glob(&options.input).unwrap() {
        match entry {
            Ok(path) => {
                let file_name = path.display();
                println!("{:?}", file_name);

                let mut patched_options = options.clone();
                patched_options.input = to_absolute(&patched_options, &path);

                if options.output.is_empty() {
                    patched_options.output = format!("{}.json", file_name);
                } else {
                    patched_options.output = format!("{}/{}.json", options.output, file_name);
                }
                files_to_process.push(patched_options)
            }

            // if the path matched but was unreadable,
            // thereby preventing its contents from matching
            Err(e) => println!("{:?}", e),
        }
    }

    let (s, r) = crossbeam::channel::unbounded();
    for e in files_to_process {
        s.send(e).unwrap()
    }

    drop(s);


    for e in r.try_iter() {
        println!("{:?}", e);
        process(e)
    }
}


fn run_files(options: ApplicationOptions) {
    for entry in glob::glob(&options.input).unwrap() {
        match entry {
            Ok(path) => {
                let file_name = path.display();
                println!("{:?}", file_name);

                let mut patched_options = options.clone();
                patched_options.input = to_absolute(&options, &path);

                if options.output.is_empty() {
                    patched_options.output = format!("{}.json", file_name);
                } else {
                    patched_options.output = format!("{}/{}.json", options.output, file_name);
                }

                let (data, header) = read_data(&patched_options);

                run_to_file(data, header, patched_options)
            }

            // if the path matched but was unreadable,
            // thereby preventing its contents from matching
            Err(e) => println!("{:?}", e),
        }
    }
}

fn process(options: ApplicationOptions) {
    let (data, header) = read_data(&options);
    run_to_file(data, header, options)
}

pub fn run(options: ApplicationOptions) -> Result<(), Box<dyn Error>> {
    if options.input.contains("*") && options.output == "" {
        // run_files(options.clone())
        run_files_channel(options.clone())
    } else {
        if options.output.is_empty() {
            let (data, header) = read_data(&options);
            run_to_stdout(data, header)
        } else {
            process(options)
        }
    }


    Ok(())
}

pub struct ApplicationOptions {
    pub input: String,
    pub output: String,
    pub quiet: bool,
}

impl Clone for ApplicationOptions {
    fn clone(&self) -> Self {
        ApplicationOptions {
            input: self.input.clone(),
            output: self.output.clone(),
            quiet: self.quiet.clone(),
        }
    }
}


impl Default for ApplicationOptions {
    fn default() -> Self {
        Self {
            input: String::from("*.csv"),
            output: String::from("."),
            quiet: false,
        }
    }
}

impl Debug for ApplicationOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.input)
            .field(&self.output)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::assert_eq;
    use std::collections::HashMap;
    use csv::StringRecord;

    #[test]
    fn test_build_json_line() {
        use super::build_json_line;
        let mut data = HashMap::new();
        let mut header = StringRecord::new();
        header.push_field("test-key");
        data.insert(String::from("test-key"), String::from("test-value"));
        let json = build_json_line(data, header);
        assert_eq!(json, "{\"test-key\":\"test-value\"}\n")
    }

    #[test]
    fn test_buid_json_line_with_doublequotes() {
        use super::build_json_line;
        let mut data = HashMap::new();
        let mut header = StringRecord::new();
        header.push_field("test-key");
        data.insert(String::from("test-key"), String::from("test\"-value"));
        let json = build_json_line(data, header);
        assert_eq!(json, "{\"test-key\":\"test\\\"-value\"}\n")
    }
}
