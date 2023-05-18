use crate::ApplicationOptions;
use clap::Parser;
use log::info;

impl Clone for ApplicationOptions {
    fn clone(&self) -> Self {
        ApplicationOptions {
            input: self.input.clone(),
            output: self.output.clone(),
            numeric_fields: self.numeric_fields.clone(),
            pretty_print: self.pretty_print,
            auto_numbers: self.auto_numbers,
        }
    }
}

impl Default for ApplicationOptions {
    fn default() -> Self {
        Self {
            input: vec!["".to_owned()],
            output: None,
            numeric_fields: None,
            pretty_print: false,
            auto_numbers: false,
        }
    }
}

pub fn arg_parse() -> ApplicationOptions {
    env_logger::init();

    let cli = ApplicationOptions::parse();

    info!("Parsed following arguments: ");
    info!("{:?}", &cli);
    cli
}
