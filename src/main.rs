use color_eyre::eyre::Result;

fn main() -> Result<()> {
    color_eyre::install()?;
    csv_to_json::run()?;
    Ok(())
}
