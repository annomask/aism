use std::io::Result;
fn main() -> Result<()> {
    let cur = std::env::current_dir()?;
    let mut config = prost_build::Config::new();
    config.out_dir(cur.join("src/series")).compile_protos(
        &[cur.join("src/series/meta.proto")],
        &[cur.join("src/series")],
    )?;

    Ok(())
}
