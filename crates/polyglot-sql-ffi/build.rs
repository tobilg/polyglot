fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("missing CARGO_MANIFEST_DIR");
    let config_path = format!("{}/cbindgen.toml", crate_dir);
    let config = cbindgen::Config::from_file(&config_path).expect("failed to read cbindgen.toml");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
        .expect("Unable to generate C bindings")
        .write_to_file("polyglot_sql.h");
}
