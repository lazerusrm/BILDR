fn main() {
    if let Err(error) = harness_desktop::run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
