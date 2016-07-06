extern crate serde_json;
extern crate serde;
extern crate pbr;
extern crate regex;
extern crate docopt;
extern crate rustc_serialize;
extern crate env_logger;
extern crate ansi_term;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[macro_use]
extern crate sonicd;

mod util {
    #[cfg(feature = "serde_macros")]
    include!("util.rs.in");

    #[cfg(not(feature = "serde_macros"))]
    include!(concat!(env!("OUT_DIR"), "/util.rs"));
}

use std::path::PathBuf;
use pbr::ProgressBar;
use util::*;
use docopt::Docopt;
use std::process;
use ::std::io::{Stderr, stderr};

const USAGE: &'static str = "
.
                           d8b
                           Y8P

.d8888b   .d88b.  88888b.  888  .d8888b
88K      d88\"\"88b 888 \"88b 888 d88P\"
\"Y8888b. 888  888 888  888 888 888
     X88 Y88..88P 888  888 888 Y88b.
 88888P'  \"Y88P\"  888  888 888  \"Y8888P

Usage:
  sonic <source> [-d <var>...] [options] -e <query>
  sonic <source> [-d <var>...] [options] -f <file>
  sonic login [options]
  sonic -h | --help
  sonic --version

Options:
  --execute, -e         run command literal
  --file, -f            run command from file
  -c <config>           use config file (default: $HOME/.sonicrc)
  -d                    inject variable name to value (i.e. -d foo=bar)
  --silent, -S          output data only
  -r, --rows-only       print rows only
  -h, --help            show this message
  --version             show server and cli version
  --verbose             set log level to 'debug'
";

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
static COMMIT: Option<&'static str> = option_env!("SONIC_COMMIT");

#[derive(Debug, RustcDecodable)]
struct Args {
    arg_file: String,
    arg_query: String,
    flag_silent: bool,
    flag_rows_only: bool,
    arg_var: Vec<String>,
    flag_file: bool,
    flag_c: String,
    flag_verbose: bool,
    arg_source: String,
    cmd_login: bool,
    flag_execute: bool,
    flag_version: bool,
    flag_help: bool,
}

error_chain! {
    types {
        Error, ErrorKind, ChainErr, Result;
    }

    links {
        sonicd::Error, sonicd::ErrorKind, SonicdError;
    }

    foreign_links {
        ::std::io::Error, IoError, "I/O operation failed";
        ::serde_json::Error, Json, "JSON serialization/deserialization error";
    }

    errors {
        InvalidFormat(co: String, msg: String) {
            description("invalid format error")
                display("invalid format {}: {}", co, msg)
        }
    }
}

fn exec(host: &str, port: &u16, query: sonicd::Query, rows_only: bool, silent: bool) -> Result<()> {

    let mut pb: ProgressBar<Stderr> = ProgressBar::new(stderr(), 100);
    pb.format("╢░░_╟");
    pb.tick_format("▀▐▄▌");
    pb.show_message = true;

    let res = {
        let fn_out = |msg: sonicd::OutputChunk| {
            println!("{}",
                     msg.0
                         .iter()
                         .fold(String::new(), |acc, x| format!("{}{:?}\t", acc, x))
                         .trim_right());
        };

        let fn_meta = |msg: sonicd::TypeMetadata| {
            info!("received type metadata: {:?}", msg);
            if !rows_only {
                println!("{}",
                         msg.0
                             .iter()
                             .fold(String::new(), |acc, col| format!("{}{:?}\t", acc, col.0))
                             .trim_right());
            }
        };

        let fn_prog = |msg: sonicd::QueryProgress| {
            if !silent {
                let sonicd::QueryProgress { total, progress, status, .. } = msg;

                pb.message(&format!("{:?} ", status));

                if let Some(total) = total {
                    pb.total = total as u64;
                }

                if progress >= 1.0 {
                    pb.add(progress as u64);
                } else {
                    pb.tick();
                };
            }
        };

        sonicd::stream(query, (host, *port), fn_out, fn_prog, fn_meta)
    };

    if let Ok(_) = res {
        pb.message("Done ");
    } else {
        pb.message("Error ");
    }
    pb.finish();
    try!(res);
    Ok(())
}

fn _main(args: Args) -> Result<()> {

    let Args { arg_file,
               arg_query,
               flag_silent,
               flag_rows_only,
               arg_var,
               flag_file,
               flag_c,
               arg_source,
               cmd_login,
               flag_execute,
               flag_version,
               .. } = args;

    let ClientConfig { sonicd, tcp_port, sources, auth } = if flag_c != "" {
        debug!("sourcing passed config in path '{:?}'", &flag_c);
        try!(read_config(&PathBuf::from(flag_c)))
    } else {
        debug!("sourcing default config in path '$HOME/.sonicrc'");
        try!(get_default_config())
    };

    if flag_file {

        let query_str = try!(read_file_contents(&PathBuf::from(&arg_file)));
        let split = try!(split_key_value(&arg_var));
        let injected = try!(inject_vars(&query_str, &split));
        let query = try!(build(arg_source, sources, auth, injected));

        exec(&sonicd, &tcp_port, query, flag_rows_only, flag_silent)

    } else if flag_execute {

        let query = try!(build(arg_source, sources, auth, arg_query));

        exec(&sonicd, &tcp_port, query, flag_rows_only, flag_silent)

    } else if cmd_login {

        login(&sonicd, &tcp_port)

    } else if flag_version {

        println!("Sonic CLI version {} ({})",
                 VERSION,
                 COMMIT.unwrap_or_else(|| "dev"));

        Ok(())

    } else {

        panic!("unexpected args");

    }
}

fn main() {

    let args: Args = Docopt::new(USAGE).and_then(|d| d.decode()).unwrap_or_else(|e| e.exit());

    let verbose: bool = args.flag_verbose;

    if verbose {
        let mut builder = env_logger::LogBuilder::new();
        builder.parse("debug");
        builder.init().unwrap();
    } else {
        env_logger::init().unwrap();
    }

    debug!("parsed args {:?}", args);

    match _main(args) {
        Ok(_) => {}
        Err(e) => {
            report_error(&e, verbose);
            process::exit(1)
        }
    }
}
