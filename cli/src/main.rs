#![feature(custom_derive, plugin, custom_attribute, box_syntax, lookup_host)]
#![plugin(docopt_macros, serde_macros)]
extern crate serde_json;
extern crate serde;
extern crate pbr;
extern crate regex;
extern crate docopt;
extern crate rustc_serialize;
extern crate env_logger;
extern crate ansi_term;
#[macro_use] extern crate error_chain;
#[macro_use] extern crate log;
#[macro_use] extern crate sonicd;

mod util;

use std::path::PathBuf;
use ansi_term::Colour::Red;
use pbr::ProgressBar;
use util::*;
use std::process;
use std::io::{Write};
use ::std::io::{Stderr, stderr};

docopt!(Args derive Debug, "
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
  sonic login
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
");

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
static COMMIT: Option<&'static str> = option_env!("SONIC_COMMIT");


error_chain! {
    types {
        Error, ErrorKind, ChainErr, Result;
    }

    links {
        sonicd::Error, sonicd::ErrorKind, SonicdError;
    }

    foreign_links {
        ::std::io::Error, IoError, "io error";
        ::serde_json::Error, Json, "JSON serde error";
        ::std::net::AddrParseError, AddrParseError, "error parsing inet address";
    }

    errors {
        InvalidFormat(co: String, msg: String) {
            description("invalid format error")
                display("invalid format {}: {}", co, msg)
        }
    }
}

fn exec(host: &str, port: &u16, query: sonicd::Query, rows_only: bool, silent: bool) -> Result<()> {

    let addr = try!(get_addr(host, port));

    let mut pb: ProgressBar<Stderr> = ProgressBar::new(stderr(), 100);
    pb.format("╢░░_╟");
    pb.tick_format("▀▐▄▌");
    pb.show_message = true;

    let res = {
        let fn_out = |msg: sonicd::OutputChunk| {
            println!("{}", msg.0.iter().fold(String::new(), |acc, x| {
                format!("{}{:?}\t",acc, x)
            }).trim_right());
        };

        let fn_meta = |msg: sonicd::TypeMetadata| {
            info!("received type metadata: {:?}", msg);
            if !rows_only {
                println!("{}", msg.0.iter().fold(String::new(), |acc, col| {
                    format!("{}{:?}\t", acc, col.0)
                }).trim_right());
            }
        };

        let fn_prog = |msg: sonicd::QueryProgress| {
            if !silent {
                let sonicd::QueryProgress { total, progress, status, ..} = msg;

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

        sonicd::stream(query, addr, fn_out, fn_prog, fn_meta)
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

    let Args { arg_file, arg_query, flag_silent, flag_rows_only,
    arg_var, flag_file, flag_c, arg_source,
    cmd_login, flag_execute, flag_version, .. } = args;

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

    } else if flag_version{

        println!("Sonic CLI version {} ({})",
        VERSION,
        COMMIT.unwrap_or_else(|| "dev"));

        Ok(())

    } else {

        panic!("unexpected args");

    }
}

fn main() {

    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());

    env_logger::init().unwrap();

    debug!("parsed args {:?}", args);

    match _main(args) {
        Ok(_) => {},
        Err(Error(ErrorKind::SonicdError(sonicd::ErrorKind::QueryError(msg)), _)) =>  {
            stderr()
                .write(&format!("\n{} {}",
                                Red.paint("error:"), msg).as_bytes()).unwrap();
            stderr().flush().unwrap();
            process::exit(1)
        },
        Err(Error(kind, stacktrace)) =>  {
            stderr()
                .write(&format!("\n{} {}\n{:?}",
                                Red.paint("error:"), kind, stacktrace).as_bytes()).unwrap();
            stderr().flush().unwrap();
            process::exit(1)
        }
    }
}
