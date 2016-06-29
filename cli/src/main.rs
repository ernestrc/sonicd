#![feature(custom_derive, plugin, custom_attribute, box_syntax)]
#![plugin(docopt_macros, serde_macros)]
extern crate serde_json;
extern crate serde;
extern crate pbr;
extern crate regex;
extern crate docopt;
extern crate rustc_serialize;
extern crate env_logger;
#[macro_use] extern crate log;
#[macro_use] extern crate sonicd;

mod util;

use std::path::PathBuf;
use pbr::ProgressBar;
use sonicd::{SonicMessage, Query, Result, version, stream};
use util::*;
use std::process;
use std::io::{Write, self};
use serde_json::Value;

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

fn run(host: &str, port: &u16, query: Query, rows_only: bool, silent: bool) -> Result<()> {

    let mut pb = ProgressBar::new(100);
    pb.format("╢░░_╟");

    let fn_out = |msg: SonicMessage| {
        match msg.p {
            Some(Value::Array(d)) => {
                println!("{}", d.iter().fold(String::new(), |acc, x| {
                    format!("{}{:?}\t",acc, x)
                }).trim_right());
            }
            e => panic!("protocol error: expected array in output payload but found {:?}", e)
        }
    };

    let fn_meta = |msg: SonicMessage| {
        if !rows_only {
            match msg.p {
                Some(Value::Array(d)) => {
                    debug!("recv type metadata: {:?}", d);
                    println!("{}", d.iter().fold(String::new(), |acc, col| {
                        format!("{}{:?}\t", acc, col.as_array().unwrap()[0])
                    }).trim_right())
                },
                e => panic!("protocol error: expected json array in metadata payload but found {:?}", e)
            }
        }
    };

    let fn_prog = |msg: SonicMessage| {
        if !silent {
            let fields = msg.p.unwrap();
            fields.find("progress").and_then(|p| {
                p.as_f64().map(|pi| {
                    if pi >= 99.0 {
                        pb.finish();
                    } else if pi >= 1.0 {
                        pb.add(pi as u64);
                    }
                })
            });
            fields.find("output")
                .and_then(|o| o.as_string().map(|os| {
                    io::stderr().write(&format!("{}\r", os).as_bytes()).unwrap();
                }));
        }
    };

    try!(stream(query.into_msg(), host, port, fn_out, fn_prog, fn_meta));

    Ok(())

}

fn _main(args: Args) -> Result<()> {

    let Args { arg_file, arg_query, flag_silent, flag_rows_only,
    arg_var, flag_file, flag_c, arg_source,
    cmd_login, flag_execute, flag_version, .. } = args;

    let ClientConfig { sonicd, http_port, tcp_port, sources, auth } = if flag_c != "" {
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

        run(&sonicd, &tcp_port, query, flag_rows_only, flag_silent)

    } else if flag_execute {

        let query = try!(build(arg_source, sources, auth, arg_query));

        run(&sonicd, &tcp_port, query, flag_rows_only, flag_silent)

    } else if cmd_login {

        login(&sonicd, &tcp_port)

    } else if flag_version{

        let server_v = try!(version(&sonicd, &http_port));
        println!("sonic cli version {} ({}); server version {}",
        VERSION,
        COMMIT.unwrap_or_else(|| "dev"),
        server_v);

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
        Err(r) =>  {
            io::stderr().write(&format!("{}", r).as_bytes()).unwrap();
            process::exit(1)
        }
    }
}
