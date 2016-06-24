use libsonicd::{Query, ClientConfig, Result, Error, authenticate};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::process::Command;
use std::fs::OpenOptions;
use std::str::FromStr;
use std::string::ToString;
use std::env;
use std::path::PathBuf;
use serde_json::Value;
use regex::Regex;
use std::collections::BTreeMap;

static EDITOR: &'static str = "vim";

fn write_config(config: &ClientConfig, path: &PathBuf) -> Result<()> {
    debug!("overwriting or creating new configuration file with {:?}",
           config);
    match OpenOptions::new().truncate(true).create(true).write(true).open(path) {
        Ok(mut f) => {
            let encoded = ::serde_json::to_string_pretty(config)
                .map_err(|e| {
                    format!("error when encoding JSON to config file: {}", e)
                })
            .unwrap();
            f.write_all(encoded.as_bytes())
                .map_err(|e| format!("error when writing to config file: {}", e))
                .unwrap();
            debug!("write success to config file {:?}", path);
            Ok(())
        }
        Err(e) => Err(Error::OtherError(e.to_string())),
    }
}

fn get_config_path() -> PathBuf {
    let mut sonicrc = env::home_dir().expect("can't find your home folder");
    sonicrc.push(".sonicrc");
    sonicrc
}

fn run_cmd_file(mut cmd: Command, path: &PathBuf) -> String {
    cmd.arg(path.to_str().unwrap()).status().unwrap();
    let mut f = OpenOptions::new().open(path).unwrap();
    let mut body = String::new();
    f.read_to_string(&mut body).unwrap();
    body
}

pub fn read_file_contents(path: &PathBuf) -> Result<String> {

    let mut file = try!(File::open(&path).map_err(|e| {
        Error::OtherError(format!("could not open file in '{:?}': {}", &path, e))
    }));
    let mut contents = String::new();
    try!(file.read_to_string(&mut contents)
         .map_err(|e| Error::OtherError(format!("could not read file in '{:?}': {}", &path, e))));

    return Ok(contents);
}

pub fn read_config(path: &PathBuf) -> Result<ClientConfig> {

    let contents = try!(read_file_contents(&path));

    ::serde_json::from_str::<ClientConfig>(&contents.to_string())
        .map_err(|e| Error::OtherError(format!("Could not deserialize config file: {}", e)))
}


/// Sources .sonicrc user config or creates new and prompts user to edit it
pub fn get_default_config() -> Result<ClientConfig> {

    let path = get_config_path();

    debug!("trying to get configuration in path {:?}", path);

    match fs::metadata(&path) {
        Ok(ref cfg_attr) if cfg_attr.is_file() => {
            debug!("found a file in {:?}", path);
            read_config(&path)
        }
        _ => {
            let mut stdout = io::stdout();
            stdout.write(b"It looks like it's the first time you're using the sonic CLI. Let's configure a few things. Press any key to continue").unwrap();
            stdout.flush().unwrap();
            let mut input = String::new();
            match io::stdin().read_line(&mut input) {
                Ok(_) => {
                    write_config(&ClientConfig::empty(), &path).unwrap();
                    let c: ClientConfig =
                        ::serde_json::from_str(&run_cmd_file(Command::new(EDITOR), &path)).unwrap();
                    Ok(c)
                }
                Err(error) => Err(Error::OtherError(error.to_string())),
            }
        }
    }
}


/// Splits strings by character '='
///
/// # Examples
/// ```
/// use libsonic::util::split_key_value;
///
/// let vars = vec!["TABLE=account".to_string(), "DATECUT=2015-09-13".to_string()];
/// let result = split_key_value(&vars).unwrap();
/// 
/// assert_eq!(result[0].0, "TABLE");
/// assert_eq!(result[0].1, "account");
/// assert_eq!(result[1].0, "DATECUT");
/// assert_eq!(result[1].1, "2015-09-13");
/// ```
/// 
/// It returns an error if string doesn't contain '='.
///
/// # Failures
/// ```
/// use libsonic::util::split_key_value;
///
/// let vars = vec!["key val".to_string()];
/// split_key_value(&vars);
/// ```
pub fn split_key_value(vars: &Vec<String>) -> Result<Vec<(String, String)>> {
    debug!("parsing variables {:?}", vars);
    let mut m: Vec<(String, String)> = Vec::new();
    for var in vars.iter() {
        if var.contains("=") {
            let mut split = var.split("=");
            m.push((split.next().unwrap().to_string(),
            split.next().unwrap().to_string()));
        } else {
            return Err(Error::OtherError(format!("Cannot split {}. It should follow format \
                                               'key=value'",
                                               var)));
        }
    }
    debug!("Successfully parsed parsed variables {:?} into {:?}",
           vars,
           &m);
    return Ok(m);
}


/// Attempts to inject all variables to the given template:
///
/// # Examples
/// ```
/// use libsonic::util::inject_vars;
///
/// let query = "select count(*) from ${TABLE} where dt > '${DATECUT}' and dt <= \
///     date_sub('${DATECUT}', 30);".to_string();
///
/// let vars = vec![("TABLE".to_string(), "accounts".to_string()),
///     ("DATECUT".to_string(), "2015-01-02".to_string())];
/// 
/// assert_eq!(inject_vars(&query, &vars).unwrap(),
///     "select count(*) from accounts where dt > '2015-01-02' and dt <= \
///     date_sub('2015-01-02', 30);".to_string());
///
/// ```
///
/// It will return an Error if there is a discrepancy between variables and template
///
/// # Failures
/// ```
/// use libsonic::util::inject_vars;
///
/// let query = "select count(*) from hamburgers".to_string();
/// let vars = vec![("TABLE".to_string(), "accounts".to_string())];
/// inject_vars(&query, &vars);
/// 
/// let query = "select count(*) from ${TABLE} where ${POTATOES}".to_string();
/// let vars = vec![("TABLE".to_string(), "accounts".to_string())];
/// inject_vars(&query, &vars);
/// 
/// ```
pub fn inject_vars(template: &str, vars: &Vec<(String, String)>) -> Result<String> {
    debug!("injecting variables {:?} into '{:?}'", vars, template);
    let mut q = String::from_str(template).unwrap();
    for var in vars.iter() {
        let k = "${".to_string() + &var.0 + "}";
        if !q.contains(&k) {
            return Err(Error::OtherError(format!("{} not found in template", k)));
        } else {
            q = q.replace(&k, &var.1);
        }
    }

    debug!("injected all variables: '{:?}'", &q);

    // check if some variables were left un-injected
    let re = Regex::new(r"(\$\{.*\})").unwrap();
    if re.is_match(&q) {
        Err(Error::OtherError("Some variables remain uninjected".to_string()))
    } else {
        Ok(q)
    }
}

pub fn build_query(src_alias: &str, mut srcfg: BTreeMap<String, Value>, query: &str, auth: Option<String>) -> Result<Query> {

    let source_config = srcfg.remove(src_alias);

    let config: Value = try!(match source_config {
        Some(o@Value::Object(_)) => Ok(o),
        None => Ok(Value::String(src_alias.to_owned())),
        _ => Err(Error::OtherError(format!("source '{}' config is not an object", &src_alias))),
    });

    Ok(Query {
        id: None,
        trace_id: None,
        auth: auth,
        query: query.to_owned(),
        config: config,
    })
}

pub fn login(host: &str, tcp_port: &u16) -> Result<()> {

    let user = try!(::std::env::var("USER").map_err(|e| {
        Error::OtherError(format!("could not get current user: {}", e.to_string()))
    }));

    try!(io::stdout().write(b"Api Key: ")
         .map_err(|e| Error::OtherError(e.to_string())));

    io::stdout().flush().unwrap();

    let mut key = String::new();

    match io::stdin().read_line(&mut key) {
                Ok(_) => {
                    let token = try!(authenticate(user, key.trim().to_owned(), host, tcp_port));
                    let path = get_config_path();
                    let config = try!(read_config(&path));

                    let new_config = ClientConfig { auth: Some(token), ..config };
                    try!(write_config(&new_config, &path));

                    println!("OK");
                    Ok(())
                },
                Err(e) => Err(Error::OtherError(e.to_string())),
    }
}
