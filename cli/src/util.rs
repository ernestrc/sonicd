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
use std::net::SocketAddr;
use std::fmt::Display;
use super::{Result, ErrorKind};
use super::ChainErr;

static DEFAULT_EDITOR: &'static str = "vim";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientConfig {
    pub sonicd: String,
    pub tcp_port: u16,
    pub sources: BTreeMap<String, Value>,
    pub auth: Option<String>
}

impl ClientConfig {
    pub fn empty() -> ClientConfig {
        ClientConfig {
            sonicd: "0.0.0.0".to_string(),
            tcp_port: 10001,
            sources: BTreeMap::new(),
            auth: None
        }
    }
}


fn parse_addr<T: Display>(addr: T, port: &u16) -> Result<SocketAddr> {
    let addr = try!(format!("{}:{}", addr, port).parse::<SocketAddr>());
    Ok(addr)
}

pub fn get_addr(addr: &str, port: &u16) -> Result<SocketAddr> {
    match parse_addr(addr, port) {
        Ok(addr) => Ok(addr),
        Err(_) => {
            let mut lookup = try!(::std::net::lookup_host(&addr));
            match lookup.next() {
                Some(lr) => {
                    let addr = try!(lr.map(|h| SocketAddr::new(h.ip(), *port)));
                    Ok(addr)
                }
                None => Err(format!("unexpected error when looking up host {}", &addr).into()),
            }
        }
    }
}


fn write_config(config: &ClientConfig, path: &PathBuf) -> Result<()> {
    debug!("overwriting or creating new configuration file with {:?}",
           config);
    let mut f = try!(OpenOptions::new().truncate(true).create(true).write(true).open(path));

    let encoded = try!(::serde_json::to_string_pretty(config));

    try!(f.write_all(encoded.as_bytes()));

    debug!("write success to config file {:?}", path);

    Ok(())
}

fn get_config_path() -> PathBuf {
    let mut sonicrc = env::home_dir().expect("can't find your home folder");
    sonicrc.push(".sonicrc");
    sonicrc
}

fn edit_file(path: &PathBuf) -> Result<String> {

    let editor = option_env!("EDITOR").unwrap_or_else(|| DEFAULT_EDITOR);

    let mut cmd = Command::new(editor);

    let path = try!(path.to_str().ok_or_else(|| format!("error checking for utf8 validity {:?}", &path)));

    try!(cmd.arg(path).status());

    let mut f = try!(OpenOptions::new().read(true).open(path));

    let mut body = String::new();

    try!(f.read_to_string(&mut body));

    Ok(body)
}

pub fn read_file_contents(path: &PathBuf) -> Result<String> {

    let mut file = try!(File::open(&path));
    let mut contents = String::new();

    try!(file.read_to_string(&mut contents));

    return Ok(contents);
}

pub fn read_config(path: &PathBuf) -> Result<ClientConfig> {

    let contents = try!(read_file_contents(&path));

    let config = try!(::serde_json::from_str::<ClientConfig>(&contents.to_string())
                      .chain_err(|| format!("config {:?} doesn't seem to be a valid JSON", path)));

    Ok(config)
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
            try!(stdout.flush());
            let mut input = String::new();
            match io::stdin().read_line(&mut input) {
                Ok(_) => {
                    try!(write_config(&ClientConfig::empty(), &path));
                    let contents = try!(edit_file(&path));
                    let c: ClientConfig =
                        try!(::serde_json::from_str(&contents));
                    println!("successfully saved configuration in $HOME/.sonicrc");
                    Ok(c)
                }
                Err(error) => Err(error.into()),
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
    for var in vars.into_iter() {
        if var.contains("=") {
            let mut split = var.split("=");
            let left = try!(split.next().ok_or_else(|| ErrorKind::InvalidFormat(var.clone(), "<key>=<value>".to_owned())));
            let right = try!(split.next().ok_or_else(|| ErrorKind::InvalidFormat(var.clone(), "<key>=<value>".to_owned())));
            m.push((left.to_owned(), right.to_owned()));
        } else {
            let err = format!("Cannot split. It should follow format 'key=value'");
            return Err(ErrorKind::InvalidFormat(var.clone(), err).into());
        }
    }
    debug!("Successfully parsed parsed variables {:?} into {:?}", vars, &m);
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
            return Err(ErrorKind::InvalidFormat(k, "not found in template".to_owned()).into());
        } else {
            q = q.replace(&k, &var.1);
        }
    }

    debug!("injected all variables: '{:?}'", &q);

    // check if some variables were left un-injected
    let re = Regex::new(r"(\$\{.*\})").unwrap();
    if re.is_match(&q) {
        Err("Some variables remain uninjected".into())
    } else {
        Ok(q)
    }
}

pub fn build(alias: String, mut sources: BTreeMap<String, Value>,
             auth: Option<String>, raw_query: String) -> Result<::sonicd::Query> {

    let clean = sources.remove(&alias);

    let source_config = match clean {
        Some(o@Value::Object(_)) => o,
        None => Value::String(alias),
        _ => {
            return Err(format!("source '{}' config is not an object", &alias).into());
        },
    };

    let query = ::sonicd::Query {
        id: None,
        trace_id: None,
        auth: auth,
        query: raw_query,
        config: source_config,
    };

    Ok(query)
}

pub fn login(host: &str, tcp_port: &u16) -> Result<()> {

    let user = option_env!("USER").unwrap_or_else(|| "unknown user");

    try!(io::stdout().write(b"Enter key: "));

    try!(io::stdout().flush());

    let mut key = String::new();

    try!(io::stdin().read_line(&mut key));

    let addr = try!(get_addr(host, tcp_port));

    let token = try!(::sonicd::authenticate(user.to_owned(), key.trim().to_owned(), addr, None));
    let path = get_config_path();
    let config = try!(read_config(&path));

    let new_config = ClientConfig { auth: Some(token), ..config };

    try!(write_config(&new_config, &path));

    println!("OK");
    Ok(())
}
