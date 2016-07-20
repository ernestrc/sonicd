use std::fmt::Debug;

use serde_json::Value;

use error::Result;

pub trait Source 
where Self: Debug + Handler {
}

#[derive(Debug)]
pub struct SyntheticSource;

impl Source for SyntheticSource {}
