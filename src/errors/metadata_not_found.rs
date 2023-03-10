use std::error::Error;

#[derive(Debug)]
pub struct MetadataNotFound(pub String);

impl Error for MetadataNotFound {}

impl std::fmt::Display for MetadataNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
