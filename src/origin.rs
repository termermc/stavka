use std::collections::HashMap;
use http::Uri;
use http::uri::{Authority, Scheme};

pub struct OriginManager {
    /// Key: normalized DNS hostname
    /// Value: URL-formatted IP (e.g. `192.168.1.1`, `[::1]`)
    host_to_origin_host: HashMap<String, String>
}

impl OriginManager {
    pub fn new() -> OriginManager {
        OriginManager {
            host_to_origin_host: HashMap::new()
        }
    }

    /// Resolves the appropriate origin URI for the specified URI.
    /// If there is no origin for the URI, returns [None].
    /// The hostname must not contain a port number.
    pub fn uri_to_origin_uri<T: Into<Uri>>(&self, uri: T, hostname: &str) -> Option<Uri> {
        let mut uri = uri.into();
        let host = uri.host().unwrap_or(hostname);

        let origin_host = self.host_to_origin_host.get(host)?;

        let mut builder = Uri::builder().
            scheme(Scheme::HTTPS).
            authority(origin_host.to_owned());

        if let Some(path_and_query) = uri.path_and_query() {
            builder = builder.path_and_query(path_and_query.to_owned());
        }

        Some(builder.build().expect("URI built from inputted URI should have been valid"))
    }

    /// Sets the origin host for the specified host.
    pub fn set_origin_host(&mut self, host: String, origin_host: String) {
        self.host_to_origin_host.insert(host, origin_host);
    }
}
