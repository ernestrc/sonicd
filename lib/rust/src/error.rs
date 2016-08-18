
error_chain! {
    types {
        Error, ErrorKind, ChainErr, Result;
    }

    links { }

    foreign_links {
        ::serde_json::Error, Json;
        ::std::io::Error, IoError;
        ::std::net::AddrParseError, ParseAddr;
        ::std::string::FromUtf8Error, Utf8Error;
        ::nix::Error, NixError;
    }

    errors {
        BigEndianError(e: ::std::io::Error) {
            description("BigEndian error")
                display("{}", e)
        }

        QueryError(msg: String) {
            description("error in query")
                display("{}", msg)
        }

        Proto(msg: String) {
            description("Sonicd protocol error")
                display("Sonicd protocol error: {}: lib is v.{}", msg, super::VERSION)
        }
    }
}
