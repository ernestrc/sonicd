
error_chain! {
    types {
        Error, ErrorKind, ChainErr, Result;
    }

    links { }

    foreign_links {
        ::serde_json::Error, Json, "JSON serde error";
        ::std::io::Error, IoError, "io error";
        ::std::net::AddrParseError, ParseAddr, "address parse error";
        ::std::string::FromUtf8Error, Utf8Error, "error decoding utf8";
        ::nix::Error, NixError, "nix error";
    }

    errors {

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
