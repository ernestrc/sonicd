error_chain! {
    types {
        Error, ErrorKind, ChainErr, Result;
    }

    links {
        ::sonicd::Error, ::sonicd::ErrorKind, SonicdError;
    }

    foreign_links {
        ::nix::Error, NixError, "nix error";
        ::std::io::Error, IoError, "io error";
    }

    errors {
        UnexpectedState {
            description("Unexpected handler state")
                display("unexpected handler state")
        }
        Bufferoverflow {
            description("Buffer overflow")
                display("buffer overflow error")
        }
    }
}
