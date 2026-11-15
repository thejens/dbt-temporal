//! Which artifact-store failures no retry can fix.
//!
//! The store backends return `anyhow::Error`s whose chain still carries the
//! typed error each layer produced (`std::io::Error` for the local filesystem,
//! `object_store::Error` for GCS and S3). Classification reads those types, not
//! message text: a provider can reword a message between releases, but a missing
//! bucket stays `NotFound`.

use std::error::Error;
use std::fmt;
use std::io::ErrorKind;

/// A reference or path the store refuses outright: it escapes the store's root,
/// names the root itself, or falls outside the configured prefix.
///
/// Typed so the refusal classifies as permanent without anyone inspecting its
/// message; the same reference fails the same way on every attempt.
#[derive(Debug)]
pub struct InvalidArtifactPath(pub String);

impl fmt::Display for InvalidArtifactPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for InvalidArtifactPath {}

/// Whether retrying this failure can never succeed.
///
/// That is the case when the object or bucket does not exist, the credentials
/// lack access, or the path or configuration is invalid. Everything else — dropped connections, throttling, 5xx responses —
/// is treated as transient.
pub fn is_permanent(err: &anyhow::Error) -> bool {
    err.chain().any(cause_is_permanent)
}

fn cause_is_permanent(cause: &(dyn Error + 'static)) -> bool {
    if cause.is::<InvalidArtifactPath>() {
        return true;
    }
    if let Some(io) = cause.downcast_ref::<std::io::Error>() {
        return io_kind_is_permanent(io.kind());
    }
    #[cfg(any(feature = "gcs", feature = "aws"))]
    if let Some(store) = cause.downcast_ref::<object_store::Error>() {
        return object_store_error_is_permanent(store);
    }
    false
}

/// Kinds that describe the request or the environment's permissions, as opposed
/// to a transient condition of the device or network.
const fn io_kind_is_permanent(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::NotFound
            | ErrorKind::PermissionDenied
            | ErrorKind::InvalidInput
            | ErrorKind::InvalidData
            | ErrorKind::InvalidFilename
            | ErrorKind::Unsupported
            | ErrorKind::NotADirectory
            | ErrorKind::IsADirectory
            | ErrorKind::ReadOnlyFilesystem
    )
}

/// `object_store::Error` is `#[non_exhaustive]`, so permanence is an allow-list:
/// a variant added later is retried until someone classifies it, which costs
/// attempts but never fails a run that would have recovered.
///
/// A GCS or S3 request against a bucket that does not exist reports `NotFound`;
/// refused credentials report `PermissionDenied` or `Unauthenticated`.
#[cfg(any(feature = "gcs", feature = "aws"))]
const fn object_store_error_is_permanent(err: &object_store::Error) -> bool {
    use object_store::Error;
    matches!(
        err,
        Error::NotFound { .. }
            | Error::PermissionDenied { .. }
            | Error::Unauthenticated { .. }
            | Error::InvalidPath { .. }
            | Error::NotSupported { .. }
            | Error::NotImplemented { .. }
            | Error::UnknownConfigurationKey { .. }
    )
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use anyhow::Context as _;

    fn io(kind: ErrorKind) -> anyhow::Error {
        anyhow::Error::new(std::io::Error::new(kind, "boom")).context("writing artifact")
    }

    #[test]
    fn a_missing_or_forbidden_path_is_permanent() {
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::InvalidInput,
        ] {
            assert!(is_permanent(&io(kind)), "{kind:?}");
        }
    }

    #[test]
    fn a_dropped_connection_or_timeout_is_transient() {
        for kind in [
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionRefused,
            ErrorKind::TimedOut,
            ErrorKind::Interrupted,
            ErrorKind::StorageFull,
        ] {
            assert!(!is_permanent(&io(kind)), "{kind:?}");
        }
    }

    #[test]
    fn an_error_without_a_typed_cause_is_transient() {
        assert!(!is_permanent(&anyhow::anyhow!("something went wrong")));
    }

    #[test]
    fn a_refused_path_is_permanent_through_any_context() {
        let err = anyhow::Error::new(InvalidArtifactPath("escapes the store".into()))
            .context("resolving artifact")
            .context("collecting an earlier segment");
        assert!(is_permanent(&err));
    }

    #[test]
    fn the_typed_cause_wins_over_the_wording_around_it() {
        let err = anyhow::Error::new(std::io::Error::from(ErrorKind::ConnectionReset))
            .context("permission denied while writing");
        assert!(!is_permanent(&err));
    }

    #[test]
    fn a_context_over_a_local_not_found_stays_permanent() {
        let missing = std::fs::read("/nonexistent/dbt-temporal/artifact");
        let err = missing
            .context("reading artifact")
            .expect_err("the path does not exist");
        assert!(is_permanent(&err));
    }

    #[cfg(any(feature = "gcs", feature = "aws"))]
    mod object_store_errors {
        use super::*;
        use object_store::Error;

        fn source() -> Box<dyn std::error::Error + Send + Sync> {
            "status 403".into()
        }

        fn wrapped(err: Error) -> anyhow::Error {
            anyhow::Error::new(err).context("writing artifact: runs/inv-1/manifest.json")
        }

        #[test]
        fn missing_bucket_and_refused_credentials_are_permanent() {
            let permanent = [
                Error::NotFound {
                    path: "p".into(),
                    source: source(),
                },
                Error::PermissionDenied {
                    path: "p".into(),
                    source: source(),
                },
                Error::Unauthenticated {
                    path: "p".into(),
                    source: source(),
                },
                Error::NotSupported { source: source() },
                Error::NotImplemented {
                    operation: "put".into(),
                    implementer: "x".into(),
                },
                Error::UnknownConfigurationKey {
                    store: "S3",
                    key: "k".into(),
                },
            ];
            for err in permanent {
                let label = err.to_string();
                assert!(is_permanent(&wrapped(err)), "{label}");
            }
        }

        #[test]
        fn service_and_network_failures_are_transient() {
            let transient = [
                Error::Generic {
                    store: "GCS",
                    source: "connection reset".into(),
                },
                Error::Precondition {
                    path: "p".into(),
                    source: source(),
                },
            ];
            for err in transient {
                let label = err.to_string();
                assert!(!is_permanent(&wrapped(err)), "{label}");
            }
        }

        #[tokio::test]
        async fn reading_a_missing_key_from_a_store_is_permanent() {
            use object_store::ObjectStoreExt as _;
            let store = object_store::memory::InMemory::new();
            let err = store
                .get(&object_store::path::Path::from("absent"))
                .await
                .map(|_| ())
                .context("reading artifact: absent")
                .expect_err("nothing was stored");
            assert!(is_permanent(&err));
        }
    }
}
