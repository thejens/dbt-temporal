/// A credential read from the environment.
///
/// [`Debug`](std::fmt::Debug) prints a fixed placeholder, so a config that is
/// logged with `{:?}` cannot leak it. The value is only reachable through
/// [`Secret::expose`], which makes every use that hands it onward visible at
/// the call site.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The credential itself, for the one place that sends it to its service.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_shows_the_value() {
        let secret = Secret::new("hunter2");
        assert_eq!(format!("{secret:?}"), "Secret(<redacted>)");
        assert_eq!(format!("{secret:#?}"), "Secret(<redacted>)");
        assert_eq!(secret.expose(), "hunter2");
    }
}
