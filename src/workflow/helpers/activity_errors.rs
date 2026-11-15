/// The full message of an activity error, including the activity's own reason.
///
/// The SDK's `Display` prints only the top-level failure — for a failed
/// activity that is the generic "Activity task failed". What the activity
/// actually returned ("no nodes matched after applying selectors", a
/// warehouse error) travels in the failure's `cause` chain, which neither
/// `Display` nor `{:#}` reaches. Without it a rejected run tells the operator
/// nothing and sends them to the worker logs.
pub fn activity_error_message(e: &temporalio_sdk::ActivityExecutionError) -> String {
    let mut message = e.to_string();
    if let Some(failure) = e.failure() {
        for cause in failure_cause_messages(failure) {
            message.push_str(": ");
            message.push_str(cause);
        }
    }
    message
}

/// The messages along a failure's `cause` chain, outermost first, skipping
/// empty ones and any that repeat the message before them.
pub fn failure_cause_messages(
    failure: &temporalio_common::protos::temporal::api::failure::v1::Failure,
) -> Vec<&str> {
    let mut messages: Vec<&str> = Vec::new();
    let mut previous = failure.message.as_str();
    let mut next = failure.cause.as_deref();
    while let Some(cause) = next {
        let message = cause.message.as_str();
        if !message.is_empty() && message != previous {
            messages.push(message);
        }
        previous = message;
        next = cause.cause.as_deref();
    }
    messages
}

/// Extract a concise error message from a Temporal activity execution error.
pub fn short_activity_error(e: &temporalio_sdk::ActivityExecutionError) -> String {
    use crate::error::truncate_at_char_boundary;
    let full = activity_error_message(e);

    // Truncate if very long.
    if full.len() > 200 {
        format!("{}...", truncate_at_char_boundary(&full, 200))
    } else {
        full
    }
}

#[cfg(test)]
// float_cmp: tests compare directly-assigned floats, not computed values.
// unwrap_used: unwrap is acceptable in tests for brevity.
#[allow(
    clippy::float_cmp,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_wrap
)]
mod tests {
    use super::*;
    use crate::error::truncate_at_char_boundary;

    // --- failure_cause_messages ---

    fn failure(
        message: &str,
        cause: Option<temporalio_common::protos::temporal::api::failure::v1::Failure>,
    ) -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        temporalio_common::protos::temporal::api::failure::v1::Failure {
            message: message.to_string(),
            cause: cause.map(Box::new),
            ..Default::default()
        }
    }

    #[test]
    fn failure_cause_messages_walks_the_chain_outermost_first() {
        let chain = failure(
            "Activity task failed",
            Some(failure(
                "no nodes matched after applying selectors",
                Some(failure("selector: no_such_model", None)),
            )),
        );
        assert_eq!(
            failure_cause_messages(&chain),
            vec![
                "no nodes matched after applying selectors",
                "selector: no_such_model"
            ]
        );
    }

    #[test]
    fn failure_cause_messages_skips_empty_and_repeated_messages() {
        let chain = failure(
            "Activity task failed",
            Some(failure(
                "Activity task failed",
                Some(failure("", Some(failure("permission denied", None)))),
            )),
        );
        assert_eq!(failure_cause_messages(&chain), vec!["permission denied"]);
    }

    #[test]
    fn failure_cause_messages_is_empty_without_a_cause() {
        assert_eq!(
            failure_cause_messages(&failure("Activity task failed", None)),
            Vec::<&str>::new()
        );
    }

    // --- short_activity_error ---

    fn make_serialization_error(message: &str) -> temporalio_sdk::ActivityExecutionError {
        use temporalio_common::data_converters::PayloadConversionError;
        temporalio_sdk::ActivityExecutionError::Serialization(
            PayloadConversionError::EncodingError(Box::new(std::io::Error::other(
                message.to_string(),
            ))),
        )
    }

    #[test]
    fn short_activity_error_passes_short_messages_through() {
        let err = make_serialization_error("relation does not exist");
        let short = short_activity_error(&err);
        // Display: "Payload conversion failed: Encoding error: <message>"
        assert_eq!(short, "Payload conversion failed: Encoding error: relation does not exist");
    }

    #[test]
    fn short_activity_error_truncates_long_messages_to_200_plus_ellipsis() {
        // Full display is "Payload conversion failed: Encoding error: " + message.
        // Use a message long enough that the full string exceeds 200 chars.
        let long_msg: String = "A".repeat(250);
        let err = make_serialization_error(&long_msg);
        let short = short_activity_error(&err);
        assert!(short.ends_with("..."), "got: {short}");
        assert_eq!(short.len(), 203);
    }

    #[test]
    fn short_activity_error_does_not_truncate_at_exactly_200() {
        // "Payload conversion failed: Encoding error: " is 43 chars.
        // Build message so full Display is exactly 200 chars — no truncation.
        const PREFIX: &str = "Payload conversion failed: Encoding error: ";
        let msg = "B".repeat(200 - PREFIX.len());
        let err = make_serialization_error(&msg);
        let short = short_activity_error(&err);
        assert_eq!(short.len(), 200);
        assert!(!short.ends_with("..."));
    }

    #[test]
    fn short_activity_error_truncates_multibyte_on_char_boundary() {
        // Byte 200 of the full display lands mid-codepoint here; the slice
        // must back up to a boundary instead of panicking.
        let msg = "é".repeat(150);
        let err = make_serialization_error(&msg);
        let short = short_activity_error(&err);
        assert!(short.ends_with("..."), "got: {short}");
        assert!(short.len() <= 203);
    }

    // --- truncate_at_char_boundary ---

    #[test]
    fn truncate_at_char_boundary_backs_up_to_boundary() {
        // 'a' is 1 byte, 'é' is 2 — a 2-byte cut lands mid-'é'.
        assert_eq!(truncate_at_char_boundary("aé", 2), "a");
        assert_eq!(truncate_at_char_boundary("aé", 3), "aé");
        assert_eq!(truncate_at_char_boundary("aé", 10), "aé");
        assert_eq!(truncate_at_char_boundary("", 5), "");
    }
}
