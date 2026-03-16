//! Represents a SQL LIKE scalar operation.

use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - negated: Whether the LIKE operation is negated (NOT LIKE).
    /// - escape_char: Optional escape character for the LIKE pattern.
    /// - case_insensative: Whether the LIKE operation is case-insensitive (ILIKE).
    /// Scalars:
    /// - expr: The expression to be matched.
    /// - pattern: The pattern to match against.
    Like, LikeBorrowed {
        properties: ScalarProperties,
        metadata: LikeMetadata {
            negated: bool,
            escape_char: Option<char>,
            case_insensative: bool,
        },
        inputs: {
            operators: [],
            scalars: [expr, pattern],
        }
    }
);
impl_scalar_conversion!(Like, LikeBorrowed);

impl Like {
    pub fn new(
        expr: Arc<Scalar>,
        pattern: Arc<Scalar>,
        negated: bool,
        case_insensative: bool,
        escape_char: Option<char>,
    ) -> Self {
        Self {
            meta: LikeMetadata {
                negated,
                escape_char,
                case_insensative,
            },
            common: IRCommon::with_input_scalars_only(Arc::new([expr, pattern])),
        }
    }
}

impl LikeMetadata {
    pub fn get_metadata_string(&self) -> String {
        let escape_char = self
            .escape_char
            .map(|c| c.to_string())
            .unwrap_or_else(|| "null".to_string());

        format!(
            "{{ negated: {}, escape_char: {}, case_insensative: {} }}",
            self.negated, self.escape_char.map(|c| c.to_string()).unwrap_or(escape_char), self.case_insensative
        )
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Some(Self {
                negated: false,
                escape_char: None,
                case_insensative: false,
            });
        }

        let payload = metadata
            .strip_prefix("{ ")
            .and_then(|m| m.strip_suffix(" }"))?;

        let mut negated = None;
        let mut escape_char = None;
        let mut case_insensative = None;

        for part in payload.split(", ") {
            if let Some(v) = part.strip_prefix("negated: ") {
                negated = v.parse::<bool>().ok();
            } else if let Some(v) = part.strip_prefix("escape_char: ") {
                escape_char = if v == "null" {
                    Some(None)
                } else {
                    v.chars().next().map(Some)
                };
            } else if let Some(v) = part.strip_prefix("case_insensative: ") {
                case_insensative = v.parse::<bool>().ok();
            }
        }

        Some(Self {
            negated: negated?,
            escape_char: escape_char?,
            case_insensative: case_insensative?,
        })
    }
}

impl Explain for LikeBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let maybe_negation = if *self.negated() { "NOT " } else { "" };
        let like_type = if *self.case_insensative() {
            "ILIKE"
        } else {
            "LIKE"
        };
        let maybe_escape = if let Some(c) = self.escape_char() {
            format!(" ESCAPE {c}")
        } else {
            "".to_string()
        };
        let fmt = format!(
            "{} {maybe_negation}{like_type} {}{maybe_escape}",
            self.expr().explain(ctx, option).to_one_line_string(true),
            self.pattern().explain(ctx, option).to_one_line_string(true),
        );
        Pretty::display(&fmt)
    }
}
