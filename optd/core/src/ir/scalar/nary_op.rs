//! N-ary scalar operations like AND, OR.

use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use itertools::Itertools;
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - op_kind: The kind of N-ary operation (And, Or).
    /// Scalars:
    /// - terms: The input scalar expressions.
    NaryOp, NaryOpBorrowed {
        properties: ScalarProperties,
        metadata: NaryOpMetadata {
            op_kind: NaryOpKind,
        },
        inputs: {
            operators: [],
            scalars: terms[],
        }
    }
);
impl_scalar_conversion!(NaryOp, NaryOpBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NaryOpKind {
    And,
    Or,
}

impl std::fmt::Display for NaryOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            NaryOpKind::And => "AND",
            NaryOpKind::Or => "OR",
        };
        write!(f, "{s}")
    }
}

impl NaryOp {
    pub fn new(op_kind: NaryOpKind, terms: Arc<[Arc<Scalar>]>) -> Self {
        Self {
            meta: NaryOpMetadata { op_kind },
            common: IRCommon::with_input_scalars_only(terms),
        }
    }
}

impl NaryOpMetadata {
    pub fn get_metadata_string(&self) -> String {
        format!("{{ op_kind: {} }}", self.op_kind)
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Some(Self {
                op_kind: NaryOpKind::And,
            });
        }

        let payload = metadata
            .strip_prefix("{ ")
            .and_then(|m| m.strip_suffix(" }"))?;
        let value = payload.strip_prefix("op_kind: ")?.trim();
        let op_kind = match value {
            "AND" => NaryOpKind::And,
            "OR" => NaryOpKind::Or,
            _ => return None,
        };
        Some(Self { op_kind })
    }
}

impl NaryOpBorrowed<'_> {
    pub fn is_and(&self) -> bool {
        matches!(self.op_kind(), NaryOpKind::And)
    }

    pub fn is_or(&self) -> bool {
        matches!(self.op_kind(), NaryOpKind::Or)
    }
}

impl Explain for NaryOpBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let explained_terms = self
            .terms()
            .iter()
            .map(|t| format!("({})", t.explain(ctx, option).to_one_line_string(true)))
            .join(&format!(" {} ", self.op_kind()));

        Pretty::display(&explained_terms)
    }
}
