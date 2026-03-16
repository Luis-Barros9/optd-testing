//! The logical join operator joins two input relations based on a join
//! condition.

use crate::ir::{
    Column, IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - join_type: The type of join (e.g., Inner, Left, Mark, Single).
    /// Scalars:
    /// - join_cond: The join conditions to join on
    LogicalJoin, LogicalJoinBorrowed {
        properties: OperatorProperties,
        metadata: LogicalJoinMetadata {
            join_type: JoinType,
        },
        inputs: {
            operators: [outer, inner],
            scalars: [join_cond],
        }
    }
);
impl_operator_conversion!(LogicalJoin, LogicalJoinBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Single,
    Mark(Column),
}

impl JoinType {
    pub fn get_metadata_string(&self) -> String {
        match self {
            JoinType::Inner => "Inner".to_string(),
            JoinType::Left => "Left".to_string(),
            JoinType::Single => "Single".to_string(),
            JoinType::Mark(col) => format!("Mark({})", col.0),
        }
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        match metadata {
            "Inner" => Some(JoinType::Inner),
            "Left" => Some(JoinType::Left),
            "Single" => Some(JoinType::Single),
            _ => {
                let value = metadata
                    .strip_prefix("Mark(")
                    .and_then(|m| m.strip_suffix(")"))?
                    .trim()
                    .parse::<usize>()
                    .ok()?;
                Some(JoinType::Mark(Column(value)))
            }
        }
    }
}

impl LogicalJoinMetadata {
    pub fn get_metadata_string(&self) -> String {
        format!("{{ join_type: {} }}", self.join_type.get_metadata_string())
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Some(Self {
                join_type: JoinType::Inner,
            });
        }

        let payload = metadata
            .strip_prefix("{ ")
            .and_then(|m| m.strip_suffix(" }"))?;
        let join = payload.strip_prefix("join_type: ")?.trim();
        Some(Self {
            join_type: JoinType::from_metadata_string(join)?,
        })
    }
}

impl LogicalJoin {
    pub fn new(
        join_type: JoinType,
        outer: Arc<Operator>,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
    ) -> Self {
        Self {
            meta: LogicalJoinMetadata { join_type },
            common: IRCommon::new(Arc::new([outer, inner]), Arc::new([join_cond])),
        }
    }
}

impl Explain for LogicalJoinBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(3);
        fields.push((".join_type", Pretty::debug(self.join_type())));
        fields.push((".join_cond", self.join_cond().explain(ctx, option)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("LogicalJoin", fields, children)
    }
}
