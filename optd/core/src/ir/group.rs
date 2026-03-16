//! Defines the Group operator, which represents an equivalence class
//! in the query optimizer.

use crate::ir::{
    IRCommon,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

/// Uniquely identifies an equivalent class in the optimizer.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub i64);

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "G{}", self.0)
    }
}

impl std::fmt::Debug for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "G{}", self.0)
    }
}

define_node!(
    /// Metadata:
    /// - tuple_ordering: The tuple ordering that this enforcer imposes.
    /// Scalars: (none)
    Group, GroupBorrowed {
        properties: OperatorProperties,
        metadata: GroupMetadata {
            group_id: GroupId,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);

impl_operator_conversion!(Group, GroupBorrowed);

impl Group {
    pub fn new(group_id: GroupId, properties: Arc<OperatorProperties>) -> Self {
        Self {
            meta: GroupMetadata { group_id },
            common: IRCommon::with_properties_only(properties),
        }
    }

    pub fn dump_to_db(&self) 
    {
        
    }

}

impl GroupMetadata {
    pub fn get_metadata_string(&self) -> String {
        format!("{{ group_id: {} }}", self.group_id.0)
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Some(Self {
                group_id: GroupId(0),
            });
        }

        let payload = metadata
            .strip_prefix("{ ")
            .and_then(|m| m.strip_suffix(" }"))?;
        let value = payload.strip_prefix("group_id: ")?.trim();
        let group_id = value.parse::<i64>().ok()?;
        Some(Self {
            group_id: GroupId(group_id),
        })
    }
}

impl Explain for GroupBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &super::IRContext,
        option: &super::explain::ExplainOption,
    ) -> Pretty<'a> {
        let mut fields = vec![(".group_id", Pretty::display(self.group_id()))];
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("Group", fields)
    }
}
