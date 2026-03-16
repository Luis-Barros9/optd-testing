//! The hash join operator joins two input relations based on a join
//! condition, using hashing as the join strategy - as one implementation of the
//! logical join operator

use crate::ir::{
    Column, IRCommon, Operator, Scalar,
    builder::column_ref,
    convert::IntoScalar,
    macros::{define_node, impl_operator_conversion},
    operator::join::JoinType,
    properties::OperatorProperties,
    scalar::NaryOp,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - join_type: The type of join (e.g., Inner, Left).
    /// - keys: The columns from each table to hash tuples on and match
    /// Scalars:
    /// - non_equi_conds: The join conditions that are not equi-join, thus cannot
    ///                   be done using hashing
    PhysicalHashJoin, PhysicalHashJoinBorrowed {
        properties: OperatorProperties,
        metadata: PhysicalHashJoinMetadata {
            join_type: JoinType,
            keys: Arc<[(Column, Column)]>,
        },
        inputs: {
            operators: [build_side, probe_side],
            scalars: [non_equi_conds],
        }
    }
);
impl_operator_conversion!(PhysicalHashJoin, PhysicalHashJoinBorrowed);

impl PhysicalHashJoin {
    pub fn new(
        join_type: JoinType,
        build_side: Arc<Operator>,
        probe_side: Arc<Operator>,
        keys: Arc<[(Column, Column)]>,
        non_equi_conds: Arc<Scalar>,
    ) -> Self {
        Self {
            meta: PhysicalHashJoinMetadata { join_type, keys },
            common: IRCommon::new(
                Arc::new([build_side, probe_side]),
                Arc::new([non_equi_conds]),
            ),
        }
    }
}

impl PhysicalHashJoinMetadata {
    pub fn get_metadata_string(&self) -> String {
        let keys = self
            .keys
            .iter()
            .map(|(left, right)| format!("({}, {})", left.0, right.0))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "{{ join_type: {}, keys: [{}] }}",
            self.join_type.get_metadata_string(),
            keys
        )
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Some(Self {
                join_type: JoinType::Inner,
                keys: Arc::new([]),
            });
        }

        let payload = metadata
            .strip_prefix("{ ")
            .and_then(|m| m.strip_suffix(" }"))?;
        let join_part = payload
            .strip_prefix("join_type: ")?
            .split_once(", keys: ")?;

        let join_type = JoinType::from_metadata_string(join_part.0.trim())?;
        let keys_raw = join_part.1.trim().strip_prefix("[")?.strip_suffix("]")?;
        let keys = if keys_raw.trim().is_empty() {
            Vec::new()
        } else {
            let normalized = keys_raw.trim().strip_prefix("(")?.strip_suffix(")")?;
            normalized
                .split("), (")
                .map(|item| {
                    let (l, r) = item.split_once(", ")?;
                    let left = l.trim().parse::<usize>().ok()?;
                    let right = r.trim().parse::<usize>().ok()?;
                    Some((Column(left), Column(right)))
                })
                .collect::<Option<Vec<_>>>()?
        };

        Some(Self {
            join_type,
            keys: Arc::from(keys.into_boxed_slice()),
        })
    }
}

impl crate::ir::explain::Explain for PhysicalHashJoinBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(3);
        fields.push((".join_type", Pretty::debug(self.join_type())));

        let terms = self
            .keys()
            .iter()
            .map(|(l, r)| column_ref(*l).eq(column_ref(*r)))
            .chain(std::iter::once(self.non_equi_conds().clone()))
            .collect();

        let join_cond = NaryOp::new(crate::ir::scalar::NaryOpKind::And, terms).into_scalar();
        fields.push((".join_conds", join_cond.explain(ctx, option)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("PhysicalHashJoin", fields, children)
    }
}
