//! Operators are the core component of the IR. They represent operations
//! that can be performed on data, such as scans, joins, filters.
//!
//! While each operator has a specific structure and metadata, they all share
//! common characteristics, such as input operators and scalar expressions.
//!
//! This module defines the `Operator` struct, which encapsulates these common
//! characteristics, along with an enum `OperatorKind` that enumerates all
//! possible operator types and their associated metadata.
//!
//! When tree plans are constructed, operators are stored in the `Operator`
//! struct, with their specific type and metadata represented by the
//! `OperatorKind` enum. They can be "downcasted" (i.e. reconstructed) to their
//! specific types when needed.

mod enforcer;
mod logical;
mod physical;

use std::collections::HashSet;
use std::sync::Arc;

pub use enforcer::sort::*;
pub use logical::aggregate::*;
pub use logical::dependent_join::*;
pub use logical::get::*;
pub use logical::join::{LogicalJoin, LogicalJoinBorrowed, LogicalJoinMetadata};
pub use logical::order_by::*;
pub use logical::project::*;
pub use logical::remap::*;
pub use logical::select::*;
pub use logical::subquery::*;
pub use physical::filter::*;
pub use physical::hash_aggregate::*;
pub use physical::hash_join::*;
pub use physical::nl_join::*;
pub use physical::project::*;
pub use physical::table_scan::*;

pub mod join {
    pub use super::logical::join::JoinType;
}

pub use physical::mock_scan::*;

use crate::ir::explain::Explain;
use crate::ir::properties::OperatorProperties;
use crate::ir::{Column, Group, GroupId, GroupMetadata, IRCommon, Scalar};

/// The operator type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperatorKind {
    Group(GroupMetadata),
    MockScan(MockScanMetadata),
    LogicalGet(LogicalGetMetadata),
    LogicalJoin(LogicalJoinMetadata),
    LogicalDependentJoin(LogicalDependentJoinMetadata),
    LogicalSelect(LogicalSelectMetadata),
    LogicalProject(LogicalProjectMetadata),
    LogicalAggregate(LogicalAggregateMetadata),
    LogicalOrderBy(LogicalOrderByMetadata),
    LogicalRemap(LogicalRemapMetadata),
    LogicalSubquery(LogicalSubqueryMetadata),
    EnforcerSort(EnforcerSortMetadata),
    PhysicalTableScan(PhysicalTableScanMetadata),
    PhysicalNLJoin(PhysicalNLJoinMetadata),
    PhysicalHashJoin(PhysicalHashJoinMetadata),
    PhysicalFilter(PhysicalFilterMetadata),
    PhysicalProject(PhysicalProjectMetadata),
    PhysicalHashAggregate(PhysicalHashAggregateMetadata),
}


#[derive(Debug, PartialEq)]
pub enum OperatorCategory {
    Logical,
    Physical,
    Enforcer,
    Placeholder,
}

impl OperatorKind {
    /// Returns the category of the operator.
    pub fn category(&self) -> OperatorCategory {
        use OperatorKind::*;
        match self {
            Group(_) => OperatorCategory::Placeholder,
            LogicalGet(_) => OperatorCategory::Logical,
            LogicalJoin(_) => OperatorCategory::Logical,
            LogicalDependentJoin(_) => OperatorCategory::Logical,
            LogicalProject(_) => OperatorCategory::Logical,
            LogicalAggregate(_) => OperatorCategory::Logical,
            LogicalOrderBy(_) => OperatorCategory::Logical,
            LogicalRemap(_) => OperatorCategory::Logical,
            LogicalSelect(_) => OperatorCategory::Logical,
            LogicalSubquery(_) => OperatorCategory::Logical,
            EnforcerSort(_) => OperatorCategory::Enforcer,
            PhysicalFilter(_) => OperatorCategory::Physical,
            PhysicalProject(_) => OperatorCategory::Physical,
            PhysicalHashJoin(_) => OperatorCategory::Physical,
            PhysicalNLJoin(_) => OperatorCategory::Physical,
            PhysicalTableScan(_) => OperatorCategory::Physical,
            PhysicalHashAggregate(_) => OperatorCategory::Physical,
            MockScan(_) => OperatorCategory::Physical,
        }
    }

    pub fn get_kind_string(&self)-> String{
        use OperatorKind::*;
        match self{
            Group(_) => "Group".to_string(),
            LogicalGet(_) => "LogicalGet".to_string(),
            LogicalJoin(_) => "LogicalJoin".to_string(),
            LogicalDependentJoin(_) => "LogicalDependentJoin".to_string(),
            LogicalProject(_) => "LogicalProject".to_string(),
            LogicalAggregate(_) => "LogicalAggregate".to_string(),
            LogicalOrderBy(_) => "LogicalOrderBy".to_string(),
            LogicalRemap(_) => "LogicalRemap".to_string(),
            LogicalSelect(_) => "LogicalSelect".to_string(),
            LogicalSubquery(_) => "LogicalSubquery".to_string(),
            EnforcerSort(_) => "EnforcerSort".to_string(),
            PhysicalFilter(_) => "PhysicalFilter".to_string(),
            PhysicalProject(_) => "PhysicalProject".to_string(),
            PhysicalHashJoin(_) => "PhysicalHashJoin".to_string(),
            PhysicalNLJoin(_) => "PhysicalNLJoin".to_string(),
            PhysicalTableScan(_) => "PhysicalTableScan".to_string(),
            PhysicalHashAggregate(_) => "PhysicalHashAggregate".to_string(),
            MockScan(_) => "MockScan".to_string(),
        }
    }

    pub fn get_metadata_string(&self) -> String {
        use OperatorKind::*;
        match self {
            Group(meta) => meta.get_metadata_string(),
            MockScan(meta) => meta.get_metadata_string(),
            LogicalGet(meta) => meta.get_metadata_string(),
            LogicalJoin(meta) => meta.get_metadata_string(),
            LogicalDependentJoin(meta) => meta.get_metadata_string(),
            LogicalOrderBy(meta) => meta.get_metadata_string(),
            EnforcerSort(meta) => meta.get_metadata_string(),
            PhysicalTableScan(meta) => meta.get_metadata_string(),
            PhysicalNLJoin(meta) => meta.get_metadata_string(),
            PhysicalHashJoin(meta) => meta.get_metadata_string(),
            LogicalSelect(_)
            | LogicalProject(_)
            | LogicalAggregate(_)
            | LogicalRemap(_)
            | LogicalSubquery(_)
            | PhysicalFilter(_)
            | PhysicalProject(_)
            | PhysicalHashAggregate(_) => "".to_string(),
        }
    }

    pub fn from_kind_and_metadata_string(kind: &str, metadata: &str) -> Option<OperatorKind> {
        let metadata = metadata.trim();
        match kind {
            "Group" => Some(OperatorKind::Group(GroupMetadata::from_metadata_string(metadata)?)),
            "MockScan" => {
                Some(OperatorKind::MockScan(MockScanMetadata::from_metadata_string(metadata)?))
            }
            "LogicalGet" => Some(OperatorKind::LogicalGet(LogicalGetMetadata::from_metadata_string(metadata)?)),
            "LogicalJoin" => {
                Some(OperatorKind::LogicalJoin(LogicalJoinMetadata::from_metadata_string(metadata)?))
            }
            "LogicalDependentJoin" => Some(OperatorKind::LogicalDependentJoin(
                LogicalDependentJoinMetadata::from_metadata_string(metadata)?,
            )),
            "LogicalSelect" => {
                if metadata.is_empty() {
                    Some(OperatorKind::LogicalSelect(LogicalSelectMetadata {}))
                } else {
                    None
                }
            }
            "LogicalProject" => {
                if metadata.is_empty() {
                    Some(OperatorKind::LogicalProject(LogicalProjectMetadata {}))
                } else {
                    None
                }
            }
            "LogicalAggregate" => {
                if metadata.is_empty() {
                    Some(OperatorKind::LogicalAggregate(LogicalAggregateMetadata {}))
                } else {
                    None
                }
            }
            "LogicalOrderBy" => Some(OperatorKind::LogicalOrderBy(
                LogicalOrderByMetadata::from_metadata_string(metadata)?,
            )),
            "LogicalRemap" => {
                if metadata.is_empty() {
                    Some(OperatorKind::LogicalRemap(LogicalRemapMetadata {}))
                } else {
                    None
                }
            }
            "LogicalSubquery" => {
                if metadata.is_empty() {
                    Some(OperatorKind::LogicalSubquery(LogicalSubqueryMetadata {}))
                } else {
                    None
                }
            }
            "EnforcerSort" => {
                Some(OperatorKind::EnforcerSort(EnforcerSortMetadata::from_metadata_string(metadata)?))
            }
            "PhysicalTableScan" => Some(OperatorKind::PhysicalTableScan(
                PhysicalTableScanMetadata::from_metadata_string(metadata)?,
            )),
            "PhysicalNLJoin" => {
                Some(OperatorKind::PhysicalNLJoin(PhysicalNLJoinMetadata::from_metadata_string(metadata)?))
            }
            "PhysicalHashJoin" => Some(OperatorKind::PhysicalHashJoin(
                PhysicalHashJoinMetadata::from_metadata_string(metadata)?,
            )),
            "PhysicalFilter" => {
                if metadata.is_empty() {
                    Some(OperatorKind::PhysicalFilter(PhysicalFilterMetadata {}))
                } else {
                    None
                }
            }
            "PhysicalProject" => {
                if metadata.is_empty() {
                    Some(OperatorKind::PhysicalProject(PhysicalProjectMetadata {}))
                } else {
                    None
                }
            }
            "PhysicalHashAggregate" => {
                if metadata.is_empty() {
                    Some(OperatorKind::PhysicalHashAggregate(PhysicalHashAggregateMetadata {}))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn from_kind_string(kind: &str) -> Option<OperatorKind> {
        Self::from_kind_and_metadata_string(kind, "")
    }



    /// Returns true if the operator may produce columns as output.
    pub fn maybe_produce_columns(&self) -> bool {
        match self {
            OperatorKind::LogicalGet(_) | OperatorKind::PhysicalTableScan(_) => true,
            OperatorKind::LogicalProject(_) | OperatorKind::PhysicalProject(_) => true,
            OperatorKind::LogicalAggregate(_) | OperatorKind::PhysicalHashAggregate(_) => true,
            OperatorKind::MockScan(_) => true,
            _other => false,
        }
    }
}

/// The operator struct that is able to represent any operator type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Operator {
    /// The group ID if this operator is a placeholder for a group.
    pub group_id: Option<GroupId>,
    /// The operator type and associated metadata.
    pub kind: OperatorKind,
    /// The input operators and scalars.
    pub common: IRCommon<OperatorProperties>,
}

impl Operator {
    pub fn from_raw_parts(
        group_id: Option<GroupId>,
        kind: OperatorKind,
        common: IRCommon<OperatorProperties>,
    ) -> Self {
        Self {
            group_id,
            kind,
            common,
        }
    }

    /// Gets the slice to the input operators.
    pub fn input_operators(&self) -> &[Arc<Operator>] {
        &self.common.input_operators
    }

    /// Gets the slice to the input scalar expressions.
    pub fn input_scalars(&self) -> &[Arc<Scalar>] {
        &self.common.input_scalars
    }

    /// Gests the operator properties.
    pub fn properties(&self) -> &Arc<OperatorProperties> {
        &self.common.properties
    }

    /// Clones the operator, optionally replacing the input operators and the input scalar expressions.
    pub fn clone_with_inputs(
        &self,
        input_operators: Option<Arc<[Arc<Operator>]>>,
        input_scalars: Option<Arc<[Arc<Scalar>]>>,
    ) -> Self {
        let input_operators =
            input_operators.unwrap_or_else(|| self.common.input_operators.clone());
        let input_scalars = input_scalars.unwrap_or_else(|| self.common.input_scalars.clone());
        Self {
            group_id: None,
            kind: self.kind.clone(),
            common: IRCommon::new(input_operators, input_scalars),
        }
    }

    /// Gets the set of columns used by this operator and its children
    /// TODO: Are all columns used by an operator always stored in its scalar
    /// set? Can we guarantee used columns will not be part of the metadata
    /// set? If this is not guaranteed, should operators implement some sort of
    /// used_columns property (similar to output_schema), so we don't have this
    /// fragile assumption?
    pub fn collect_used_columns(&self) -> HashSet<Column> {
        let mut used = HashSet::new();
        self.collect_used_columns_recursive(&mut used);
        used
    }

    /// Recursive subcall used in collceted_used_columns to get child uses
    fn collect_used_columns_recursive(&self, used: &mut HashSet<Column>) {
        for scalar in self.input_scalars() {
            for col in scalar.used_columns().iter() {
                used.insert(*col);
            }
        }
        for child in self.input_operators() {
            child.collect_used_columns_recursive(used);
        }
    }
}

impl Explain for Operator {
    fn explain<'a>(
        &self,
        ctx: &super::IRContext,
        option: &super::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        match &self.kind {
            OperatorKind::Group(meta) => {
                Group::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::MockScan(meta) => {
                MockScan::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalGet(meta) => {
                LogicalGet::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalJoin(meta) => {
                LogicalJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                LogicalDependentJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalSelect(meta) => {
                LogicalSelect::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalOrderBy(meta) => {
                LogicalOrderBy::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::EnforcerSort(meta) => {
                EnforcerSort::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalTableScan(meta) => {
                PhysicalTableScan::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                PhysicalNLJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalHashJoin(meta) => {
                PhysicalHashJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalFilter(meta) => {
                PhysicalFilter::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalProject(meta) => {
                LogicalProject::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalProject(meta) => {
                PhysicalProject::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalAggregate(meta) => {
                LogicalAggregate::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                PhysicalHashAggregate::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalRemap(meta) => {
                LogicalRemap::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalSubquery(meta) => {
                LogicalSubquery::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::catalog::DataSourceId;
    use bitvec::vec::BitVec;

    fn assert_roundtrip(case_name: &str, kind: OperatorKind) {
        println!("[roundtrip] START case={case_name}");
        let original = Operator::from_raw_parts(None, kind, IRCommon::empty());
        let kind_str = original.kind.get_kind_string();
        let metadata_str = original.kind.get_metadata_string();
        println!(
            "[roundtrip] ENCODE case={case_name} kind='{}' metadata='{}'",
            kind_str, metadata_str
        );

        let parsed_kind = OperatorKind::from_kind_and_metadata_string(&kind_str, &metadata_str)
            .unwrap_or_else(|| {
                panic!(
                    "roundtrip parse should succeed for kind='{kind_str}', metadata='{metadata_str}'"
                )
            });
        println!("[roundtrip] PARSE case={case_name} ok");
        let rebuilt = Operator::from_raw_parts(None, parsed_kind, IRCommon::empty());

        assert_eq!(original, rebuilt);
        println!("[roundtrip] END case={case_name} ok");
    }

    #[test]
    fn operator_kind_roundtrip_string_conversion() {
        assert_roundtrip("Group", OperatorKind::Group(GroupMetadata {
            group_id: GroupId(7),
        }));

        assert_roundtrip("MockScan", OperatorKind::MockScan(MockScanMetadata {
            mock_id: 42,
            spec: Arc::new(MockSpec::default()),
        }));

        assert_roundtrip("LogicalGet", OperatorKind::LogicalGet(LogicalGetMetadata {
            source: DataSourceId(1),
            first_column: Column(0),
            projections: Arc::new([0, 1, 2]),
        }));

        assert_roundtrip("LogicalJoin", OperatorKind::LogicalJoin(LogicalJoinMetadata {
            join_type: join::JoinType::Mark(Column(3)),
        }));

        assert_roundtrip("LogicalDependentJoin", OperatorKind::LogicalDependentJoin(
            LogicalDependentJoinMetadata {
                join_type: join::JoinType::Left,
            },
        ));

        assert_roundtrip("LogicalOrderBy", OperatorKind::LogicalOrderBy(LogicalOrderByMetadata {
            directions: BitVec::from_iter([true, false, true]).into_boxed_bitslice(),
        }));

        assert_roundtrip("EnforcerSort", OperatorKind::EnforcerSort(EnforcerSortMetadata {
            tuple_ordering: Default::default(),
        }));

        assert_roundtrip("PhysicalTableScan", OperatorKind::PhysicalTableScan(PhysicalTableScanMetadata {
            source: DataSourceId(9),
            first_column: Column(10),
            projections: Arc::new([3, 4, 5]),
        }));

        assert_roundtrip("PhysicalNLJoin", OperatorKind::PhysicalNLJoin(PhysicalNLJoinMetadata {
            join_type: join::JoinType::Single,
        }));

        assert_roundtrip("PhysicalHashJoin", OperatorKind::PhysicalHashJoin(PhysicalHashJoinMetadata {
            join_type: join::JoinType::Inner,
            keys: Arc::from(vec![(Column(1), Column(2)), (Column(3), Column(4))]),
        }));

        assert_roundtrip("LogicalSelect", OperatorKind::LogicalSelect(LogicalSelectMetadata {}));
        assert_roundtrip("LogicalProject", OperatorKind::LogicalProject(LogicalProjectMetadata {}));
        assert_roundtrip("LogicalAggregate", OperatorKind::LogicalAggregate(LogicalAggregateMetadata {}));
        assert_roundtrip("LogicalRemap", OperatorKind::LogicalRemap(LogicalRemapMetadata {}));
        assert_roundtrip("LogicalSubquery", OperatorKind::LogicalSubquery(LogicalSubqueryMetadata {}));
        assert_roundtrip("PhysicalFilter", OperatorKind::PhysicalFilter(PhysicalFilterMetadata {}));
        assert_roundtrip("PhysicalProject", OperatorKind::PhysicalProject(PhysicalProjectMetadata {}));
        assert_roundtrip("PhysicalHashAggregate", OperatorKind::PhysicalHashAggregate(
            PhysicalHashAggregateMetadata {},
        ));
    }
}
