//! The table scan operator is a scan on some table data - as one implementation
//! of the logical get operator.

use crate::ir::{
    Column, IRCommon,
    catalog::DataSourceId,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - source: The data source to scan.
    /// - first_column: The columns of the data source have monotonic indices
    ///                 starting from this column.
    /// - projections: The list of column indices to project from this table.
    /// Scalars: (none)
    PhysicalTableScan, PhysicalTableScanBorrowed {
        properties: OperatorProperties,
        metadata: PhysicalTableScanMetadata {
            source: DataSourceId,
            first_column: Column,
            projections: Arc<[usize]>,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_operator_conversion!(PhysicalTableScan, PhysicalTableScanBorrowed);

impl PhysicalTableScan {
    pub fn new(source: DataSourceId, first_column: Column, projections: Arc<[usize]>) -> Self {
        Self {
            meta: PhysicalTableScanMetadata {
                source,
                first_column,
                projections,
            },
            common: IRCommon::empty(),
        }
    }
}

impl PhysicalTableScanMetadata {
    pub fn get_metadata_string(&self) -> String {
        let projections = self
            .projections
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "{{ source: {}, first_column: {}, projections: [{}] }}",
            self.source.0, self.first_column.0, projections
        )
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Some(Self {
                source: DataSourceId(0),
                first_column: Column(0),
                projections: Arc::new([]),
            });
        }

        let payload = metadata
            .strip_prefix("{ ")
            .and_then(|m| m.strip_suffix(" }"))?;

        let payload = payload.strip_prefix("source: ")?;
        let (source_raw, payload) = payload.split_once(", first_column: ")?;
        let (first_column_raw, projections_raw) = payload.split_once(", projections: ")?;

        let source = DataSourceId(source_raw.trim().parse::<i64>().ok()?);
        let first_column = Column(first_column_raw.trim().parse::<usize>().ok()?);

        let list = projections_raw
            .trim()
            .strip_prefix("[")?
            .strip_suffix("]")?
            .trim();
        let parsed = if list.is_empty() {
            Vec::new()
        } else {
            list.split(',')
                .map(|n| n.trim().parse::<usize>().ok())
                .collect::<Option<Vec<_>>>()?
        };

        Some(Self {
            source,
            first_column,
            projections: Arc::from(parsed.into_boxed_slice()),
        })
    }
}

impl Explain for PhysicalTableScanBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(2);
        fields.push((".source", Pretty::display(&self.source().0)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("PhysicalTableScan", fields)
    }
}
