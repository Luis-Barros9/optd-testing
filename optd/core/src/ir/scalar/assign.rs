//! Assigns a scalar expression to a new Column

use crate::ir::{
    Column, IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - column: The column being assigned to.
    /// Scalars:
    /// - expr: The expression being assigned to this new column
    ColumnAssign, ColumnAssignBorrowed {
        properties: ScalarProperties,
        metadata: ColumnAssignMetadata {
            column: Column,
        },
        inputs: {
            operators: [],
            scalars: [expr],
        }
    }
);
impl_scalar_conversion!(ColumnAssign, ColumnAssignBorrowed);

impl ColumnAssign {
    pub fn new(column: Column, expr: Arc<Scalar>) -> Self {
        Self {
            meta: ColumnAssignMetadata { column },
            common: IRCommon::with_input_scalars_only(Arc::new([expr])),
        }
    }
}

impl ColumnAssignMetadata {
    pub fn get_metadata_string(&self) -> String {
        format!("{{ column: {} }}", self.column.0)
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Some(Self { column: Column(0) });
        }

        let payload = metadata
            .strip_prefix("{ ")
            .and_then(|m| m.strip_suffix(" }"))?;
        let value = payload.strip_prefix("column: ")?.trim();
        let column = value.parse::<usize>().ok()?;
        Some(Self {
            column: Column(column),
        })
    }
}

impl Explain for ColumnAssignBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let column_meta = ctx.get_column_meta(self.column());
        let fmt = format!(
            "{}({}) := {}",
            &column_meta.name,
            self.column(),
            self.expr().explain(ctx, option).to_one_line_string(true)
        );
        Pretty::display(&fmt)
    }
}
