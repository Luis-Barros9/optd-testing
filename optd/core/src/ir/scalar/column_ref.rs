//! A reference to a Column in the IR.

use crate::ir::{
    Column, IRCommon,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use pretty_xmlish::Pretty;

define_node!(
    /// Metadata:
    /// - column: The referenced column.
    /// Scalars: (none)
    ColumnRef, ColumnRefBorrowed {
        properties: ScalarProperties,
        metadata: ColumnRefMetadata {
            column: Column,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_scalar_conversion!(ColumnRef, ColumnRefBorrowed);

impl ColumnRef {
    pub fn new(column: Column) -> Self {
        Self {
            meta: ColumnRefMetadata { column },
            common: IRCommon::empty(),
        }
    }
}

impl ColumnRefMetadata {
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

impl Explain for ColumnRefBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        _option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let meta = ctx.get_column_meta(self.column());
        Pretty::display(&format!("{}({})", meta.name, self.column()))
    }
}
