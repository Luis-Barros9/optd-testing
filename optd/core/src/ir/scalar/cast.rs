//! IR node for casting scalar expressions to different data types.

use crate::ir::{
    DataType, IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - data_type: The target data type to cast to.
    /// Scalars:
    /// - expr: The scalar expression to be cast.
    Cast, CastBorrowed {
        properties: ScalarProperties,
        metadata: CastMetadata {
            data_type: DataType,
        },
        inputs: {
            operators: [],
            scalars: [expr],
        }
    }
);
impl_scalar_conversion!(Cast, CastBorrowed);

impl Cast {
    pub fn new(data_type: DataType, expr: Arc<Scalar>) -> Self {
        Self {
            meta: CastMetadata { data_type },
            common: IRCommon::with_input_scalars_only(Arc::new([expr])),
        }
    }
}

impl CastMetadata {
    pub fn get_metadata_string(&self) -> String {
        format!("{{ data_type: {:?} }}", self.data_type)
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() || metadata.starts_with("{ data_type: ") {
            return Some(Self {
                data_type: DataType::Null,
            });
        }
        None
    }
}

impl Explain for CastBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let fmt = format!(
            "CAST ({} AS {:?})",
            self.expr().explain(ctx, option).to_one_line_string(true),
            self.data_type(),
        );
        Pretty::display(&fmt)
    }
}
