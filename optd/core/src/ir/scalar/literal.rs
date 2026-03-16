//! Literal scalar values are used to represent constant values in expressions.
//! These hold ScalarValue enum variants.

use crate::ir::{
    IRCommon, ScalarValue,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use pretty_xmlish::Pretty;

define_node!(
    /// Metadata:
    /// - value: The literal scalar value.
    /// Scalars: (none)
    Literal, LiteralBorrowed {
        properties: ScalarProperties,
        metadata: LiteralMetadata {
            value: ScalarValue,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_scalar_conversion!(Literal, LiteralBorrowed);

impl Literal {
    pub fn new(value: ScalarValue) -> Self {
        Self {
            meta: LiteralMetadata { value },
            common: IRCommon::empty(),
        }
    }
}

impl LiteralMetadata {
    pub fn get_metadata_string(&self) -> String {
        format!("{{ value: {} }}", self.value)
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() || metadata.starts_with("{ value: ") {
            return Some(Self {
                value: ScalarValue::Utf8(None),
            });
        }
        None
    }
}

impl Literal {
    pub fn boolean(v: impl Into<Option<bool>>) -> Self {
        Self::new(ScalarValue::Boolean(v.into()))
    }

    pub fn int32(v: impl Into<Option<i32>>) -> Self {
        Self::new(ScalarValue::Int32(v.into()))
    }
}

impl Explain for LiteralBorrowed<'_> {
    fn explain<'a>(
        &self,
        _ctx: &crate::ir::IRContext,
        _option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        Pretty::display(self.value())
    }
}
