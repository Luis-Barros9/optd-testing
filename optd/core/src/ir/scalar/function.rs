//! Scalar functions are used to represent scalar function calls in the IR.

use crate::ir::{
    DataType, IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use itertools::Itertools;
use pretty_xmlish::Pretty;
use std::sync::Arc;

// TODO: Full type signature in optd, right now just use id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
    Window,
}

impl FunctionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            FunctionKind::Scalar => "Scalar",
            FunctionKind::Aggregate => "Aggregate",
            FunctionKind::Window => "Window",
        }
    }
}

define_node!(
    /// Metadata:
    /// - id: The identifier of the function.
    /// - kind: The kind of the function (scalar, aggregate, window).
    /// - return_type: The return data type of the function.
    /// Scalars:
    /// - params: The parameters of the function.
    Function, FunctionBorrowed {
        properties: ScalarProperties,
        metadata: FunctionMetadata {
            id: Arc<str>,
            kind: FunctionKind,
            return_type: DataType,
        },
        inputs: {
            operators: [],
            scalars: params[],
        },
    }
);
impl_scalar_conversion!(Function, FunctionBorrowed);

impl Function {
    pub fn new_aggregate(
        id: impl Into<Arc<str>>,
        params: Arc<[Arc<Scalar>]>,
        return_type: DataType,
    ) -> Self {
        Self {
            meta: FunctionMetadata {
                id: id.into(),
                kind: FunctionKind::Aggregate,
                return_type,
            },
            common: IRCommon::with_input_scalars_only(params),
        }
    }
}

impl FunctionMetadata {
    pub fn get_metadata_string(&self) -> String {
        format!(
            "{{ id: {}, kind: {}, return_type: {:?} }}",
            self.id,
            self.kind.as_str(),
            self.return_type
        )
    }

    pub fn from_metadata_string(metadata: &str) -> Option<Self> {
        let metadata = metadata.trim();
        if metadata.is_empty() {
            return Some(Self {
                id: std::sync::Arc::from(""),
                kind: FunctionKind::Scalar,
                return_type: DataType::Null,
            });
        }

        if metadata.contains("id:") && metadata.contains("kind:") && metadata.contains("return_type:") {
            return Some(Self {
                id: std::sync::Arc::from(""),
                kind: FunctionKind::Scalar,
                return_type: DataType::Null,
            });
        }
        None
    }
}

impl Explain for FunctionBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let params = self
            .params()
            .iter()
            .map(|t| t.explain(ctx, option).to_one_line_string(true))
            .join(", ");

        Pretty::Text(format!("{}({params})", self.id()).into())
    }
}
