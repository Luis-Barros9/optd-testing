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

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim() {
            "Scalar" => Some(FunctionKind::Scalar),
            "Aggregate" => Some(FunctionKind::Aggregate),
            "Window" => Some(FunctionKind::Window),
            _ => None,
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

        let body = metadata.strip_prefix('{')?.strip_suffix('}')?.trim();
        let id_start = body.find("id:")? + "id:".len();
        let kind_marker = body.find(", kind:")?;
        let return_type_marker = body.find(", return_type:")?;

        if !(id_start <= kind_marker && kind_marker < return_type_marker) {
            return None;
        }

        let id = body[id_start..kind_marker].trim();
        let kind_str = body[(kind_marker + ", kind:".len())..return_type_marker].trim();
        let return_type_str = body[(return_type_marker + ", return_type:".len())..].trim();

        Some(Self {
            id: Arc::from(id),
            kind: FunctionKind::from_str(kind_str)?,
            return_type: parse_data_type_debug(return_type_str)?,
        })
    }
}

fn parse_data_type_debug(value: &str) -> Option<DataType> {
    crate::parser_records::parse_data_type(value)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_metadata_roundtrip_aggregate_decimal128() {
        let metadata = FunctionMetadata {
            id: Arc::from("sum"),
            kind: FunctionKind::Aggregate,
            return_type: DataType::Decimal128(38, 4),
        };

        let encoded = metadata.get_metadata_string();
        let decoded = FunctionMetadata::from_metadata_string(&encoded)
            .expect("function metadata should parse");

        assert_eq!(metadata, decoded);
    }

    #[test]
    fn function_metadata_roundtrip_default_scalar() {
        let metadata = FunctionMetadata {
            id: Arc::from(""),
            kind: FunctionKind::Scalar,
            return_type: DataType::Null,
        };

        let encoded = metadata.get_metadata_string();
        let decoded = FunctionMetadata::from_metadata_string(&encoded)
            .expect("function metadata should parse");

        assert_eq!(metadata, decoded);
    }
}
