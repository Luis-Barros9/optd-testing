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
    let value = value.trim();
    match value {
        "Null" => Some(DataType::Null),
        "Boolean" => Some(DataType::Boolean),
        "Int8" => Some(DataType::Int8),
        "Int16" => Some(DataType::Int16),
        "Int32" => Some(DataType::Int32),
        "Int64" => Some(DataType::Int64),
        "UInt8" => Some(DataType::UInt8),
        "UInt16" => Some(DataType::UInt16),
        "UInt32" => Some(DataType::UInt32),
        "UInt64" => Some(DataType::UInt64),
        "Utf8" => Some(DataType::Utf8),
        "Utf8View" => Some(DataType::Utf8View),
        "Date32" => Some(DataType::Date32),
        "Date64" => Some(DataType::Date64),
        _ if value.starts_with("Decimal32(") && value.ends_with(')') => {
            let (precision, scale) = parse_decimal_params(value, "Decimal32")?;
            Some(DataType::Decimal32(precision, scale))
        }
        _ if value.starts_with("Decimal64(") && value.ends_with(')') => {
            let (precision, scale) = parse_decimal_params(value, "Decimal64")?;
            Some(DataType::Decimal64(precision, scale))
        }
        _ if value.starts_with("Decimal128(") && value.ends_with(')') => {
            let (precision, scale) = parse_decimal_params(value, "Decimal128")?;
            Some(DataType::Decimal128(precision, scale))
        }
        _ => None,
    }
}

fn parse_decimal_params(value: &str, prefix: &str) -> Option<(u8, i8)> {
    let inner = value
        .strip_prefix(prefix)?
        .strip_prefix('(')?
        .strip_suffix(')')?;
    let (precision, scale) = inner.split_once(',')?;
    let precision = precision.trim().parse::<u8>().ok()?;
    let scale = scale.trim().parse::<i8>().ok()?;
    Some((precision, scale))
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
