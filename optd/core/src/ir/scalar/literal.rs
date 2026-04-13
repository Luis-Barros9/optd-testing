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
        if metadata.is_empty() {
            return Some(Self {
                value: ScalarValue::Utf8(None),
            });
        }

        let value = metadata
            .strip_prefix("{ value: ")?
            .strip_suffix(" }")?
            .trim();

        Some(Self {
            value: parse_scalar_value(value)?,
        })
    }
}

fn parse_scalar_value(value: &str) -> Option<ScalarValue> {
    let value = value.trim();

    if value.eq_ignore_ascii_case("null") {
        return Some(ScalarValue::Utf8(None));
    }

    let (raw_value, raw_type) = value.rsplit_once("::")?;
    let raw_value = raw_value.trim();
    let raw_type = raw_type.trim().to_ascii_lowercase();

    match raw_type.as_str() {
        "boolean" => raw_value.parse::<bool>().ok().map(|value| ScalarValue::Boolean(Some(value))),
        "integer" => raw_value.parse::<i32>().ok().map(|value| ScalarValue::Int32(Some(value))),
        "bigint" => raw_value.parse::<i64>().ok().map(|value| ScalarValue::Int64(Some(value))),
        "int8" => raw_value.parse::<i8>().ok().map(|value| ScalarValue::Int8(Some(value))),
        "int16" => raw_value.parse::<i16>().ok().map(|value| ScalarValue::Int16(Some(value))),
        "uint8" => raw_value.parse::<u8>().ok().map(|value| ScalarValue::UInt8(Some(value))),
        "uint16" => raw_value.parse::<u16>().ok().map(|value| ScalarValue::UInt16(Some(value))),
        "uint32" => raw_value.parse::<u32>().ok().map(|value| ScalarValue::UInt32(Some(value))),
        "uint64" => raw_value.parse::<u64>().ok().map(|value| ScalarValue::UInt64(Some(value))),
        "utf8" => Some(ScalarValue::Utf8(Some(raw_value.to_string()))),
        "utf8_view" => Some(ScalarValue::Utf8View(Some(raw_value.to_string()))),
        "date32" => {
            let date = chrono::NaiveDate::parse_from_str(raw_value, "%Y-%m-%d").ok()?;
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let days = (date - epoch).num_days();
            i32::try_from(days).ok().map(|days| ScalarValue::Date32(Some(days)))
        }
        "date64" => {
            let date = chrono::NaiveDate::parse_from_str(raw_value, "%Y-%m-%d").ok()?;
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let millis = (date - epoch).num_milliseconds();
            Some(ScalarValue::Date64(Some(millis)))
        }
        _ if raw_type.starts_with("decimal32(") && raw_type.ends_with(')') => {
            let (precision, scale) = parse_decimal_params(&raw_type, "decimal32")?;
            let value = raw_value.parse::<i32>().ok()?;
            Some(ScalarValue::Decimal32(Some(value), precision, scale))
        }
        _ if raw_type.starts_with("decimal64(") && raw_type.ends_with(')') => {
            let (precision, scale) = parse_decimal_params(&raw_type, "decimal64")?;
            let value = raw_value.parse::<i64>().ok()?;
            Some(ScalarValue::Decimal64(Some(value), precision, scale))
        }
        _ if raw_type.starts_with("decimal128(") && raw_type.ends_with(')') => {
            let (precision, scale) = parse_decimal_params(&raw_type, "decimal128")?;
            let value = raw_value.parse::<i128>().ok()?;
            Some(ScalarValue::Decimal128(Some(value), precision, scale))
        }
        _ => None,
    }
}

fn parse_decimal_params(raw_type: &str, prefix: &str) -> Option<(u8, i8)> {
    let params = raw_type.strip_prefix(prefix)?.strip_prefix('(')?.strip_suffix(')')?;
    let (precision, scale) = params.split_once(',')?;
    let precision = precision.trim().parse::<u8>().ok()?;
    let scale = scale.trim().parse::<i8>().ok()?;
    Some((precision, scale))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_metadata_roundtrip_utf8_view() {
        let metadata = LiteralMetadata {
            value: ScalarValue::Utf8View(Some("FURNITURE".to_string())),
        };

        let encoded = metadata.get_metadata_string();
        let decoded = LiteralMetadata::from_metadata_string(&encoded)
            .expect("utf8_view metadata should parse");

        assert_eq!(metadata.value, decoded.value);
    }

    #[test]
    fn literal_metadata_roundtrip_bigint() {
        let metadata = LiteralMetadata {
            value: ScalarValue::Int64(Some(150000)),
        };

        let encoded = metadata.get_metadata_string();
        let decoded = LiteralMetadata::from_metadata_string(&encoded)
            .expect("bigint metadata should parse");

        assert_eq!(metadata.value, decoded.value);
    }

    #[test]
    fn literal_metadata_roundtrip_date32() {
        let metadata = LiteralMetadata {
            value: ScalarValue::Date32(Some(9218)),
        };

        let encoded = metadata.get_metadata_string();
        let decoded = LiteralMetadata::from_metadata_string(&encoded)
            .expect("date32 metadata should parse");

        assert_eq!(metadata.value, decoded.value);
    }

    #[test]
    fn literal_metadata_roundtrip_decimal128() {
        let metadata = LiteralMetadata {
            value: ScalarValue::Decimal128(Some(1), 20, 0),
        };

        let encoded = metadata.get_metadata_string();
        let decoded = LiteralMetadata::from_metadata_string(&encoded)
            .expect("decimal128 metadata should parse");

        assert_eq!(metadata.value, decoded.value);
    }
}
