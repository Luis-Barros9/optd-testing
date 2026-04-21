//! Scalar expressions are used to compute values in various contexts, such as
//! projections, filters, and join conditions. They can represent literals,
//! column references, and other scalar operations.
//!
//! Each scalar expression is represented by the `Scalar` struct, which contains
//! metadata specific to the type of scalar expression it represents.
//!
//! Similar to operators, scalar expressions are stored in plans using the
//! Scalar struct, which holds the specific kind of scalar expression and its
//! associated metadata. This can be "downcast" to the specific scalar type
//! when needed.

mod assign;
mod binary_op;
mod cast;
mod column_ref;
mod function;
mod like;
mod list;
mod literal;
mod nary_op;

use std::sync::Arc;
use serde_json::{json, Map, Value};

pub use assign::*;
pub use binary_op::*;
pub use cast::*;
pub use column_ref::*;
pub use function::*;
pub use like::*;
pub use list::*;
pub use literal::*;
pub use nary_op::*;

use crate::ir::{
    ColumnSet, IRCommon, Operator, ScalarValue, convert::IntoScalar, explain::Explain,
    properties::ScalarProperties,
};

/// The scalar type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarKind {
    Literal(LiteralMetadata),
    ColumnRef(ColumnRefMetadata),
    ColumnAssign(ColumnAssignMetadata),
    BinaryOp(BinaryOpMetadata),
    NaryOp(NaryOpMetadata),
    List(ListMetadata),
    Function(FunctionMetadata),
    Cast(CastMetadata),
    Like(LikeMetadata),
}

impl ScalarKind {
    pub fn get_kind_string(&self) -> String {
        match self {
            ScalarKind::Literal(_) => "Literal".to_string(),
            ScalarKind::ColumnRef(_) => "ColumnRef".to_string(),
            ScalarKind::ColumnAssign(_) => "ColumnAssign".to_string(),
            ScalarKind::BinaryOp(_) => "BinaryOp".to_string(),
            ScalarKind::NaryOp(_) => "NaryOp".to_string(),
            ScalarKind::List(_) => "List".to_string(),
            ScalarKind::Function(_) => "Function".to_string(),
            ScalarKind::Cast(_) => "Cast".to_string(),
            ScalarKind::Like(_) => "Like".to_string(),
        }
    }

    pub fn get_metadata_string(&self) -> String {
        match self {
            ScalarKind::Literal(meta) => meta.get_metadata_string(),
            ScalarKind::ColumnRef(meta) => meta.get_metadata_string(),
            ScalarKind::ColumnAssign(meta) => meta.get_metadata_string(),
            ScalarKind::BinaryOp(meta) => meta.get_metadata_string(),
            ScalarKind::NaryOp(meta) => meta.get_metadata_string(),
            ScalarKind::List(meta) => meta.get_metadata_string(),
            ScalarKind::Function(meta) => meta.get_metadata_string(),
            ScalarKind::Cast(meta) => meta.get_metadata_string(),
            ScalarKind::Like(meta) => meta.get_metadata_string(),
        }
    }

    /// Inverse from kind and metadata strings.
    ///
    /// `metadata` may be empty. When provided, it is validated (and parsed for
    /// simple cases) according to the format produced by `get_metadata_string`.
    pub fn from_kind_and_metadata_string(kind: &str, metadata: &str) -> Option<ScalarKind> {
        match kind {
            "Literal" => Some(ScalarKind::Literal(LiteralMetadata::from_metadata_string(metadata)?)),
            "ColumnRef" => {
                Some(ScalarKind::ColumnRef(ColumnRefMetadata::from_metadata_string(metadata)?))
            }
            "ColumnAssign" => Some(ScalarKind::ColumnAssign(
                ColumnAssignMetadata::from_metadata_string(metadata)?,
            )),
            "BinaryOp" => {
                Some(ScalarKind::BinaryOp(BinaryOpMetadata::from_metadata_string(metadata)?))
            }
            "NaryOp" => Some(ScalarKind::NaryOp(NaryOpMetadata::from_metadata_string(metadata)?)),
            "List" => Some(ScalarKind::List(ListMetadata::from_metadata_string(metadata)?)),
            "Function" => {
                Some(ScalarKind::Function(FunctionMetadata::from_metadata_string(metadata)?))
            }
            "Cast" => Some(ScalarKind::Cast(CastMetadata::from_metadata_string(metadata)?)),
            "Like" => Some(ScalarKind::Like(LikeMetadata::from_metadata_string(metadata)?)),
            _ => None,
        }
    }

    /// Convenience overload-like helper when only kind is provided.
    pub fn from_kind_string(kind: &str) -> Option<ScalarKind> {
        Self::from_kind_and_metadata_string(kind, "")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Scalar {
    pub kind: ScalarKind,
    pub common: IRCommon<ScalarProperties>,
}

impl Scalar {
    /// Gets the slice to the input operators.
    pub fn input_operators(&self) -> &[Arc<Operator>] {
        &self.common.input_operators
    }

    /// Gets the slice to the input scalar expressions.
    pub fn input_scalars(&self) -> &[Arc<Scalar>] {
        &self.common.input_scalars
    }

    /// Generates a mutable JSON object with scalar-specific fields.
    ///
    /// Callers can append extra relation fields afterwards (e.g. id, position,
    /// referenced, parent_scalar).
    pub fn to_json(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("kind".to_string(), json!(self.kind.get_kind_string()));
        let metadata = self.kind.get_metadata_string();
        if !metadata.is_empty() {
            obj.insert("metadata".to_string(), json!(metadata));
        }
        Value::Object(obj)
    }

        
    /// Clones the operator, optionally replacing the input operators and the input scalar expressions.
    pub fn clone_with_inputs(
        &self,
        scalars: Option<Arc<[Arc<Scalar>]>>,
        operators: Option<Arc<[Arc<Operator>]>>,
    ) -> Self {
        let operators = operators.unwrap_or_else(|| self.common.input_operators.clone());
        let scalars = scalars.unwrap_or_else(|| self.common.input_scalars.clone());
        Self {
            kind: self.kind.clone(),
            common: IRCommon::new(operators, scalars),
        }
    }

    /// Returns the set of columns used by this scalar expression.
    pub fn used_columns(&self) -> ColumnSet {
        match &self.kind {
            ScalarKind::Literal(_) => ColumnSet::default(),
            ScalarKind::ColumnRef(meta) => ColumnSet::from_iter(std::iter::once(meta.column)),
            ScalarKind::BinaryOp(_)
            | ScalarKind::NaryOp(_)
            | ScalarKind::ColumnAssign(_)
            | ScalarKind::List(_)
            | ScalarKind::Cast(_)
            | ScalarKind::Like(_)
            | ScalarKind::Function(_) => self.input_scalars().iter().fold(
                ColumnSet::default(),
                |mut used_columns, scalar| {
                    used_columns |= &scalar.used_columns();
                    used_columns
                },
            ),
        }
    }

    /// Conjoin predicates with `AND`, returning `true` for an empty list.
    pub fn combine_conjuncts(mut conds: Vec<Arc<Scalar>>) -> Arc<Scalar> {
        if conds.is_empty() {
            Literal::boolean(true).into_scalar()
        } else if conds.len() == 1 {
            conds.pop().unwrap()
        } else {
            NaryOp::new(NaryOpKind::And, conds.into()).into_scalar()
        }
    }

    // Simplifies an n-ary scalar by dropping redundant terms
    pub fn simplify_nary_scalar(self: Arc<Self>) -> Arc<Scalar> {
        match &self.kind {
            ScalarKind::BinaryOp(bin) if bin.op_kind == BinaryOpKind::IsNotDistinctFrom => {
                let lhs = self.input_scalars()[0].clone();
                let rhs = self.input_scalars()[1].clone();
                if lhs == rhs {
                    Literal::boolean(true).into_scalar()
                } else {
                    self
                }
            }
            ScalarKind::NaryOp(nary) if nary.op_kind == NaryOpKind::And => {
                let mut terms = Vec::new();
                for term in self.input_scalars() {
                    if matches!(&term.kind, ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true))))
                    {
                        continue;
                    }
                    if matches!(&term.kind, ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(false))))
                    {
                        return Literal::boolean(false).into_scalar();
                    }
                    terms.push(term.clone());
                }

                if terms.is_empty() {
                    return Literal::boolean(true).into_scalar();
                }
                if terms.len() == 1 {
                    return terms.pop().unwrap();
                }
                if terms.as_slice() == self.input_scalars() {
                    return self;
                }

                Arc::new(self.clone_with_inputs(Some(Arc::from(terms)), None))
            }
            _ => self,
        }
    }

    pub fn is_true_scalar(&self) -> bool {
        matches!(
            &self.kind,
            ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true)))
        )
    }
}

impl Explain for Scalar {
    fn explain<'a>(
        &self,
        ctx: &super::IRContext,
        option: &super::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        match &self.kind {
            ScalarKind::Literal(meta) => {
                Literal::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::ColumnRef(meta) => {
                ColumnRef::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::BinaryOp(meta) => {
                BinaryOp::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::NaryOp(meta) => {
                NaryOp::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::ColumnAssign(meta) => {
                ColumnAssign::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::List(meta) => {
                List::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Function(meta) => {
                Function::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Cast(meta) => {
                Cast::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Like(meta) => {
                Like::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_roundtrip(case_name: &str, kind: ScalarKind) {
        println!("[scalar-roundtrip] START case={case_name}");
        let original = Scalar {
            kind,
            common: IRCommon::empty(),
        };

        let kind_str = original.kind.get_kind_string();
        let metadata_str = original.kind.get_metadata_string();
        println!(
            "[scalar-roundtrip] ENCODE case={case_name} kind='{}' metadata='{}'",
            kind_str, metadata_str
        );

        let parsed_kind = ScalarKind::from_kind_and_metadata_string(&kind_str, &metadata_str)
            .unwrap_or_else(|| {
                panic!(
                    "scalar roundtrip parse should succeed for kind='{kind_str}', metadata='{metadata_str}'"
                )
            });
        println!("[scalar-roundtrip] PARSE case={case_name} ok");

        let rebuilt = Scalar {
            kind: parsed_kind,
            common: IRCommon::empty(),
        };

        assert_eq!(original, rebuilt);
        println!("[scalar-roundtrip] END case={case_name} ok");
    }

    #[test]
    fn scalar_kind_roundtrip_string_conversion() {
        assert_roundtrip(
            "Literal",
            ScalarKind::Literal(LiteralMetadata {
                value: ScalarValue::Utf8View(Some("FURNITURE".to_string())),
            }),
        );
        assert_roundtrip(
            "ColumnRef",
            ScalarKind::ColumnRef(ColumnRefMetadata { column: crate::ir::Column(11) }),
        );
        assert_roundtrip(
            "ColumnAssign",
            ScalarKind::ColumnAssign(ColumnAssignMetadata {
                column: crate::ir::Column(22),
            }),
        );
        assert_roundtrip(
            "BinaryOp",
            ScalarKind::BinaryOp(BinaryOpMetadata {
                op_kind: BinaryOpKind::IsNotDistinctFrom,
            }),
        );
        assert_roundtrip(
            "NaryOp",
            ScalarKind::NaryOp(NaryOpMetadata {
                op_kind: NaryOpKind::Or,
            }),
        );
        assert_roundtrip("List", ScalarKind::List(ListMetadata {}));
        assert_roundtrip(
            "Function",
            ScalarKind::Function(FunctionMetadata {
                id: Arc::from(""),
                kind: FunctionKind::Scalar,
                return_type: crate::ir::DataType::Null,
            }),
        );
        assert_roundtrip(
            "Function",
            ScalarKind::Function(FunctionMetadata {
                id: Arc::from("sum"),
                kind: FunctionKind::Aggregate,
                return_type: crate::ir::DataType::Decimal128(38, 4),
            }),
        );
        assert_roundtrip(
            "Cast",
            ScalarKind::Cast(CastMetadata {
                data_type: crate::ir::DataType::Null,
            }),
        );
        assert_roundtrip(
            "Like",
            ScalarKind::Like(LikeMetadata {
                negated: true,
                escape_char: Some('!'),
                case_insensative: false,
            }),
        );
    }
}
