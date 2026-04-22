
use std::{
    collections::{BTreeMap, HashMap, VecDeque}, fmt::Debug, hash::Hash, sync::{Arc, OnceLock, atomic::AtomicI64}
};



use itertools::Itertools;
use tokio::sync::watch;
use tracing::{info, instrument, trace};

use crate::{
    ir::{
        Column, ColumnSet, Group, GroupId, IRCommon, IRContext, Operator, OperatorKind, Scalar, convert::IntoOperator, cost::Cost, explain::{Explain, ExplainOption}, properties::{Cardinality, GetProperty, OperatorProperties, OutputColumns, Required}
    },
    utility::union_find::UnionFind,
};

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Int32Array, RecordBatch, StringArray, StringViewArray,
};
use serde_json::{json, Value};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct MemoGroupExpr {
    meta: OperatorKind,
    inputs: Box<[GroupId]>,
    split: usize,
}

impl MemoGroupExpr {
    pub fn new(meta: OperatorKind, inputs: Box<[GroupId]>, split: usize) -> Self {
        Self {
            meta,
            inputs,
            split,
        }
    }

    pub fn input_operators(&self) -> &[GroupId] {
        &self.inputs[..self.split]
    }

    pub fn input_scalars(&self) -> &[GroupId] {
        &self.inputs[self.split..]
    }

    pub fn kind(&self) -> &OperatorKind {
        &self.meta
    }

    pub fn clone_with_inputs(&self, inputs: Box<[GroupId]>) -> Self {
        Self {
            meta: self.meta.clone(),
            inputs,
            split: self.split,
        }
    }
}

impl std::fmt::Debug for MemoGroupExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoGroupExpr")
            .field("meta", &self.meta)
            .field("inputs", &self.input_operators())
            .field("scalars", &self.input_scalars())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(pub(crate) i64);

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "id#{}", self.0)
    }
}

impl Id {
    pub const UNKNOWN: Self = Id(0);
}

impl From<GroupId> for Id {
    fn from(value: GroupId) -> Self {
        Id(value.0)
    }
}

impl From<Id> for GroupId {
    fn from(value: Id) -> Self {
        GroupId(value.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WithId<K> {
    id: Id,
    key: K,
}

impl<K> WithId<K> {
    pub const fn unknown(key: K) -> Self {
        Self {
            id: Id::UNKNOWN,
            key,
        }
    }

    pub const fn new(id: Id, key: K) -> Self {
        Self { id, key }
    }
    pub const fn id(&self) -> Id {
        self.id
    }

    pub const fn key(&self) -> &K {
        &self.key
    }
}

impl<K> From<K> for WithId<K> {
    fn from(value: K) -> Self {
        WithId::unknown(value)
    }
}

pub struct MemoTable {
    /// Scalar deduplication.
    scalar_dedup: HashMap<Arc<Scalar>, GroupId>,
    scalar_id_to_key: HashMap<GroupId, Arc<Scalar>>,
    /// Operator deduplication.
    operator_dedup: HashMap<Arc<MemoGroupExpr>, Id>,
    /// Operator Id to
    id_to_group_ids: UnionFind<GroupId>,
    groups: BTreeMap<GroupId, MemoGroup>,
    id_allocator: IdAllocator,
    ctx: IRContext,
}



impl std::fmt::Debug for MemoTable 
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MemoTable {{")?;
        writeln!(f, "  Scalars")?;
        for (scalar_id, scalar) in &self.scalar_id_to_key {
            writeln!(f, "    {} = {:?}\n", scalar_id, scalar)?;
        }
        writeln!(f, "  Groups: {}\n", self.groups.len())?;
        
        for (group_id, group) in &self.groups {
            writeln!(f, "  ═══════════════════════════════════")?;
            writeln!(f, "  Group {}", group_id)?;
            writeln!(f, "  ═══════════════════════════════════")?;
            
            // Format exploration
            let exploration = group.exploration.borrow();
            writeln!(f, "    Exploration:")?;
            writeln!(f, "      Status: {:?}", exploration.status)?;
            writeln!(f, "      Expressions: [")?;
            for expr in &exploration.exprs {
                writeln!(f, "        - {} {:?}", expr.id(), expr.key())?;
            }
            writeln!(f, "      ]")?;
            writeln!(f, "      Properties: {:?}", exploration.properties)?;
            
            // Format optimizations
            if !group.optimizations.is_empty() {
                writeln!(f, "\n    Optimizations:")?;
                for (required, optimization) in &group.optimizations {
                    let opt = optimization.borrow();
                    writeln!(f, "      Required: {}", required)?;
                    writeln!(f, "        Status: {:?}", opt.status)?;
                    writeln!(f, "        Costed Exprs: [")?;
                    for costed in &opt.costed_exprs {
                        writeln!(f, "          - {} op_cost={:?} total_cost={:?}",
                                 costed.group_expr.id(),
                                 costed.operator_cost,
                                 costed.total_cost)?;
                    }
                    writeln!(f, "        ]")?;
                    if !opt.enforcers.is_empty() {
                        writeln!(f, "        Enforcers: {:?}", opt.enforcers)?;
                    }
                }
            }
            writeln!(f)?;
        }
        
        writeln!(f, "}}")
    }
}

impl MemoTable {
    pub fn new(ctx: IRContext) -> Self {
        Self {
            scalar_dedup: Default::default(),
            scalar_id_to_key: Default::default(),
            operator_dedup: Default::default(),
            id_to_group_ids: Default::default(),
            groups: Default::default(),
            id_allocator: Default::default(),
            ctx,
        }
    }

    
    pub fn clear_memo(&mut self) {
        self.scalar_dedup.clear();
        self.scalar_id_to_key.clear();
        self.operator_dedup.clear();
        self.id_to_group_ids = Default::default();
        self.groups.clear();
        self.id_allocator = Default::default(); // NOTE: LATER TRY WITHOUT RESTARTING ID ALLOCATOR
    }

    pub fn load_from_db(&mut self, db_rows: HashMap<String, Vec<RecordBatch>>){
        //TODO load the memo from the database, see memo.sql for the schema
        // make sure the structures are empty before loading    
        fn parse_float(
            batch: &RecordBatch,
            row: usize,
            column: &str,
        ) -> Result<Option<f32>, String> {
            let col = batch
                .column_by_name(column)
                .ok_or_else(|| format!("missing '{}' column", column))?
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| format!("column '{}' is not Float32", column))?;

            if col.is_null(row) {
                return Ok(None);
            }

            Ok(Some(col.value(row)))
        }

        fn parse_int(
            batch: &RecordBatch,
            row: usize,
            column: &str,
        ) -> Result<GroupId, String> {
            let col = batch
                .column_by_name(column)
                .ok_or_else(|| format!("missing '{}' column", column))?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| format!("column '{}' is not Int32", column))?;

            if col.is_null(row) {
                return Err(format!("column '{}' is NULL at row {}", column, row));
            }

            Ok(GroupId(i64::from(col.value(row))))
        }

        fn parse_kind(batch: &RecordBatch, row: usize, column: &str) -> Result<String, String> {
            let arr = batch
                .column_by_name(column)
                .ok_or_else(|| format!("missing '{}' column", column))?;

            if let Some(col) = arr.as_any().downcast_ref::<StringViewArray>() {
                if col.is_null(row) {
                    return Err(format!("column '{}' is NULL at row {}", column, row));
                }
                return Ok(col.value(row).to_string());
            }

            if let Some(col) = arr.as_any().downcast_ref::<StringArray>() {
                if col.is_null(row) {
                    return Err(format!("column '{}' is NULL at row {}", column, row));
                }
                return Ok(col.value(row).to_string());
            }

            Err(format!("column '{}' is not Utf8/Utf8View", column))
        }

        fn parse_nullablemetadata(
            batch: &RecordBatch,
            row: usize,
            column: &str,
        ) -> Result<Option<String>, String> {
            let arr = batch
                .column_by_name(column)
                .ok_or_else(|| format!("missing '{}' column", column))?;

            if let Some(col) = arr.as_any().downcast_ref::<StringViewArray>() {
                if col.is_null(row) {
                    return Ok(None);
                }
                return Ok(Some(col.value(row).to_string()));
            }

            if let Some(col) = arr.as_any().downcast_ref::<StringArray>() {
                if col.is_null(row) {
                    return Ok(None);
                }
                return Ok(Some(col.value(row).to_string()));
            }

            Err(format!("column '{}' is not Utf8/Utf8View", column))
        }


        //println!("Loading memo from database...");
        self.clear_memo();


        let mut scalars_inputs: HashMap<GroupId, Vec<(i32, Scalar)>> = HashMap::new(); // scalar -> (position, input_scalar)
        // estrutura que faça mapping dos ids da bd, para ids no memo
        //let id_mapping: HashMap<GroupId, GroupId> = HashMap::new();
        let mut referenced_scalars: HashMap<GroupId, Arc<Scalar>> = HashMap::new(); // key -> id usado na bd
        let mut groups_by_id: HashMap<GroupId, MemoGroup> = HashMap::new();
        let mut expressions_per_group: HashMap<GroupId, Vec<WithId<Arc<MemoGroupExpr>>>> = HashMap::new(); // group_id -> list of expressions
        let mut expression_inputs_by_expr: HashMap<GroupId, Vec<(i32, GroupId)>> = HashMap::new();
        let mut expression_scalars_by_expr: HashMap<GroupId, Vec<(i32, GroupId)>> = HashMap::new();
        for batch in db_rows.get("scalar").unwrap_or(&vec![]) {
            for row in 0..batch.num_rows() {
                // TODO rever
                let scalar_id = match parse_int(batch, row, "id") {
                    Ok(id) => id,
                    Err(err) => {
                        trace!("Skipping scalar row {}: {}", row, err);
                        continue;
                    }
                };

                let kind_name = match parse_kind(batch, row, "kind") {
                    Ok(kind) => kind,
                    Err(err) => {
                        trace!("Skipping scalar row {}: {}", row, err);
                        continue;
                    }
                };

                let metadata = match parse_nullablemetadata(batch, row, "metadata") {
                    Ok(value) => value,
                    Err(err) => {
                        trace!("Skipping scalar row {}: {}", row, err);
                        continue;
                    }
                };

                let metadata_str = metadata.as_deref().unwrap_or("");

                let referenced = {
                    let Some(col) = batch
                        .column_by_name("referenced")
                        .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                    else {
                        trace!("Skipping scalar row {}: missing/invalid 'referenced' Boolean column", row);
                        continue;
                    };
                    !col.is_null(row) && col.value(row)
                };

                let parent_scalar = {
                    let Some(col) = batch
                        .column_by_name("parent_scalar")
                        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                    else {
                        trace!("Skipping scalar row {}: missing/invalid 'parent_scalar' Int32 column", row);
                        continue;
                    };

                    if col.is_null(row) {
                        None
                    } else {
                        Some(GroupId(i64::from(col.value(row))))
                    }
                };

                let scalar_position = {
                    let Some(col) = batch
                        .column_by_name("position")
                        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                    else {
                        trace!("Skipping scalar row {}: missing/invalid 'position' Int32 column", row);
                        continue;
                    };

                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row))
                    }
                };

                let Some(parsed_kind) = crate::ir::scalar::ScalarKind::from_kind_and_metadata_string(&kind_name, metadata_str) else {
                    trace!(
                        "Skipping scalar row: could not parse ScalarKind from kind='{}' metadata='{}'",
                        kind_name,
                        metadata_str
                    );
                    continue;
                };

                let mut common = IRCommon::empty();
                
                if scalars_inputs.contains_key(&scalar_id){
                    let mut ordered_inputs = scalars_inputs.get(&scalar_id).unwrap().clone();
                    ordered_inputs.sort_by_key(|(position, _)| *position);
                    common = IRCommon::with_input_scalars_only(
                        ordered_inputs
                            .into_iter()
                            .map(|(_, scalar)| Arc::new(scalar))
                            .collect()
                    );
                }

                let scalar = Arc::new(Scalar {
                    kind: parsed_kind,
                    common: common
                });

                if referenced {
                    referenced_scalars.insert(scalar_id, scalar);
                }
                else
                {
                    let parent = parent_scalar.unwrap_or(GroupId(0));
                    let position = scalar_position.unwrap_or(0);

                    if scalars_inputs.contains_key(&parent) {
                        scalars_inputs
                            .get_mut(&parent)
                            .unwrap()
                            .push((position, (*scalar).clone()));
                    }
                    else {
                        scalars_inputs.insert(parent, vec![(position, (*scalar).clone())]);
                    }
                }
                
                trace!(
                    "parsed scalar row id={} referenced={} kind={} metadata={}",
                    scalar_id.0,
                    referenced,
                    kind_name,
                    metadata_str
                );
            }
        }
        // é suposto termos uma estrutura que suporta cada um dos scalars


        // Parse expression_input table
        for batch in db_rows.get("expression_input").unwrap_or(&vec![]) {
            for row in 0..batch.num_rows() {
                // TODO rever
                let _expr_id = match parse_int(batch, row, "expression_id") {
                    Ok(id) => id,
                    Err(err) => {
                        trace!("Skipping expression_input row {}: {}", row, err);
                        continue;
                    }
                };

                let _input_group = match parse_int(batch, row, "input_group") {
                    Ok(id) => id,
                    Err(err) => {
                        trace!("Skipping expression_input row {}: {}", row, err);
                        continue;
                    }
                };

                let _position = match parse_int(batch, row, "position") {
                    Ok(pos) => pos.0 as i32,
                    Err(err) => {
                        trace!("Skipping expression_input row {}: {}", row, err);
                        continue;
                    }
                };

                expression_inputs_by_expr
                    .entry(_expr_id)
                    .or_default()
                    .push((_position, _input_group));

                trace!("parsed expression_input row expr_id={} input_group={} position={}", _expr_id.0, _input_group.0, _position);
                //TODO: insert into memo structures
            }
        }

        // Parse expression_scalar table
        for batch in db_rows.get("expression_scalar").unwrap_or(&vec![]) {
            for row in 0..batch.num_rows() {
                // TODO rever
                let _expr_id = match parse_int(batch, row, "expression_id") {
                    Ok(id) => id,
                    Err(err) => {
                        trace!("Skipping expression_scalar row {}: {}", row, err);
                        continue;
                    }
                };

                let _scalar_id = match parse_int(batch, row, "scalar_id") {
                    Ok(id) => id,
                    Err(err) => {
                        trace!("Skipping expression_scalar row {}: {}", row, err);
                        continue;
                    }
                };

                let _position = match parse_int(batch, row, "position") {
                    Ok(pos) => pos.0 as i32,
                    Err(err) => {
                        trace!("Skipping expression_scalar row {}: {}", row, err);
                        continue;
                    }
                };

                expression_scalars_by_expr
                    .entry(_expr_id)
                    .or_default()
                    .push((_position, _scalar_id));

                trace!("parsed expression_scalar row expr_id={} scalar_id={} position={}", _expr_id.0, _scalar_id.0, _position);
                //TODO: insert into memo structures
            }
        }


        let make_expression = |metadata: Option<&str>, kind: &str, id: GroupId| -> Option<MemoGroupExpr> {
            let metadata_str = metadata.unwrap_or("");
            let op_kind = OperatorKind::from_kind_and_metadata_string(kind, metadata_str)?;

            let mut input_scalars = expression_scalars_by_expr
                .get(&id)
                .cloned()
                .unwrap_or_default();
            let mut input_groups = expression_inputs_by_expr
                .get(&id)
                .cloned()
                .unwrap_or_default();

            // Keep operator inputs ordered by their DB position.
            input_groups.sort_by_key(|(position, _)| *position);
            input_scalars.sort_by_key(|(position, _)| *position);

            let split = input_groups.len();
            let inputs = input_groups
                .iter()
                .map(|(_, input_group)| *input_group)
                .chain(input_scalars.iter().map(|(_, scalar_id)| *scalar_id))
                .collect::<Vec<_>>()
                .into_boxed_slice();

            Some(MemoGroupExpr::new(op_kind, inputs, split))
        };

        // Parse expression table
        for batch in db_rows.get("expression").unwrap_or(&vec![]) {
            for row in 0..batch.num_rows() {
                // TODO rever
                let expr_id = match parse_int(batch, row, "id") {
                    Ok(id) => id,
                    Err(err) => {
                        trace!("Skipping expression row {}: {}", row, err);
                        continue;
                    }
                };

                let group_id = match parse_int(batch, row, "group_id") {
                    Ok(id) => id,
                    Err(err) => {
                        trace!("Skipping expression row {}: {}", row, err);
                        continue;
                    }
                };

                let kind = match parse_kind(batch, row, "kind") {
                    Ok(k) => k,
                    Err(err) => {
                        trace!("Skipping expression row {}: {}", row, err);
                        continue;
                    }
                };

                let metadata = match parse_nullablemetadata(batch, row, "metadata") {
                    Ok(m) => m,
                    Err(err) => {
                        trace!("Skipping expression row {}: {}", row, err);
                        continue;
                    }
                };

                let _cost = match parse_float(batch, row, "cost") {
                    Ok(c) => c,
                    Err(err) => {
                        trace!("Skipping expression row {}: {}", row, err);
                        continue;
                    }
                };

                let expr = match make_expression(metadata.as_deref(), &kind, expr_id) {
                    Some(expr) => Arc::new(expr),
                    None => {
                        trace!(
                            "Skipping expression row {}: could not build expression from kind='{}' metadata='{}'",
                            row,
                            kind,
                            metadata.as_deref().unwrap_or("")
                        );
                        continue;
                    }
                };

                expressions_per_group
                    .entry(group_id)
                    .or_default()
                    .push(WithId::new(Id::from(expr_id), expr));

                trace!("parsed expression row id={} group_id={} kind={}", expr_id.0, group_id.0, kind);
                //TODO: insert into memo structures
            }
        }


        // Parse group table
        for batch in db_rows.get("group").unwrap_or(&vec![]) {
            for row in 0..batch.num_rows() {
                // TODO rever
                let group_id = match parse_int(batch, row, "id") {
                    Ok(id) => id,
                    Err(err) => {
                        trace!("Skipping group row {}: {}", row, err);
                        continue;
                    }
                };

                let kind = match parse_kind(batch, row, "kind") {
                    Ok(k) => k,
                    Err(err) => {
                        trace!("Skipping group row {}: {}", row, err);
                        continue;
                    }
                };

                let metadata = match parse_nullablemetadata(batch, row, "metadata") {
                    Ok(m) => m,
                    Err(err) => {
                        trace!("Skipping group row {}: {}", row, err);
                        continue;
                    }
                };

                let cardinality = match parse_float(batch, row, "cardinality") {
                    Ok(c) => c,
                    Err(err) => {
                        trace!("Skipping group row {}: {}", row, err);
                        continue;
                    }
                };
                let columns = match parse_nullablemetadata(batch, row, "columns") {
                    Ok(m) => m,
                    Err(err) => {
                        trace!("Skipping group row {}: {}", row, err);
                        continue;
                    }
                    };
                
                
                let expression = match make_expression(metadata.as_deref(), &kind, group_id) {
                    Some(expr) => expr,
                    None => {
                        trace!(
                            "Skipping group row {}: could not build expression from kind='{}' metadata='{}'",
                            row,
                            kind,
                            metadata.as_deref().unwrap_or("")
                        );
                        continue;
                    }
                };

                let first_expr = WithId::new(Id::from(group_id), Arc::new(expression));

                let card: OnceLock<Cardinality> = OnceLock::new();
                card.get_or_init(|| Cardinality::new(cardinality.unwrap_or(0.0).into()));

                let output_cols = OnceLock::new();
                output_cols.get_or_init(|| {
                    Arc::new(ColumnSet::from_iter(
                        columns
                            .unwrap_or("".into())
                            .split(',')
                            .filter(|s| !s.trim().is_empty())
                            .map(|s| {
                                let idx = s.trim().parse::<usize>().expect("invalid column index");
                                Column(idx)
                            }),
                    ))
                });

                let properties = Arc::new(
                    OperatorProperties {
                        cardinality: card,
                        output_columns: output_cols
                    }
                );
                
                let group = MemoGroup::new(first_expr, properties);

                let other_exprs: Vec<WithId<Arc<MemoGroupExpr>>> = expressions_per_group
                    .get(&group_id)
                    .map(|exprs| exprs.to_vec())
                    .unwrap_or_else(Vec::new);
                group.exploration.send_modify(|state| {
                    state.status = Status::Complete; // TODO make sure it doesnt broke optimization pha
                    state.exprs.extend(other_exprs.iter().cloned());

                });
                



                groups_by_id.insert(group_id, group);
                trace!("parsed group row id={} kind={}", group_id.0, kind);
                //TODO: insert into memo structures
            }
        }   

    
        

        // populate main structure
        // 1) Materialize groups staged from DB
        self.groups = groups_by_id.into_iter().collect::<BTreeMap<_, _>>();

        // 2) Rebuild operator dedup + union-find + scalar maps
        let mut max_used_id: i64 = 0;

        for group_id in self.groups.keys() {
            max_used_id = max_used_id.max(group_id.0);
            self.id_to_group_ids.merge(group_id, group_id);
        }

        for group in self.groups.values() {
            let exploration = group.exploration.borrow();
            for expr in &exploration.exprs {
                self.operator_dedup.insert(expr.key().clone(), expr.id());
                max_used_id = max_used_id.max(expr.id().0);
            }
        }

        // Keep only referenced scalars, as requested.
        for (scalar_id, scalar) in referenced_scalars {
            max_used_id = max_used_id.max(scalar_id.0);
            self.scalar_dedup.insert(scalar.clone(), scalar_id);
            self.scalar_id_to_key.insert(scalar_id, scalar);
        }

        // 3) Ensure new ids continue after the largest id loaded from DB.
        let next_id = (max_used_id + 1).max(1);
        self.id_allocator
            .next_id
            .store(next_id, std::sync::atomic::Ordering::Relaxed);

        /*
        println!("Memo after loading from database:");
        println!("{:?}", self);
        println!("Finished loading memo from database.");
        */
    }

    pub fn dump_to_db(&self) -> HashMap<String, Vec<String>> {
        //TODO dump the memo to the database, see memo.sql for the schema
        // let timestamp = SystemTime::now(); for now use default
        let mut db_statements: HashMap<String, Vec<String>> = HashMap::new();
        


        for (scalar_id, scalar) in &self.scalar_id_to_key {
            let kind = scalar.kind.get_kind_string();
            let metadata = scalar.kind.get_metadata_string();
            if metadata.is_empty() {
                db_statements
                    .entry("insert into scalar (id, kind, referenced)".into())
                    .or_insert_with(Vec::new)
                    .push(format!("({}, '{}', true)", scalar_id.0, kind));
            } else {
                db_statements
                    .entry("insert into scalar (id, kind, metadata, referenced)".into())
                    .or_insert_with(Vec::new)
                    .push(format!("({}, '{}', '{}', true)", scalar_id.0, kind, metadata));
            }
            // TODO: loop through the input_scalars of each scalar recursively and insert into scalar adding a reference to the parent
            let mut queue: VecDeque<(Arc<Scalar>, GroupId, i32)> = scalar
                .input_scalars()
                .iter()
                .cloned()
                .enumerate()
                .map(|(position, s)| (s, *scalar_id, position as i32))
                .collect();

            while let Some((scalar, parent_id, position)) = queue.pop_front() {
                let id = GroupId::from(self.id_allocator.next_id());
                let kind = scalar.kind.get_kind_string();
                let metadata = scalar.kind.get_metadata_string();
                if metadata.is_empty() {
                    db_statements
                        .entry("insert into scalar (id, kind, referenced, parent_scalar, position)".into())
                        .or_insert_with(Vec::new)
                        .push(format!("({}, '{}', false, {}, {})", id.0, kind, parent_id.0, position));
                } else {
                    db_statements
                        .entry("insert into scalar (id, kind, metadata, referenced, parent_scalar, position)".into())
                        .or_insert_with(Vec::new)
                        .push(format!("({}, '{}', '{}', false, {}, {})", id.0, kind, metadata, parent_id.0, position));
                }
                     queue.extend(
                    scalar.
                    input_scalars().
                    iter().
                    cloned().
                    enumerate().
                    map(|(child_position, s)| (s, id, child_position as i32)));           
            }
        }

        for (_group_id, group) in &self.groups {

            let group_statements = group.dump_to_db();
            for (stmt, mut values) in group_statements {
                db_statements
                    .entry(stmt)
                    .or_insert_with(Vec::new)
                    .append(&mut values);
            }
        }
        db_statements

    }

    pub fn dump_to_json(&self) -> Value {
        let mut scalar_rows: Vec<Value> = Vec::new();
        let mut group_rows: Vec<Value> = Vec::new();
        let mut expression_rows: Vec<Value> = Vec::new();
        let mut expression_input_rows: Vec<Value> = Vec::new();
        let mut expression_scalar_rows: Vec<Value> = Vec::new();

        let mut max_used_id = 0_i64;
        for scalar_id in self.scalar_id_to_key.keys() {
            max_used_id = max_used_id.max(scalar_id.0);
        }
        for group_id in self.groups.keys() {
            max_used_id = max_used_id.max(group_id.0);
        }
        for group in self.groups.values() {
            let exploration = group.exploration.borrow();
            for expr in &exploration.exprs {
                max_used_id = max_used_id.max(expr.id().0);
            }
        }
        let mut next_generated_id = (max_used_id + 1).max(1);

        let mut root_scalars: Vec<(GroupId, Arc<Scalar>)> = self
            .scalar_id_to_key
            .iter()
            .map(|(id, scalar)| (*id, scalar.clone()))
            .collect();
        root_scalars.sort_by_key(|(id, _)| id.0);

        for (scalar_id, scalar) in root_scalars {
            let mut row = scalar.as_ref().to_json();
            row.as_object_mut()
                .unwrap()
                .insert("id".to_string(), json!(scalar_id.0));
            row.as_object_mut()
                .unwrap()
                .insert("referenced".to_string(), json!(true));
            scalar_rows.push(row);

            let mut queue: VecDeque<(Arc<Scalar>, GroupId, i32)> = scalar
                .input_scalars()
                .iter()
                .cloned()
                .enumerate()
                .map(|(position, s)| (s, scalar_id, position as i32))
                .collect();

            while let Some((scalar, parent_id, position)) = queue.pop_front() {
                let id = GroupId(next_generated_id);
                next_generated_id += 1;

                let mut row = scalar.as_ref().to_json();
                row.as_object_mut()
                    .unwrap()
                    .insert("id".to_string(), json!(id.0));
                row.as_object_mut()
                    .unwrap()
                    .insert("referenced".to_string(), json!(false));
                row.as_object_mut()
                    .unwrap()
                    .insert("parent_scalar".to_string(), json!(parent_id.0));
                row.as_object_mut()
                    .unwrap()
                    .insert("position".to_string(), json!(position));
                scalar_rows.push(row);

                queue.extend(
                    scalar
                        .input_scalars()
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(child_position, s)| (s, id, child_position as i32)),
                );
            }
        }

        for group in self.groups.values() {
            if let Some(group_dump) = group.dump_to_json() {
                group_rows.push(group_dump.group);
                expression_rows.extend(group_dump.expressions);
                expression_input_rows.extend(group_dump.expression_inputs);
                expression_scalar_rows.extend(group_dump.expression_scalars);
            }
        }

        json!({
            "scalar": scalar_rows,
            "group": group_rows,
            "expression": expression_rows,
            "expression_input": expression_input_rows,
            "expression_scalar": expression_scalar_rows,
        })
    }


    /// Adds an operator to the memo table.
    ///
    /// Returns the group id where the operator belongs:
    /// - If it's a new operator: creates a new memo group and returns its id.
    /// - If it already exists: returns txisting group id.
    ///
    /// **Note:** This would not trigger group merges.
    #[instrument(parent = None, skip_all)]
    pub fn insert_new_operator(&mut self, operator: Arc<Operator>) -> Result<GroupId, GroupId> {
        self.insert_operator(operator.clone()).map(|first_expr| {
            trace!(id = %first_expr.id(), "obtain new expr");
            let id = first_expr.id();
            let memo_group = MemoGroup::new(first_expr, operator.properties().clone());
            let res = self.groups.insert(GroupId::from(id), memo_group);
            assert!(res.is_none());
            GroupId::from(id)
        })
    }

    /// Inserts an operator into a specific memo group.
    ///
    /// If the operator is new:
    /// - Adds it as a new expression to the target group
    /// - Returns the new expression
    ///
    /// If the operator already exists in another group:
    /// - Merges that group with the target group
    /// - Returns an error with the target group id.
    ///
    /// **Note:** This may trigger cascading group merges.
    #[instrument(parent = None, skip(self, operator))]
    pub fn insert_operator_into_group(
        &mut self,
        operator: Arc<Operator>,
        into_group_id: GroupId,
    ) -> Result<WithId<Arc<MemoGroupExpr>>, GroupId> {
        let res: Result<WithId<Arc<MemoGroupExpr>>, GroupId> = self.insert_operator(operator.clone());
        let into_group_id = self.id_to_group_ids.find(&into_group_id);
        res.inspect(|expr| {
            info!(id = %expr.id(), "obtain new expr");
            let group = self.groups.get(&into_group_id).unwrap();
            self.id_to_group_ids
                .merge(&into_group_id, &GroupId::from(expr.id()));
            group.exploration.send_modify(|exploration| {
                exploration.exprs.push(expr.clone());
            });
        })
        .map_err(|from_group_id| {
            trace!(
                "got existing group {}, group merges triggered",
                from_group_id
            );
            self.dump();
            self.merge_group(into_group_id, from_group_id);
            trace!("group merging finished");
            self.dump();
            into_group_id
        })
    }

    /// Inserts an operator into the memo table and returns its memo expression.
    ///
    /// This is the core method for adding operators to the memo table. It recursively processes
    /// all input operators and scalars:
    /// - If the operator is new: creates a new memo expression and returns it
    /// - If the operator already exists: returns an error with the existing group id
    ///
    /// **Note:** This method handles recursive insertion of child operators and scalars.
    pub fn insert_operator(
        &mut self,
        operator: Arc<Operator>,
    ) -> Result<WithId<Arc<MemoGroupExpr>>, GroupId> {
        if let OperatorKind::Group(group) = &operator.kind {
            let repr_id = self.id_to_group_ids.find(&group.group_id);
            trace!("inserted group {}", repr_id);
            return Err(repr_id);
        }

        // Split point = len(input_operators)
        let split = operator.input_operators().len();
        let mut inputs = operator
            .input_operators()
            .iter()
            .map(|op| {
                self.insert_operator(op.clone())
                    .map(|first_expr| {
                        let group_id = GroupId::from(first_expr.id());
                        info!(id = %first_expr.id(), "extra group created");
                        let memo_group = MemoGroup::new(first_expr, op.properties().clone());
                        let res = self.groups.insert(group_id, memo_group);
                        assert!(res.is_none());
                        group_id
                    })
                    .unwrap_or_else(|group_id| {
                        trace!("got existing group: {}", group_id);
                        group_id
                    })
            })
            .collect_vec();

        inputs.extend(
            operator
                .input_scalars()
                .iter()
                .map(|s| self.insert_scalar(s.clone()).unwrap_or_else(|id| id)),
        );

        let group_expr = Arc::new(MemoGroupExpr::new(
            operator.kind.clone(),
            inputs.into_boxed_slice(),
            split,
        ));

        use std::collections::hash_map::Entry;
        match self.operator_dedup.entry(group_expr.clone()) {
            Entry::Occupied(occupied) => {
                let id = occupied.get();
                Err(self.id_to_group_ids.find(&GroupId::from(*id)))
            }
            Entry::Vacant(vacant) => {
                let id = self.id_allocator.next_id();
                vacant.insert(id);
                let key_with_id = WithId::new(id, group_expr);
                self.infer_properties(operator);
                Ok(key_with_id)
            }
        }
    }

    fn infer_properties(&self, operator: Arc<Operator>) {
        operator.get_property::<Cardinality>(&self.ctx);
        operator.get_property::<OutputColumns>(&self.ctx);
    }

    /// Inserts a scalar into the memo table's scalar deduplication map.
    ///
    /// Handles scalar deduplication and group id assignment:
    /// - If the scalar is new: creates a new group id and returns it
    /// - If the scalar already exists: returns an error with the existing group id
    fn insert_scalar(&mut self, scalar: Arc<Scalar>) -> Result<GroupId, GroupId> {
        use std::collections::hash_map::Entry;
        match self.scalar_dedup.entry(scalar.clone()) {
            Entry::Occupied(occupied) => {
                let id = occupied.get();
                assert!(self.scalar_id_to_key.contains_key(id));
                trace!("got existing scalar with {:?}", id);
                Err(*id)
            }
            Entry::Vacant(vacant) => {
                let group_id = GroupId::from(self.id_allocator.next_id());
                vacant.insert(group_id);
                self.scalar_id_to_key.insert(group_id, scalar);
                info!(%group_id, "obtained new scalar expr");
                Ok(group_id)
            }
        }
    }

    /// Retrieves a scalar value by its group id.
    ///
    /// Returns the scalar associated with the given group id:
    /// - If the group id corresponds to a scalar: returns `Some(scalar)`
    /// - If the group id is not found or is not a scalar: returns `None`
    pub fn get_scalar(&self, group_id: &GroupId) -> Option<Arc<Scalar>> {
        self.scalar_id_to_key.get(group_id).cloned()
    }

    pub fn get_operator_one_level(
        &self,
        group_expr: &MemoGroupExpr,
        properties: Arc<OperatorProperties>,
        group_id: GroupId,
    ) -> Arc<Operator> {
        let input_scalars = group_expr
            .input_scalars()
            .iter()
            .map(|group_id| self.get_scalar(group_id).unwrap())
            .collect();

        let input_operators = group_expr
            .input_operators()
            .iter()
            .map(|group_id| {
                let memo_group = self.get_memo_group(group_id);

                Group::new(
                    memo_group.group_id,
                    memo_group.exploration.borrow().properties.clone(),
                )
                .into_operator()
            })
            .collect();

        let common = IRCommon::new_with_properties(input_operators, input_scalars, properties);
        let group_id = Some(group_id);

        Arc::new(Operator {
            group_id,
            kind: group_expr.meta.clone(),
            common,
        })
    }

    /// Gets a shared reference to the memo group corresponding to a group id.
    ///
    /// Uses the union-find structure to resolve the representative group id and returns
    /// the associated memo group:
    /// - Finds the representative group id using union-find
    /// - Returns a reference to the corresponding memo group
    pub fn get_memo_group(&self, group_id: &GroupId) -> &MemoGroup {
        let repr_group_id = self.id_to_group_ids.find(group_id);
        self.groups.get(&repr_group_id).unwrap()
    }

    /// Gets a mutable reference to the memo group corresponding to a group id.
    ///
    /// Uses the union-find structure to resolve the representative group id and returns
    /// the associated memo group:
    /// - Finds the representative group id using union-find
    /// - Returns a reference to the corresponding memo group
    pub fn get_memo_group_mut(&mut self, group_id: &GroupId) -> &mut MemoGroup {
        let repr_group_id = self.id_to_group_ids.find(group_id);
        self.groups.get_mut(&repr_group_id).unwrap()
    }

    /// Merges two memo groups into one, combining their expressions.
    ///
    /// Transfers all expressions from the source group to the target group and updates
    /// all references throughout the memo table:
    /// - Moves expressions from `from_group_id` to `into_group_id`
    /// - Updates operator deduplication map with merged expressions
    /// - Handles cascading group merges when expressions become duplicated
    /// - Uses union-find to track group equivalences
    ///
    /// **Note:** This operation may trigger additional group merges recursively.
    fn merge_group(&mut self, into_group_id: GroupId, from_group_id: GroupId) {
        info!("merging {} <- {}", into_group_id, from_group_id);
        if into_group_id == from_group_id {
            return;
        }

        let from_group = self.groups.remove(&from_group_id).unwrap();
        let into_group = self.groups.get(&into_group_id).unwrap();

        let mut from_group_exprs = Vec::new();
        from_group.exploration.send_modify(|state| {
            std::mem::swap(&mut state.exprs, &mut from_group_exprs);
            state.status = Status::Obsolete;
        });

        // After this point all receiver will notice the sender got dropped.
        drop(from_group);

        // TODO(yuchen): What about Optimization?
        // As of writing, we do not merge optimization entries, meaning that
        // existing optimization progress for `from_group` is lost.
        // Might be a simple hash map merge and resolve.
        into_group.exploration.send_modify(|state| {
            state.exprs.extend(from_group_exprs);
        });

        self.id_to_group_ids.merge(&into_group_id, &from_group_id);

        let mut pending_group_merges = Vec::new();
        for (group_id, group) in self.groups.iter_mut() {
            // design: group exprs are cloned so we don't hold the lock on `exploration` for too long.
            let mut group_exprs = group.exploration.borrow().exprs.clone();
            group_exprs.iter_mut().for_each(|expr| {
                let input_groups = expr.key.input_operators();

                if input_groups.contains(&from_group_id) {
                    let inputs_after_merge = input_groups
                        .iter()
                        .map(|group_id| {
                            if group_id.eq(&from_group_id) {
                                &into_group_id
                            } else {
                                group_id
                            }
                        })
                        .chain(expr.key.input_scalars())
                        .cloned()
                        .collect::<Box<[GroupId]>>();
                    let new_key = Arc::new(expr.key.clone_with_inputs(inputs_after_merge));
                    self.operator_dedup.remove(&expr.key);

                    use std::collections::hash_map::Entry;
                    match self.operator_dedup.entry(new_key.clone()) {
                        Entry::Occupied(occupied) => {
                            let dup_expr_id = *occupied.get();
                            let dup_group_id =
                                self.id_to_group_ids.find(&GroupId::from(dup_expr_id));
                            if dup_group_id != *group_id {
                                pending_group_merges.push((dup_group_id, *group_id));
                            }
                            *expr = WithId::new(dup_expr_id, occupied.key().clone());
                        }
                        Entry::Vacant(vacant) => {
                            vacant.insert(expr.id());
                            *expr = WithId::new(expr.id(), new_key);
                        }
                    }
                }
            });

            // Deduplication is needed so we don't have duplicated expressions in a memo group.
            group_exprs.dedup_by_key(|key| key.id());
            group.exploration.send_modify(|state| {
                std::mem::swap(&mut state.exprs, &mut group_exprs);
            });
            group_exprs.clear();
        }
        info!(?pending_group_merges);
        for (into_group_id, from_group_id) in pending_group_merges {
            let into_group_id = self.id_to_group_ids.find(&into_group_id);
            let from_group_id = self.id_to_group_ids.find(&from_group_id);
            self.merge_group(into_group_id, from_group_id);
        }
    }

    /// Prints a human-readable representation of the memo table contents.
    ///
    /// Outputs all memo groups and their expressions to stdout for debugging purposes:
    /// - Shows group ids and number of expressions per group
    /// - Lists all expressions within each group with their ids and details
    ///
    /// This method is primarily intended for debugging and testing.
    pub fn dump(&self) {
        let option = ExplainOption::default();
        info!("======== MEMO DUMP BEGIN ========");
        info!("\n[operators]");
        info!("group_ids = {:?}", self.groups.keys());
        info!("total_group_count = {}", self.groups.keys().len());
        info!("total_operator_count = {}", self.operator_dedup.len());
        for (group_id, group) in &self.groups {
            let state = group.exploration.borrow();
            assert_eq!(group_id, &group.group_id);
            info!("\n[operators.{group_id}]");
            info!("num_exprs = {}", state.exprs.len());
            info!(
                "outputcolumns = {}",
                state
                    .properties
                    .output_columns
                    .get()
                    .map(|x| format!("{x:?}"))
                    .unwrap_or("?".to_string()),
            );
            info!(
                "cardinality = {}",
                state
                    .properties
                    .cardinality
                    .get()
                    .map(|x| format!("{:.2}", x.as_f64()))
                    .unwrap_or("?".to_string()),
            );

            for expr in state.exprs.iter() {
                info!("{} = {:?}", expr.id(), expr.key());
            }

            for (required, tx) in group.optimizations.iter() {
                info!("\n[operators.{group_id}.required = {required}]");
                let state = tx.borrow();
                let best_index = state
                    .costed_exprs
                    .iter()
                    .enumerate()
                    .min_by(|(_, x), (_, y)| {
                        x.total_cost.as_f64().total_cmp(&y.total_cost.as_f64())
                    })
                    .map(|(i, _)| i);
                for (i, costed) in state.costed_exprs.iter().enumerate() {
                    let inputs = costed
                        .input_requirements
                        .iter()
                        .zip(costed.group_expr.key().input_operators())
                        .map(|((required, index), group_id)| {
                            format!("\"o#{index}@{group_id}\": {required}")
                        })
                        .join(", ");
                    let opt_desc = best_index
                        .filter(|best_index| i.eq(best_index))
                        .map(|best_index| format!("o#{best_index} (best)"))
                        .unwrap_or_else(|| format!("o#{i}{:>7}", ""));
                    info!(
                        "{opt_desc} = {{ id={}, total = {}, operation = {} inputs: {{{}}} }}",
                        costed.group_expr.id(),
                        costed.total_cost,
                        costed.operator_cost,
                        inputs
                    );
                }
            }
        }
        info!("\n[scalars]");
        for (scalar_id, scalar) in &self.scalar_id_to_key {
            let s = scalar.explain(&self.ctx, &option).to_one_line_string(true);
            info!("{scalar_id} = \"{s}\"")
        }
        info!("======== MEMO DUMP END ==========");
    }
}

struct GroupJsonDump {
    group: Value,
    expressions: Vec<Value>,
    expression_inputs: Vec<Value>,
    expression_scalars: Vec<Value>,
}

pub struct IdAllocator {
    next_id: AtomicI64,
}

impl Default for IdAllocator {
    fn default() -> Self {
        Self {
            next_id: AtomicI64::new(1),
        }
    }
}

impl IdAllocator {
    pub fn next_id(&self) -> Id {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Id(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Status {
    #[default]
    NotStarted,
    InProgress,
    Complete,
    Obsolete,
}

#[derive(Clone)]
pub struct Exploration {
    pub exprs: Vec<WithId<Arc<MemoGroupExpr>>>,
    pub properties: Arc<OperatorProperties>,
    pub status: Status,
}

impl std::fmt::Debug for Exploration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let exprs = self
            .exprs
            .iter()
            .map(|e| e.key().clone())
            .collect::<Vec<_>>();

        f.debug_struct("Exploration")
            .field("exprs", &exprs)
            .field("properties", &self.properties)
            .field("status", &self.status)
            .finish()
    }
}

impl Exploration {
    pub fn new(
        first_expr: WithId<Arc<MemoGroupExpr>>,
        properties: Arc<OperatorProperties>,
    ) -> Self {
        Self {
            exprs: vec![first_expr],
            status: Status::NotStarted,
            properties,
        }
    }

    fn dump_to_json(&self, group_id: GroupId) -> Option<GroupJsonDump> {
        let first_expr = self.exprs.first()?;

        let card = self
            .properties
            .cardinality
            .get()
            .map(|c| c.as_f64())
            .unwrap_or(-1.0);
        let columns = self
            .properties
            .output_columns
            .get()
            .map(|cols| cols.iter().map(|c| c.0.to_string()).join(","))
            .unwrap_or("?".to_string());

        let mut group = json!({
            "id": group_id.0,
            "kind": first_expr.key().kind().get_kind_string(),
            "cardinality": card,
            "columns": columns,
        });
        let metadata = first_expr.key().kind().get_metadata_string();
        if !metadata.is_empty() {
            group["metadata"] = json!(metadata);
        }

        let mut expressions = Vec::new();
        for expr in self.exprs.iter().skip(1) {
            let mut expr_row = json!({
                "id": expr.id().0,
                "group_id": group_id.0,
                "kind": expr.key().kind().get_kind_string(),
            });
            let metadata = expr.key().kind().get_metadata_string();
            if !metadata.is_empty() {
                expr_row["metadata"] = json!(metadata);
            }
            expressions.push(expr_row);
        }

        let mut expression_inputs = Vec::new();
        let mut expression_scalars = Vec::new();
        for expr in &self.exprs {
            for (position, input_group) in expr.key().input_operators().iter().enumerate() {
                expression_inputs.push(json!({
                    "expression_id": expr.id().0,
                    "input_group": input_group.0,
                    "position": position as i32,
                }));
            }

            for (position, input_scalar) in expr.key().input_scalars().iter().enumerate() {
                expression_scalars.push(json!({
                    "expression_id": expr.id().0,
                    "scalar_id": input_scalar.0,
                    "position": position as i32,
                }));
            }
        }

        Some(GroupJsonDump {
            group,
            expressions,
            expression_inputs,
            expression_scalars,
        })
    }
}
#[derive(Clone)]
pub struct CostedExpr {
    pub group_expr: WithId<Arc<MemoGroupExpr>>,
    pub operator_cost: Cost,
    pub total_cost: Cost,
    /// The input requirements and the index of the costed expressions for the inputs.
    pub input_requirements: Arc<[(Arc<Required>, usize)]>,
}

impl std::fmt::Debug for CostedExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CostedExpr")
            .field("group_expr", &self.group_expr)
            .field("operator_cost", &self.operator_cost)
            .field("total_cost", &self.total_cost)
            .field("input_requirements", &self.input_requirements)
            .finish()
    }
}
        
    

impl CostedExpr {
    pub fn new(
        group_expr: WithId<Arc<MemoGroupExpr>>,
        operator_cost: Cost,
        total_cost: Cost,
        input_requirements: Arc<[(Arc<Required>, usize)]>,
    ) -> Self {
        Self {
            group_expr,
            operator_cost,
            total_cost,
            input_requirements,
        }
    }
}

#[derive(Default, Clone)]
pub struct Optimization {
    pub costed_exprs: Vec<CostedExpr>,
    pub enforcers: Vec<Arc<MemoGroupExpr>>,
    pub status: Status,
}


impl std::fmt::Debug for Optimization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Optimization")
            .field("costed_exprs", &self.costed_exprs)
            .field("enforcers", &self.enforcers)
            .field("status", &self.status)
            .finish()
    }
}

pub struct MemoGroup {
    pub group_id: GroupId,
    pub exploration: watch::Sender<Exploration>,
    pub optimizations: HashMap<Arc<Required>, watch::Sender<Optimization>>,
}

impl MemoGroup {

    fn dump_to_json(&self) -> Option<GroupJsonDump> {
        let exploration = self.exploration.borrow();
        exploration.dump_to_json(self.group_id)
    }


    fn dump_to_db(&self) -> HashMap<String, Vec<String>> 
    {
        let mut res: HashMap<String, Vec<String>> = HashMap::new();
        let exploration = self.exploration.borrow();
        if let Some(first_expr) = exploration.exprs.first() {
            let card = exploration.properties.cardinality.get().map(|c| c.as_f64()).unwrap_or(-1.0);
            let columns = exploration
                .properties
                .output_columns
                .get()
                .map(|cols| cols.iter().map(|c| c.0.to_string()).join(","))
                .unwrap_or("?".to_string());
            let kind = first_expr.key().kind().get_kind_string();
            let metadata = first_expr.key().kind().get_metadata_string();
            if metadata.is_empty() {
                res.entry("insert into group (id,kind,cardinality,columns)".to_string())
                    .or_insert_with(Vec::new)
                    .push(format!("({},'{}',{},'{}')", self.group_id.0, kind, card, columns));
            }
            else{

                res.entry("insert into group (id,kind,metadata,cardinality,columns)".to_string())
                    .or_insert_with(Vec::new)
                    .push(format!("({},'{}','{}',{},'{}')", self.group_id.0, kind, metadata, card, columns));

            }
            let mut position = 0;
            for input_group in first_expr.key.input_operators() {
                res.entry("insert into expression_input (expression_id, input_group, position)".to_string())
                    .or_insert_with(Vec::new)
                    .push(format!("({}, {}, {})", first_expr.id().0, input_group.0, position));
                position += 1;
            }

            let mut position = 0;
            for input_scalar in first_expr.key.input_scalars() {

                res.entry("insert into expression_scalar (expression_id, scalar_id, position)".to_string())
                    .or_insert_with(Vec::new)
                    .push(format!("({}, {}, {})", first_expr.id().0, input_scalar.0, position));
                position += 1;
            }
        }

        for expr in exploration.exprs.iter().skip(1) {
            let kind = expr.key().kind().get_kind_string();
            let metadata = expr.key().kind().get_metadata_string();
            if metadata.is_empty() {
                res.entry("insert into expression (id, group_id, kind)".to_string())
                    .or_insert_with(Vec::new)
                    .push(format!("({}, {}, '{}')", expr.id().0, self.group_id.0, kind));
            }
            else{

                res.entry("insert into expression (id, group_id, kind, metadata)".to_string())
                    .or_insert_with(Vec::new)
                    .push(format!("({}, {}, '{}', '{}')", expr.id().0, self.group_id.0, kind, metadata));
            }
            let mut position = 0;
            for input_group in expr.key.input_operators() {

                res.entry("insert into expression_input (expression_id, input_group, position)".to_string())
                    .or_insert_with(Vec::new)
                    .push(format!("({}, {}, {})", expr.id().0, input_group.0, position));
                position += 1;
            }
            position = 0;
            for input_scalar in expr.key.input_scalars() {
                res.entry("insert into expression_scalar (expression_id, scalar_id, position)".to_string())
                    .or_insert_with(Vec::new)
                    .push(format!("({}, {}, {})", expr.id().0, input_scalar.0, position));
                position += 1;
            }
        }           
        res
    }
}


impl std::fmt::Debug for MemoGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MemoGroup {{")?;
        writeln!(f, "  Group ID: {}", self.group_id)?;
        
        writeln!(f, "  \n  Exploration:")?;
        let exploration = self.exploration.borrow();
        writeln!(f, "    Status: {:?}", exploration.status)?;
        writeln!(f, "    Expressions: [")?;
        for expr in &exploration.exprs {
            writeln!(f, "      - {} {:?}", expr.id(), expr.key())?;
        }
        writeln!(f, "    ]")?;
        writeln!(f, "    Properties: {:?}", exploration.properties)?;
        
        if !self.optimizations.is_empty() {
            writeln!(f, "\n  Optimizations:")?;
            for (required, optimization) in &self.optimizations {
                let opt = optimization.borrow();
                writeln!(f, "    Required: {}", required)?;
                writeln!(f, "      Status: {:?}", opt.status)?;
                writeln!(f, "      Costed Exprs: [")?;
                for costed in &opt.costed_exprs {
                    writeln!(f, "        - {} op_cost={:?} total_cost={:?}",
                             costed.group_expr.id(),
                             costed.operator_cost,
                             costed.total_cost)?;
                }
                writeln!(f, "      ]")?;
                if !opt.enforcers.is_empty() {
                    writeln!(f, "      Enforcers: {:?}", opt.enforcers)?;
                }
            }
        }
        
        writeln!(f, "}}")
    }
}

impl MemoGroup {
    /// Creates a new memo group with its first expression and properties.
    pub fn new(
        first_expr: WithId<Arc<MemoGroupExpr>>,
        properties: Arc<OperatorProperties>,
    ) -> Self {
        let group_id = GroupId::from(first_expr.id());
        let exploration = watch::Sender::new(Exploration::new(first_expr, properties));
        Self {
            group_id,
            exploration,
            optimizations: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::ir::{
        Column, IRContext, builder::*, explain::quick_explain, operator::join::JoinType,
    };

    #[test]
    fn insert_scalar() {
        let mut memo = MemoTable::new(IRContext::with_empty_magic());
        let scalar = column_ref(Column(1)).eq(int32(799));
        let scalar_from_clone = scalar.clone();
        let scalar_dup = column_ref(Column(1)).eq(int32(799));
        let id = memo.insert_scalar(scalar).unwrap();
        let res = memo.insert_scalar(scalar_from_clone);
        assert_eq!(Err(id), res);
        let res = memo.insert_scalar(scalar_dup);
        assert_eq!(Err(id), res);
    }

    #[test]
    fn insert_new_operator() {
        let ctx = IRContext::with_empty_magic();
        let mut memo = MemoTable::new(ctx.clone());
        let join = ctx.mock_scan(1, vec![1], 0.).logical_join(
            ctx.mock_scan(2, vec![2], 0.),
            boolean(true),
            JoinType::Inner,
        );

        let join_dup = ctx.mock_scan(1, vec![1], 0.).logical_join(
            ctx.mock_scan(2, vec![2], 0.),
            boolean(true),
            JoinType::Inner,
        );

        let group_id = memo.insert_new_operator(join.clone()).unwrap();
        let res = memo.insert_new_operator(join);
        assert_eq!(Err(group_id), res);
        let res = memo.insert_new_operator(join_dup);
        assert_eq!(Err(group_id), res);
    }

    #[test]
    fn insert_operator_into_group() {
        let ctx = IRContext::with_empty_magic();
        let mut memo = MemoTable::new(ctx.clone());
        let join = ctx.mock_scan(1, vec![1], 0.).logical_join(
            ctx.mock_scan(2, vec![2], 0.),
            boolean(true),
            JoinType::Inner,
        );
        let group_id = memo.insert_new_operator(join).unwrap();

        let join_commuted = ctx.mock_scan(2, vec![2], 0.).logical_join(
            ctx.mock_scan(1, vec![1], 0.),
            boolean(true),
            JoinType::Inner,
        );
        let res = memo.insert_operator_into_group(join_commuted.clone(), group_id);
        assert!(res.is_ok());

        let res = memo.insert_operator_into_group(join_commuted, group_id);
        assert!(res.is_err());

        let group = memo.get_memo_group(&group_id);
        assert_eq!(2, group.exploration.borrow().exprs.len());
    }

    #[test]
    fn parent_group_merge() {
        let ctx = IRContext::with_empty_magic();
        let mut memo = MemoTable::new(ctx.clone());

        let m1 = ctx.mock_scan(1, vec![1], 0.);
        let m1_alias = ctx.mock_scan(2, vec![1], 0.);

        let g1 = memo
            .insert_new_operator(m1.clone().logical_select(boolean(true)))
            .unwrap();

        let g2 = memo
            .insert_new_operator(m1_alias.clone().logical_select(boolean(true)))
            .unwrap();

        let m1_group_id = memo.insert_operator(m1.clone()).unwrap_err();
        let res = memo.insert_operator_into_group(m1_alias, m1_group_id);
        assert_eq!(Err(m1_group_id), res);

        assert_eq!(
            memo.id_to_group_ids.find(&g1),
            memo.id_to_group_ids.find(&g2)
        );

        let g1_group = memo.get_memo_group(&g1);
        let g2_group = memo.get_memo_group(&g2);
        assert_eq!(g1_group.group_id, g2_group.group_id);
    }

    #[test]
    #[tracing_test::traced_test]
    fn cascading_group_merges() {
        let ctx = IRContext::with_empty_magic();
        let mut memo = MemoTable::new(ctx.clone());

        let m1 = ctx.mock_scan(1, vec![1], 0.);
        trace!("\n{}", quick_explain(&m1, &memo.ctx));
        let m1_alias = ctx.mock_scan(2, vec![1], 0.);

        let g1 = memo
            .insert_new_operator(
                m1.clone()
                    .logical_select(boolean(true))
                    .logical_select(boolean(true)),
            )
            .unwrap();

        let g2 = memo
            .insert_new_operator(
                m1_alias
                    .clone()
                    .logical_select(boolean(true))
                    .logical_select(boolean(true)),
            )
            .unwrap();

        let m1_group_id = memo.insert_operator(m1.clone()).unwrap_err();
        let res = memo.insert_operator_into_group(m1_alias, m1_group_id);
        assert_eq!(Err(m1_group_id), res);

        assert_eq!(
            memo.id_to_group_ids.find(&g1),
            memo.id_to_group_ids.find(&g2)
        );

        let g1_group = memo.get_memo_group(&g1);
        let g2_group = memo.get_memo_group(&g2);
        assert_eq!(g1_group.group_id, g2_group.group_id);
        assert_eq!(1, g1_group.exploration.borrow().exprs.len());
    }

    #[test]
    fn insert_partial_binding() {
        let ctx = IRContext::with_empty_magic();
        let mut memo = MemoTable::new(ctx.clone());

        let m1 = ctx.mock_scan(1, vec![1], 0.);
        let m1_alias = ctx.mock_scan(2, vec![1], 0.);
        memo.insert_new_operator(
            m1.clone()
                .logical_select(boolean(true))
                .logical_select(boolean(true)),
        )
        .unwrap();

        memo.insert_new_operator(
            m1_alias
                .clone()
                .logical_select(boolean(true))
                .logical_select(boolean(true)),
        )
        .unwrap();

        let m1_group_id = memo.insert_operator(m1.clone()).unwrap_err();

        let properties = memo
            .get_memo_group(&m1_group_id)
            .exploration
            .borrow()
            .properties
            .clone();

        let m1_select_binding = group(m1_group_id, properties).logical_select(boolean(true));

        let into_group_id = memo
            .insert_operator(m1_alias.clone().logical_select(boolean(true)))
            .unwrap_err();

        let res = memo.insert_operator_into_group(m1_select_binding, into_group_id);
        assert_eq!(Err(into_group_id), res);
    }
}
