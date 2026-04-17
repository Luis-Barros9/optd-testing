insert into group (id,kind,metadata,cardinality,columns) VALUES
  (1,'LogicalGet','{ source: 1, first_column: 0, projections: [0, 6] }',15000,'6,0'),
  (6,'LogicalGet','{ source: 2, first_column: 8, projections: [0, 1, 4] }',150000,'8,9,12'),
  (10,'LogicalJoin','{ join_type: Inner }',9000000,'9,0,12,8'),
  (13,'LogicalGet','{ source: 3, first_column: 17, projections: [0, 1] }',600572,'17,18'),
  (17,'LogicalJoin','{ join_type: Inner }',216205920000.00003,'18,8,9,17,12');
insert into expression_input (expression_id, input_group, position) VALUES
  (3, 1, 0),
  (5, 3, 0),
  (8, 6, 0),
  (10, 5, 0),
  (10, 8, 1),
  (12, 10, 0),
  (15, 13, 0),
  (17, 12, 0),
  (17, 15, 1),
  (19, 17, 0);
insert into group (id,kind,cardinality,columns) VALUES
  (3,'LogicalSelect',1500,'0,6'),
  (5,'LogicalProject',1500,'0'),
  (8,'LogicalSelect',15000,'9,8,12'),
  (12,'LogicalProject',9000000,'12,8,9'),
  (15,'LogicalSelect',60057.200000000004,'18,17'),
  (19,'LogicalProject',216205920000.00003,'8,9,12,18');
insert into expression_scalar (expression_id, scalar_id, position) VALUES
  (3, 2, 0),
  (5, 4, 0),
  (8, 7, 0),
  (10, 9, 0),
  (12, 11, 0),
  (15, 14, 0),
  (17, 16, 0),
  (19, 18, 0);
insert into scalar (id, kind, referenced) VALUES
  (4, 'List', true),
  (18, 'List', true),
  (11, 'List', true);
insert into scalar (id, kind, metadata, referenced, parent_scalar, position) VALUES
  (20, 'ColumnAssign', '{ column: 0 }', false, 4, 0),
  (21, 'ColumnRef', '{ column: 0 }', false, 20, 0),
  (22, 'BinaryOp', '{ op_kind: = }', false, 16, 0),
  (23, 'ColumnRef', '{ column: 8 }', false, 22, 0),
  (24, 'ColumnRef', '{ column: 17 }', false, 22, 1),
  (25, 'ColumnAssign', '{ column: 8 }', false, 18, 0),
  (26, 'ColumnAssign', '{ column: 12 }', false, 18, 1),
  (27, 'ColumnAssign', '{ column: 9 }', false, 18, 2),
  (28, 'ColumnAssign', '{ column: 18 }', false, 18, 3),
  (29, 'ColumnRef', '{ column: 8 }', false, 25, 0),
  (30, 'ColumnRef', '{ column: 12 }', false, 26, 0),
  (31, 'ColumnRef', '{ column: 9 }', false, 27, 0),
  (32, 'ColumnRef', '{ column: 18 }', false, 28, 0),
  (33, 'BinaryOp', '{ op_kind: = }', false, 9, 0),
  (34, 'ColumnRef', '{ column: 0 }', false, 33, 0),
  (35, 'ColumnRef', '{ column: 9 }', false, 33, 1),
  (36, 'ColumnAssign', '{ column: 8 }', false, 11, 0),
  (37, 'ColumnAssign', '{ column: 9 }', false, 11, 1),
  (38, 'ColumnAssign', '{ column: 12 }', false, 11, 2),
  (39, 'ColumnRef', '{ column: 8 }', false, 36, 0),
  (40, 'ColumnRef', '{ column: 9 }', false, 37, 0),
  (41, 'ColumnRef', '{ column: 12 }', false, 38, 0),
  (42, 'ColumnRef', '{ column: 6 }', false, 2, 0),
  (43, 'Literal', '{ value: BUILDING::utf8_view }', false, 2, 1),
  (44, 'ColumnRef', '{ column: 8 }', false, 7, 0),
  (45, 'Literal', '{ value: 1500::bigint }', false, 7, 1),
  (46, 'ColumnRef', '{ column: 17 }', false, 14, 0),
  (47, 'Literal', '{ value: 1500::bigint }', false, 14, 1);
insert into scalar (id, kind, metadata, referenced) VALUES
  (16, 'NaryOp', '{ op_kind: AND }', true),
  (9, 'NaryOp', '{ op_kind: AND }', true),
  (2, 'BinaryOp', '{ op_kind: = }', true),
  (7, 'BinaryOp', '{ op_kind: > }', true),
  (14, 'BinaryOp', '{ op_kind: > }', true);