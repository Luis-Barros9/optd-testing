CREATE EXTERNAL TABLE group
STORED AS JSON
LOCATION 'data/group.ndjson';
    
CREATE EXTERNAL TABLE scalar
STORED AS JSON
LOCATION 'data/scalar.ndjson';
CREATE EXTERNAL TABLE expression
STORED AS JSON
LOCATION 'data/expression.ndjson';

CREATE EXTERNAL TABLE expression_input
STORED AS JSON
LOCATION 'data/expressioninput.ndjson';

CREATE EXTERNAL TABLE expression_scalar
STORED AS JSON
LOCATION 'data/expressionscalar.ndjson';
