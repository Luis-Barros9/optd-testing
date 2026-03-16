-- Questões por causa do resultado dos prints do memo
--  Como considerar scalars?no optd esta como um grupo mas nao entendi muito bem
-- inputs por grupo ou expressão?  ex: em output.txt G4 id#22 como pode ser input dele mesmo, 
-- acrescentar uma tabela para guardar informação sobre o estado do schema? registar alterações no mesmo
-- primeira fase: dump para uma bd e restaurar a partir da mesma, avaliar tempo de otimização, e tempo a ler da bd.

-- a base de dados não está a suportar forgein keys 
-- Error: Error during planning: Foreign key constraints are not currently supported


create table group (
    id int primary key,
    kind varchar(50), -- tipo de expressão, ex: LogicalScan, LogicalJoin, etc
    metadata varchar(255),
    created_at timestamp default CURRENT_TIMESTAMP, -- possivelmente usado para detetar se precisa reotimizaçao
    cardinality float,
    columns varchar(255) -- ajustar futuramente para nao ser string (no codigo hashset de collumns, unsigned integers,car )
);

-- possivelmente adicionar aqui 2 campos para inputs que possam ser nullables
create table expression (
    id int primary key,
    group_id int,
    kind varchar(50), -- tipo de expressão, ex: LogicalScan, LogicalJoin, etc
    metadata varchar(255),
    cost float
    --foreign key (group_id) references group(id)
);

-- perguntar sobre como uma expressão de um grupo pode ter como input o mesmo grupo
create table expression_input (
    expression_id int,
    input_group int,
    position int, -- posição do input, para manter a ordem dos inputs
    primary key (expression_id, input_group)
);


--ainda não percebi muito bem como inserir os scalars
create table scalar (
    id int primary key,
    kind varchar(50), -- tipo de scalar, ex: List(ListMetadata) ver depois outra forma que ocupe menos espaço
    metadata varchar(255),
    referenced boolean, -- se o scalar é referenciado por alguma expressão, pode ser referenciado por um scalar apenas, provavelmente basta ver se a foreign key é null ou não
    parent_scalar int null -- foreign key references scalar(id)
);






create table expression_scalar (
    expression_id int, -- foreign key references expression(id),
    scalar_id int, -- foreign key references scalar(id),
    primary key (expression_id, scalar_id)
);



-- pelo que vi no memo final nenhum scalar tem operators como inputs, ignorar para
-- create table scalar_input_operators (
--    input_expression int foreign key references expression(id),
--    scalar_id int foreign key references scalar(id),
--    primary key (input_expression, scalar_id)
--);




