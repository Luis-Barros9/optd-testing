-- Example 2.3.4 from Ding et al.

drop table if exists memo;

create table memo (
    gid int,
    op varchar,
    lchild int, rchild int,
    cols int[], exp int[]);

insert into memo values
  (0, 'noop', 0, 0, array[]::int[], array[]::int[]) ,
  (1, 'scan', 0, 0, array[0,6], array[]::int[]),
  (3, 'filter', 1, 0, array[0], array[0]),
  (6, 'scan', 0, 0, array[8,12,15,9], array[]::int[]),
  (8, 'filter', 6, 0, array[8,9,15,12], array[8,9,15,12]),
  (10, 'join', 3, 8, array[12,8,9,15,0], array[0,9]),
  (13, 'scan', 0, 0, array[27,22,17,23], array[]::int[]),
  (15, 'filter', 13, 0, array[17,22,23], array[17,22,23]),
  (19, 'join', 10, 15, array[8,12,22,17,23,15], array[8,17]);
  
/*
insert into memo values
    (0, 'noop', 0, 0, array[]::int[], array[]::int[]),
    (1, 'scan', 0, 0, array[101,102], array[]::int[]),
    (2, 'scan', 0, 0, array[201,202], array[]::int[]),
    (3, 'scan', 0, 0, array[301,302], array[]::int[]),
    (4, 'join', 1, 2, array[101,102,201,202], array[101,201]),
    (5, 'join', 4, 3, array[101,102,201,202,301,302], array[202,302]);

-- (6, 'filt', 5, 0, array[101,102,201,202,301,302], array[202]),
-- (7, 'proj', 7, 6, array[101,702], array[101,201,301]);
*/


-- for unnesting record from here:
-- https://www.postgresql.org/message-id/CAHyXU0zQMmG-cV5b27ZgyJ9xKpUjGG51gf0UROy9TkioTMe3XQ@mail.gmail.com
drop type if exists memo_t;
create type memo_t as (
    gid int,
    op varchar,
    lchild int, rchild int,
    cols int[], exp int[]
);

-- set ops for arrays, ordered for presentation
CREATE OR REPLACE FUNCTION aunion(a ANYARRAY, b ANYARRAY) RETURNS ANYARRAY AS $$
BEGIN
    RETURN (select array_agg(u.v) from (select intersection.v from
            (select unnest(a) as v union select unnest(b) as v) as intersection
            order by intersection.v) as u);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION aintersect(a ANYARRAY, b ANYARRAY) RETURNS ANYARRAY AS $$
BEGIN
    RETURN (select array_agg(u.v) from (select intersection.v from
        (select unnest(a) as v intersect select unnest(b) as v) as intersection
        order by intersection.v) as u);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION aexcept(a ANYARRAY, b ANYARRAY) RETURNS ANYARRAY AS $$
BEGIN
    RETURN (select array_agg(u.v) from (select v from
        (select unnest(a) as v except select unnest(b) as v) as intersection
        order by intersection.v) as u);
END;
$$ LANGUAGE plpgsql;

-- select aunion(array[1,2,3],array[3,4,5]);
-- select aintersect(array[1,2,3],array[3,4,5]);
-- select aexcept(array[1,2,3],array[3,4,5]);
