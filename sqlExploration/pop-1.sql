-- Example 2.3.4 from Ding et al.

drop table if exists memo;
create table memo (gid int, op varchar, lchild int, rchild int, data varchar);

-- unnest record from here:
-- https://www.postgresql.org/message-id/CAHyXU0zQMmG-cV5b27ZgyJ9xKpUjGG51gf0UROy9TkioTMe3XQ@mail.gmail.com
drop type if exists memo_t;
create type memo_t as (
    gid int,
    op varchar,
    lchild int, rchild int,
    data varchar
);

insert into memo values
    (0, 'noop', 0, 0, null),
    (1, 'scan', 0, 0, 'A'),
    (2, 'scan', 0, 0, 'B'),
    (3, 'scan', 0, 0, 'C'),
    (4, 'join', 1, 2, null),
    (5, 'join', 4, 3, null);
