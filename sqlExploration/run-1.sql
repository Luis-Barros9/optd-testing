-- explore step

select * into temp table delta from
(
(with memox (gid, op, lchild, rchild, data) as (
    -- Join commutativity
    select gid, op, rchild, lchild, data
    from memo
    where op = 'join'
    union all
    -- Join associativity
    select
        (unnest(array[
            (ugid, 'join', a, dgid, ddata),
            (dgid, 'join', b, c, udata)
        ]::memo_t[])).*
    from
    (
        select
            d.lchild as a, d.rchild as b, u.rchild as c,
            u.gid as ugid, (select max(gid) from memo)+row_number() over () as dgid,
            u.data as udata, d.data as ddata
        from memo u join memo d on u.lchild = d.gid
        where u.op = 'join' and d.op = 'join'
    ) as u
    union all
    -- original
    select * from memo
)
select * from (
    with dedup (min, gid) as (
        select min(gid) over ( partition by op, lchild, rchild, data ), gid
        from memox
    )
    select distinct dg.min as gid, m.op as op, dl.min as lchild, dr.min as rchild, m.data as data
    from memox m
        join dedup dg on m.gid = dg.gid
        join dedup dl on m.lchild = dl.gid
        join dedup dr on m.rchild = dr.gid
) as subq
)
) as d;

delete from memo;
insert into memo select * from delta;

select * from memo order by gid;
