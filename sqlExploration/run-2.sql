-- explore step
select * into temp table delta from
(
(with memox (gid, op, lchild, rchild, cols, exp) as (
    -- Join commutativity
    select gid, op, rchild, lchild, cols, exp
    from memo
    where op = 'join'
    union all
    -- Join associativity
    select
        (unnest(array[
            (ugid, 'join', a, dgid, ucols, uexp),
            (dgid, 'join', b, c, dcols, dexp)
        ]::memo_t[])).*
    from
    (
        select
            d.lchild as a, d.rchild as b, u.rchild as c,
            u.gid as ugid, (select max(gid) from memo)+row_number() over () as dgid,
            u.cols as ucols, aexcept(u.cols, aexcept(d.cols, b.cols)) as dcols,
            d.exp as uexp, u.exp as dexp
        from memo u
            join memo d on u.lchild = d.gid
            join memo b on d.rchild = b.gid
        where u.op = 'join' and d.op = 'join' and
            aintersect(u.exp,d.cols) = aintersect(u.exp,b.cols)
    ) as x
    union all
    -- original
    select * from memo
)
select * from (
    with dedup (min, gid) as (
        select min(gid) over ( partition by op, lchild, rchild, cols, exp ), gid
        from memox
    )
    select distinct
        dg.min as gid, m.op as op, dl.min as lchild, dr.min as rchild,
        m.cols as cols, m.exp as exp
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
