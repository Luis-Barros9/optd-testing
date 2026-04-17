-- explore step

select * into temp table delta from
(
(with memox (gid, op, lchild, rchild, cols, exp) as (
    -- filter push-down
    select
        (unnest(array[
            (dgid, 'filt', dl, 0, dcols, dexp),
            (ugid, uop, dgid, ur, ucols, uexp)
        ]::memo_t[])).*
    from
    (
        select
            u.gid as ugid,
            (select max(gid) from memo)+row_number() over () as dgid,
            d.op as uop,
            d.lchild as dl, d.rchild as ur,
            d.cols as ucols, u.cols as dcols,
            d.exp as uexp, u.exp as dexp
        from memo u
            join memo d on u.lchild = d.gid
            join memo i on d.lchild = i.gid
        where u.op = 'filt' and aintersect(u.exp,i.cols)=u.exp
    ) as x
    union all
    -- original
    select * from memo
)
select * from
(
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
