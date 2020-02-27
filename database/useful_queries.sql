select t3.file_name, t1.*
from previews_lines t1
join previews_hdr t3 on t3.pvh_id = t1.pvh_id
left join (
    select *
    from previews_hdr
    where ident_typ <> 40 and raw_ident is not null
) t2
on t2.pvh_id = t1.pvh_id and t2.ident_line = t1.pvl_seq
where t2.pvh_id is null
  and t1.line_text not like '%\t%'
  and t1.line_text != ''
  and t1.line_text not like 'PAGE%'
order by t1.pvh_id, t1.pvl_seq
;

create table pvhh_seq (id INT NOT NULL);
insert into pvhh_seq values(0);
select * from pvhh_seq;
UPDATE pvhh_seq SET id=0;
UPDATE pvhh_seq SET id=LAST_INSERT_ID(id+1);
SELECT LAST_INSERT_ID() as pvhh_id;
