# #########################################################
#
# perform heading hierarchy path search and
#   update previews_lines for any matches
#
# #########################################################

# get the file name time period date from the previews_hdr table
# we need this when querying the previews_hdg_hrch table
select fn_period from previews_hdr where pvh_id = ?;

# clear out any previous marked matches
update previews_lines set pvhh_id = NULL where TRUE;

# set the curr_path to the empty list
# set the curr_lvl to zero

# get the data from the previews_lines table, and loop over each record (1st db conn) --> curr_row
select
    pvl_id as curr_pvl_id,
    line_text as curr_hdg
from previews_lines
where pv_type = 'HDG'
and pvh_id = ?
order by pvl_seq;

#
# for each record...
#

#   build the set of search paths to try for the curr_hdg

#   query the previews_hdg_hrch table
#   for example::
        # if the current heading was: VERTIGO
        # and if current path was: / PREMIER PUBLISHERS / DC COMICS / DC COMICS BACKLIST
        # we would search for records that include paths that equal
        #   / PREMIER PUBLISHERS / DC COMICS / DC COMICS BACKLIST / VERTIGO
        #   / PREMIER PUBLISHERS / DC COMICS / VERTIGO
        #   / PREMIER PUBLISHERS / VERTIGO
        #   / VERTIGO
        # and would use the path with the greatest level
        # and if there are multiple paths with the same greatest level,
        # then sort alphabetically and take first one.
        with recursive hrch (pvhh_id, heading_nm, hrch_level, path) as (
            select
                t0.pvhh_id,
                t0.heading_nm,
                t0.hrch_level,
                concat('/ ', t0.heading_nm) as path
            from previews_hdg_hrch t0
            where t0.parent_pvhh_id = 0
            and :fn_period between t0.valid_from and t0.valid_to
            union all
            select
                t1.pvl_id,
                t1.heading_nm,
                t1.hrch_level,
                concat(t2.path, ' / ', t1.heading_nm) as path
            from previews_hdg_hrch as t1
            join hrch t2
            on t2.pvhh_id = t1.parent_pvhh_id
            where :fn_period between t1.valid_from and t1.valid_to
        ) select pvhh_id, hrch_level from hrch
        where path in (
            '/ PREMIER PUBLISHERS / DC COMICS / DC COMICS BACKLIST / VERTIGO',
            '/ PREMIER PUBLISHERS / DC COMICS / VERTIGO',
            '/ PREMIER PUBLISHERS / VERTIGO',
            '/ VERTIGO'
        ) order by hrch_level desc, path
        limit 1;
        # we would return the following record and use it (including heading_nm & path for clarity)
        # pvhh_id	heading_nm	hrch_level	path
        # 123456    VERTIGO	    4	        / PREMIER PUBLISHERS / DC COMICS / DC COMICS BACKLIST / VERTIGO

#   if found

#       update previews_lines set PVHH_ID = pvhh_id of found previews_hdg_hrch rec
#       where pvl_id = curr_pvl_id

#       if level of found previews_hdg_hrch rec > curr_lvl then
#           curr_path.append(curr_hdg)
#       else
#           curr_path = curr_path[:{{level of found previews_hdg_hrch rec}} - curr_lvl - 1] + [curr_hdg]
#           curr_lvl = level of found previews_hdg_hrch rec

#   else if not found do nothing...



# #########################################################
#
# if there are any unmatched headings, export all lines to a file
#
# #########################################################

with row_numbers as (
    select
        row_number() over (partition by pvh_id order by pvl_seq) + 1 as row_num,
        pvh_id,
        pvl_id,
        pvl_seq,
        pg_nbr,
        pvhh_id,
        line_text
    from previews_lines
    where pv_type = 'HDG'
)
select
    t1.row_num - 1 as row_num,     -- A  A
    t2.file_name,                  -- B  B
    t1.pvh_id,                     -- C  C
    t1.pvl_seq,                    -- D  E
    t1.pvl_id,                     -- E  D .
    t1.pg_nbr,                     -- F  F
    t3.hrch_level as hdg_lvl,      -- G  G .
    t3.detail_items_ind,           -- H  #     True to parse previews_line rec to previews_dtl rec
    null as dup_pvl_id,            -- I  H
    t1.pvhh_id,                    -- J  #     populated if match was found, else null if no match found
    t1.line_text,                  -- K  I .
    -- EXCEL formula: if( <hdg_lvl><rn> = "", "", REPT("-", (<hdg_lvl><rn> - 1) * 4) & <line_txt><rn> )
    concat('=IF(G', row_num, '="","",REPT("-",(G', row_num, '-1)*4)&K', row_num, ')') as indent
from row_numbers t1
join previews_hdr t2
on t2.pvh_id = t1.pvh_id
left join previews_hdg_hrch t3
on t3.pvhh_id = t1.pvhh_id
where t1.pvh_id = ?
order by t1.pvh_id, t1.pvl_seq;

# MANUAL PROCESS::
#
# user opens file and assigns heading hierarchy level numbers for any unmatched headings or
#   marks the heading as a duplicate (note the existing pvl_id that is the same as the duplicate)
# the user must perform the above for ALL unmatched heading lines before proceeding to the next step


# load updated export file into hdg_hrch_import_<pvh-id>_<attempt> and assign parent_pvl_id for
#   new headings
#
#   call the method:  set_parent_pvl_id_on_hrch_import()



# #########################################################
#
# merge the new headings into the previews_hdg_hrch table
#
# #########################################################

# get the data from the hdg_hrch_import_<pvh-id>_<attempt> table, and loop over each record (1st db conn)
#   only gets records that are new headings (pvhh_id is NULL and a hdg_lvl has been assigned).
#   these must be processed in the correct order so that parent headings are processed before their
#   corresponding child headings
select
    t1.pvl_id,
    t1.parent_pvl_id,
    t1.hdg_lvl as hrch_lvl,
    t1.line_text as heading_nm,
    t1.detail_items_ind,
    t2.fn_period as valid_from
from hdg_hrch_import_1_1 t1
join previews_hdr t2 on t1.pvh_id = t2.pvh_id
where t1.pvhh_id is null
and t1.hdg_lvl is not null
order by t1.pvh_id, t1.pvl_seq;

#
# for each record...
#


# get the next sequence value for the PVHH_ID

-- beforehand make sure that have inserted 1 record into the pvhh_seq table with an ID = 0
-- insert into pvhh_seq (id)  values (0);
UPDATE pvhh_seq SET id=LAST_INSERT_ID(id+1);
-- after the update, use the following python code instead to get the value:
-- pvhh_id = curs.lastrowid
SELECT LAST_INSERT_ID();


# the input will have a parent PVL_ID, which will have been processed previously, so we need to
#   lookup the records corresponding PVHH_ID

-- need to check if the following qurey returns 0 results, if so, then use a value of 0 for pvhh_id
select pvhh_id from previews_hdg_hrch where pvl_id = :parent_pvl_id
and :valid_from between valid_from and valid_to;

# insert the new heading into the previews_hdg_hrch table

insert into previews_hdg_hrch (
    pvhh_tid,
    pvl_id,
    pvhh_id,
    parent_pvhh_id,
    hrch_level,
    heading_nm,
    detail_items_ind,
    valid_from,
    valid_to
) values (
    DEFAULT,            -- auto incrment
    :pvl_id,            -- from main select
    :LAST_INSERT_ID,    -- from pvhh_seq table
    :parent_pvhh_id,    -- from separate lookup based on parent_pvl_id
    :hrch_lvl,          -- from main select
    :heading_nm,        -- from main select
    :detail_items_ind,  -- from main select
    :valid_from,        -- from main select
    '9999-12-31'
);

#
#   repeat from the beginning
#
