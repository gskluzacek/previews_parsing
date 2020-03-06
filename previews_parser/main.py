import os
import sys
import re
from datetime import date
from datetime import datetime

import codecs
from chardet.universaldetector import UniversalDetector

import mysql.connector

# lots of good information
# https://github.com/gskluzacek/previews/blob/master/old_code/Preview%20Parsing/directory_contents.md

# see the below url for details on individual fields
# https://github.com/gskluzacek/previews/blob/master/old_code/Preview%20Parsing/previews%20parsing/pp.php
#       Cancellation Codes: What They Mean
#       Caution codes       C: #-#-#)


# the first program reads the cof file and inserts a record for the file into a header table
#       pvh_id          PK sequence
#       period_dt       parses the filename (AUG14_COF.txt) to a date obj. DD-NNN-YYYY as 01-AUG-2014
#       period_str      parses the filename, and keeps the first 5 chars as a string
#       ident_str       parses the first non-blank line of the file (PREVIEWS AUGUST VOL. 24 #8)
#                       to: PREVIEWS VOL vvv ii mmm-yyyy
#                       vvv - 3 digit volume number left padded with 0
#                       ii  - 2 digit issue number left padded with 0
#                       mmm - 3 character month
#                       yyyy - 4 digit year (vol nbr + 2000 - 10)
#       local_file      file name - full path or just the name?
#       url_to_cof      url to the file on the previews web site
#       proc_status     populated to NEW
# then inserts each line into a detail table
#       pvl_id          PK sequence
#       pvh_id          FH to header table
#       pvl_seq         value starts at 1 and is incremented for each line
#       line_text       the line of text from the file
#       override_pvhl_id    populated to ????

# the second program
#   reads in a reverse heading name lookup data structure
#   reads in a heading hierarchy data structure
#   get a list of pvh_ids from the previews_hdr table
#   for each phv_id
#       reads records from previews_lines table
#       - pvl_id, pvl_seq, line_text, override_pvhl_id
#       line is split into fields
#           - pv_type           // initialize to UNKNOWN; values: H1, H2, H3, Hn, ITEM, BLANK, PAGE, IDENT, NOTFOUND
#           - pv_value      0   // can be blank, in which case it should be set to null
#           - sol_code      1   // if the sol_code is set, then the pv_type is ITEM mmmyy nnnn (remove the space)
#                               // there can be 1 or 2 tabs between the sol_code & sol_text
#                               // so we need to check if the 3rd element [index of 2] is blank, if yes
#                               // then we need adjust all remaining columns by 1 to the right
#           - sol_text      2+  //
#           - sol_page          // this is set to the value of the last line of pv_type = PAGE
#           - release_dt    3+  // can be blank, so need to check before creating date obj: mm/dd/yy
#           - unit_price    4+  // the price has a price type, colon, space, dollar sign, and an amount with a
#                               // decimal point and 2 digits after the decimal point
#           - pi_ind            // this field may have a value of SRP: PI, in which case, set pi_ind to Y
#                               // and unit_price is set to NULL, else set pi_ind to NULL
#                               // if unit price cannot be parsed, then set pi_ind to E (error)
#           -                   // the price type is (currently) discarded
#           -                   // the remaining 2 fields are ' = $' and ''
#           - pv_source         // set to the heading's path; heading separated by ' / '
#           -                   // other values for pv_type
#                               // sol_code is the empty string AND
#                               // pv_value is the empty string         pv_type --> BLANK
#                               // first 4 chars are PAGE               pv_type --> PAGE
#                               // if regex matches                     pv_type --> IDENT
# /^PREVIEWS (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).* V(OL){0,1}\.{0,1} {0,1}\d\d #\d{1,2}$/
#                               // if not ITEM, BLANK, PAGE or IDENT then the pv_type should be a heading type

# PREVIEWS LINE TYPES (pv_type)
#           [  sol_code is the 2nd field <ndx-1>  ]
#           [  pv_value is the 1st field <ndx-0>  ]
#
# ITEM      if the sol_code is populated, its an ITEM
# BLANK     sol_code & pv_vale are both blank
# PAGE      sol_code is blank and pv_value is: PAGE xxx
# IDENT     if the sol_code is blank and the value matches the
#           regular expression for the identification line, then it is a identification line
# H3        if none of the above and if lvl 2 is set - check for lvl 3
# H2        if none of the above and if lvl 1 or 2 is set - check for lvl 2
# H1        if none of the above - check for lvl 2
# NOTFOUND  if none of the above then not found

# TABLES
#
#   previews_hdr            [  a table of when each COF file was processed ]
#       pvh_id              [  PK  ]
#       period_dt
#       -period_str
#       ident_str
#       +raw_indent
#       local_file
#       -url_to_cof
#       proc_status         [  LOGGED | LOADED | PARSED  ]
#
#   previews_lines          [  UNIQUE on:: pvh_id, pvl_seq  ]
#       pvl_id              [  PK  ]
#       pvh_id              [  FK -- previews_hdr  ]
#       pvl_seq             [  starts at 1 for each pvh_id  ]
#       line_text
#       override_pvhl_id    [  if set and is not 0, then use revers heading lookup  ]
#                           [  (??? need more info on what this is)  ]
#
#   +previews_hdg_hrch      [  new table name for previews_hdg_lvls ]
#       pvhh_id             [  PK  ]
#       pvhh_cmn_id         [  links multiple versions of a headings over time ]
#       hrch_lvl
#       parent_pvhh_id      [  FK -- previews_hdg_hrch  ]
#       heading_name
#       valid_from
#       valid_to
#
#   previews_hdg_lvls       [  UNIQUE on:: parent_pvhl_id, heading_name  ]
#       pvhl_id             [  PK  ]
#       pvhl_level
#       parent_pvhl_id      [  FK -- previews_hdg_lvls  ]
#       pull_list_ind
#       heading_name
#
#   previews_raw
#       [ call this table previews_dtl instead and I think it should just contain ITEM lines ]
#       [ that are COMIC BOOKS. leave out the columns that are parsed from the SOL_TEXT      ]
#       [ also try to incorporate as some of the other columns back to the previews_lines    ]
#       [ table, like: pv_type, sol_page, heading columns, (keep pvl_id) get rid of PVH_ID,  ]
#       [ pv_seq, etc.                                                                       ]
#
#       pvr_id              [  PK  ]
#       pvh_id              [  FK -- previews_hdr  ]
#       pv_seq              [  incremented for each line processed  ]
#       pvl_id              [  FK -- previews_lines  ]
#       pv_type             [  * see below  ]
#       pv_value            [  <ndx-0>  ]
#       h1_pvhl_id          [  FK -- previews_hdg_lvls heading lookup on pv_value  ]
#       h2_pvhl_id          [  FK -- previews_hdg_lvls heading lookup on pv_value  ]
#       h3_pvhl_id          [  FK -- previews_hdg_lvls heading lookup on pv_value  ]
#       pv_source           [  heading 1 / heading 2 / heading 3  ]
#       sol_page            [  * pv_value  ]
#       sol_code            [  <ndx-1>  ]
#                           [  non-blank for ITEM the pv_type, blank for all other pv_type's  ]
#       sol_text            [  <ndx-2>  ]
#       release_dt          [  <ndx-3>  ]
#       unit_price          [  <ndx-4>  ]
#                           [  unit_price & pi_ind are mutually exclusive  ]
#       price type          [  Unit price should have a leading 'SRP' or 'MSRP'  ]
#       pi_ind              [  'please inquire (on price)' indicator  ]
#                           [  can be NULL, Y or E (invalid unit price type)  ]
#       title
#       sub_title
#       title_vol
#       title_type
#       title_status
#       title_designations
#       book_type
#       book_vol
#       book_designations
#       issue_num
#       total_issues
#       printing
#       cover_variant
#       cover_type
#       advisory_code
#       caution_code
#       sol_info_codes
#       prev_sol_code
#       edition
#       other_designations

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#           notes on headings hierarchy resolution from the legacy code
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# it appears that all lines read from the previews_lines table were inserted into the previews_raw
#       table.
#
# header columns / variables
#   - pv_source
#   - h1_pvhl_id
#   - h2_pvhl_id
#   - h3_pvhl_id
#
# accumulator variables
#   - h1_id             - hdg_1
#   - h2_id             - hdg_2
#   - h3_id             - hdg_3
#
# in each iteration of the previews_lines loop the heading columns variables are reset to NULL
# however the variables used to determine the values of the heading columns are not, they act
#   as accumulator type variables collecting the values as the lines are processed (in sequence)
# only under certain conditions are the accumulator variables assigned to the heading column variables
#
# here's how the accumulator variables are set.
#
# when H1 is found
#   $hdg_1 & $h1_id are set and
#   level 2 is set to null
#   level 3 is set to null
# when H2 is found
#   $hdg_1 & $h1_id are left unchanged and
#   $hdg_2 & $h2_id are set and
#   level 3 is set to null
# when H3 is found
#   $hdg_1 & $h1_id are left unchanged and
#   $hdg_2 & $h2_id are left unchanged and
#   $hdg_3 & $h3_id are set and
#
# the path (aka pv_source) is determined as follows:
# $src1 = isset($hdg_1) ? " / $hdg_1" : ''
# $src2 = isset($hdg_2) ? " / $hdg_2" : ''
# $src3 = isset($hdg_3) ? " / $hdg_3" : ''
# pv_source = $src1 . $src2 . $src3
#
# an ITEM is assigned whatever headings are in affect at that point in the sequence of lines.
#   that would be a source (path) and N number of heading level ids (PVHL_ID)
# a HEADING is not assigned a source (path) but is assigned N number of head level ids (PVHL_ID)
#   that are in affect at the point in the sequence of lines
# no other line types are assigned heading column values.

# the $heading_levels data structure
#
# in the legacy code, the data structure was only used in conduction with the override_pvhl_id
#       column on the preview_lines table and was used to set the: hdg_1, hdg_2 & hdg_3 and h1_id,
#       h2_id & h3_id variables.
#
# the data structure is a dictionary object, where the primary dictionary key is the PVHL_ID
#       to the PREVIEWS_HDG_LVLS table
# each primary key will have the following mandatory sub-keys / values
#       - level
#       - name
# the elements for the remaining sub-keys / values will vary on the hierarchy level
#       - level 1 will only have a [level] number of 1 and the level 1 heading [name]
#       - level 2 with have a the [level] number of 2 and the level 2 heading [name]
#         additionally it will have sub-keys of
#           - [h1_name] with the heading 1 name and
#           - [h1_id] which will contain the PVHL_ID for the level 1 heading record
#       - level 3 with have a the [level] number of 3 and the level 3 heading [name]
#         additionally it will have sub-keys of
#           - [h2_name] with the heading 2 name and
#           - [h2_id] which will contain the PVHL_ID for the level 2 heading record
#           - [h1_name] with the heading 1 name and
#           - [h1_id] which will contain the PVHL_ID for the level 1 heading record

# cof_dir = '/Users/gregskluzacek/Documents/Development/github_repos/previews_parsing/cof_files'
fn_path = '/Users/gskluzacek/Documents/GitHub/previews_parsing/cof_files'
export_dir = '/Users/gskluzacek/Downloads/hdg_hrch_exports'


def main():
    fn = '/Users/gregskluzacek/Downloads/FEB20_COF.txt'
    with open(fn, 'r') as f:
        for line in f:
            fields = line.strip('\n').split('\t')
            print(fields)


def init_txt_params(line_nbr):
    params = {
        'ident_line': line_nbr,
        'txt_ident': None,
        'txt_mo': None,
        'txt_yr': None,
        'txt_volume': None,
        'txt_vol_issue': None,
        'txt_issue': None,
        'txt_period': None,
        'txt_name': None
    }
    return params


def missing_ident_line(line_nbr):
    params = init_txt_params(line_nbr * -1)
    updt_params = {
        'ident_type': 4
    }
    params.update(updt_params)
    return params


def terse_ident_line_with_ind_line(line_nbr, match, txt_ident):
    params = init_txt_params(line_nbr + 1)
    if match:
        updt_params = {
            'ident_type': 20,
            'txt_ident': txt_ident,
            'txt_mo': match.group(1),
            'txt_volume': match.group(2),
            'txt_vol_issue': match.group(3)
        }
    else:
        updt_params = {
            'ident_type': 2
        }
    params.update(updt_params)
    return params


def basic_ident_line(line_nbr, match, txt_ident):
    params = init_txt_params(line_nbr)
    updt_params = {
        'ident_type': 10,
        'txt_ident': txt_ident,
        'txt_mo': match.group(1),
        'txt_volume': match.group(2),
        'txt_vol_issue': match.group(3)
    }
    params.update(updt_params)
    return params


def advanced_ident_line(line_nbr, match_1, match_2, txt_ident_1, txt_ident_2):
    params = init_txt_params(line_nbr)
    if match_2:
        txt_mo_str = match_1.group(1).upper()
        txt_yr_nbr = match_1.group(2)
        updt_params = {
            'ident_type': 30,
            'txt_ident': txt_ident_1 + ' | ' + txt_ident_2,
            'txt_mo': txt_mo_str,
            'txt_yr': txt_yr_nbr,
            'txt_volume': match_2.group(2),
            'txt_vol_issue': match_2.group(3),
            'txt_issue': match_2.group(1),
            'txt_period': datetime.strptime(f'{txt_yr_nbr}-{txt_mo_str}-01', '%Y-%b-%d'),
            'txt_name': f'{txt_mo_str}{int(txt_yr_nbr) - 2000}.txt',
        }
    else:
        updt_params = {
            'ident_type': 3
        }
    params.update(updt_params)
    return params


def log_cof_files() -> int:
    regex1 = r'^PREVIEWS (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).* V(?:OL)?\.? ?(\d\d) #(\d\d?)$'
    cregex1 = re.compile(regex1)

    regex3 = r'^(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).* V(?:OL)?\.? ?(\d\d) #(\d\d?)$'
    cregex3 = re.compile(regex3)

    regex4 = r'^PREVIEWS (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).* (\d\d\d\d)$'
    cregex4 = re.compile(regex4)
    regex5 = r'^ISSUE #(\d\d\d) \(VOL\. (\d\d) #(\d\d?)\)$'
    cregex5 = re.compile(regex5)

    # function returns
    # a dictionary - key = file's date obj & key = cof file name
    # a list - the list of the dictionary's keys in sorted order
    fn_names, sorted_cof_file = get_cof_files_in_sorted_order(fn_path)

    db_conn = get_db_conn()
    db_conn.autocommit = True
    curs = db_conn.cursor(dictionary=True)

    sql_stmt = "truncate table previews_hdr;"
    curs.execute(sql_stmt)

    sql_stmt = """
        insert into previews_hdr (
            pvh_id,
            ident_typ,
            ident_line,
            txt_ident,
            txt_mo,
            txt_yr,
            txt_volume,
            txt_vol_issue,
            txt_issue,
            txt_period,
            txt_name,
            fn_ident,
            fn_mo,
            fn_yr,
            fn_volume,
            fn_vol_issue,
            fn_issue,
            fn_period,
            fn_name,
            proc_sts,
            fn_path
        ) values (
            DEFAULT, 
            %(ident_type)s, 
            %(ident_line)s, 
            %(txt_ident)s, 
            %(txt_mo)s,
            %(txt_yr)s,
            %(txt_volume)s,
            %(txt_vol_issue)s,
            %(txt_issue)s,
            %(txt_period)s,
            %(txt_name)s,
            %(fn_ident)s,
            %(fn_mo)s,
            %(fn_yr)s,
            %(fn_volume)s,
            %(fn_vol_issue)s,
            %(fn_issue)s,
            %(fn_period)s,
            %(fn_name)s,
            'LOGGED',
            %(fn_path)s
        );
    """

    # loop through the list of sorted cof file date objects
    for fn_dt_obj in sorted_cof_file:

        fn_yr_nbr = fn_dt_obj.year
        fn_mo_nbr = fn_dt_obj.month
        fn_mo_str = fn_dt_obj.strftime('%b').upper()
        fn_vol_iss_nbr = fn_mo_nbr
        fn_vol_nbr = fn_yr_nbr - 1990
        fn_iss_nbr = (fn_vol_nbr - 1) * 12 + fn_vol_iss_nbr + 27

        str_vol_iss_nbr = str(fn_mo_nbr).zfill(2)
        fn_ident = f'PREVIEWS {fn_mo_str}-{fn_yr_nbr} ISSUE #{fn_iss_nbr} (VOL {fn_vol_nbr} #{str_vol_iss_nbr})'

        try:
            # open the cof file
            with open(fn_path + '/' + fn_names[fn_dt_obj], 'r') as fh:
                # keep reading the cof file until we determine what type of IDENT line it has
                #   - missing IDENT line
                #   - terse IDENT line
                #   - basic IDENT line
                #   - advanceed IDENT line
                for i, line in enumerate(fh, 1):
                    line = line.strip()

                    # check if we've hit the first head of the file which is always: PREVIEWS PUBLICATIONS
                    if line == 'PREVIEWS PUBLICATIONS':
                        txt_params = missing_ident_line(i)
                        break

                    # check if matches the ``ident line label`` which indicates the next line is the IDENT line
                    if line == 'PREVIEWS ORDER FORM':
                        # get the next line
                        line = fh.readline()
                        line = line.strip()
                        # attempt to get the regex match object on the TERSE IDENT line
                        m = cregex3.fullmatch(line)

                        txt_params = terse_ident_line_with_ind_line(i, m, line)
                        break

                    # if if the regex matches the BASIC IDENT line
                    m = cregex1.fullmatch(line)
                    if m:
                        txt_params = basic_ident_line(i, m, line)
                        break

                    # if if the regex matches the 1st of 2 ADVANCED IDENT lines
                    m = cregex4.fullmatch(line)
                    if m:
                        # 1st of 2 matched, get the next line
                        line2 = fh.readline()
                        line2 = line2.strip()
                        # attempt to get the regex match object on the 2nd of 2 ADVANCED IDENT lines
                        m1 = cregex5.fullmatch(line2)

                        txt_params = advanced_ident_line(i, m, m1, line, line2)
                        break

                params = {
                    'fn_ident': fn_ident,
                    'fn_mo': fn_mo_str,
                    'fn_yr': fn_yr_nbr,
                    'fn_volume': fn_vol_nbr,
                    'fn_vol_issue': fn_vol_iss_nbr,
                    'fn_issue': fn_iss_nbr,
                    'fn_period': fn_dt_obj,
                    'fn_name': fn_names[fn_dt_obj],
                    'fn_path': fn_path
                }
                params.update(txt_params)

                try:
                    curs.execute(sql_stmt, params)
                except Exception as err:
                    print(f'error on insert {err}')

                print(f'{fn_names[fn_dt_obj]}\t{fn_dt_obj.strftime("%b-%Y")}\t{params["ident_type"]}\t'
                      f'{params["ident_line"]}\t{params["txt_ident"]}\t{fn_ident}')

        except Exception as err:
            print(f'got error on file {fn_names[fn_dt_obj]}')
            print(err)
    curs.close()
    db_conn.close()
    return 0


def get_cof_files_in_sorted_order(cof_files_dir):
    """
    Reads the directory with the cof files and
    returns a dict{date-obj: cof-file-nm} and sorted list of dict-key's (date-obj's)
    """
    months = {
        'JAN': 1, 'FEB': 2, 'MAR': 3,
        'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9,
        'OCT': 10, 'NOV': 11, 'DEC': 12
    }

    cof_files = {}
    with os.scandir(cof_files_dir) as entries:
        for entry in entries:
            if entry.is_file() and '.txt' in entry.name:
                mo_str = entry.name[:3]
                mo_nbr = months.get(mo_str)
                if mo_nbr:
                    yr_str = entry.name[3:5]
                    if yr_str.isdigit():
                        yr_nbr = 2000 + int(yr_str)
                        dt = date(yr_nbr, mo_nbr, 1)
                        cof_files[dt] = entry.name
                    else:
                        raise Exception(f'invalid year string {yr_str}')
                else:
                    raise Exception(f'invalid month string {mo_str}')
    sorted_cof_file = sorted(cof_files.keys())
    return cof_files, sorted_cof_file


def load_line() -> int:
    db_conn_wrk = get_db_conn()
    curs_wrk = db_conn_wrk.cursor()
    sql_stmt_ins = """
        insert into previews_lines (
            pvl_id,
            pvh_id,
            pvl_seq,
            line_text            
        ) values (
            DEFAULT, 
            %(pvh_id)s, 
            %(pvl_seq)s, 
            %(line_txt)s
        );
    """
    sql_stmt_updt = """
        update previews_hdr set proc_sts = 'LOADED' where pvh_id = %(pvh_id)s;
    """

    db_conn = get_db_conn()
    curs = db_conn.cursor(dictionary=True)
    sql_stmt = """
        select pvh_id, fn_ident, fn_path, fn_name from previews_hdr where proc_sts = 'LOGGED' order by fn_period;
    """
    curs.execute(sql_stmt)
    for row in curs:
        print(f'processing: {row["fn_ident"]}, file: {row["fn_name"]}')
        with open(row['fn_path'] + '/' + row['fn_name']) as fh:
            try:
                for pvl_seq, line in enumerate(fh, 1):
                    params = {
                        'pvh_id': row["pvh_id"],
                        'pvl_seq': pvl_seq,
                        'line_txt': line.strip('\n')
                    }
                    curs_wrk.execute(sql_stmt_ins, params)
            except Exception as err:
                print(f"error on line {pvl_seq}")
                print(line)
                print(err)
                db_conn_wrk.rollback()
                continue
            params = {'pvh_id': row["pvh_id"]}
            curs_wrk.execute(sql_stmt_updt, params)
            db_conn_wrk.commit()
    curs_wrk.close()
    db_conn_wrk.close()
    curs.close()
    db_conn.close()
    return 0


def get_encoding_type(current_file: str, detector: UniversalDetector):
    detector.reset()
    with open(current_file, 'rb') as fh:
        for line in fh:
            detector.feed(line)
            if detector.done:
                break
        detector.close()
    return detector.result['encoding']


def convert_files_encoding() -> int:
    detector = UniversalDetector()
    cof_files, sorted_cof_file = get_cof_files_in_sorted_order(fn_path)
    for cof_file in sorted_cof_file:
        fn = fn_path + '/' + cof_files[cof_file]
        encoding = get_encoding_type(fn, detector)
        print(cof_files[cof_file], encoding)
        if encoding != 'utf-8':
            with codecs.open(fn, 'rU', encoding) as sourceFile:
                with codecs.open(fn_path + '/converted/' + cof_files[cof_file], 'w', 'utf-8') as targetFile:
                    for line in sourceFile:
                        targetFile.write(line)

    return 0


def set_pv_type() -> int:
    db_conn = get_db_conn()
    curs = db_conn.cursor()
    sql_stmt = """
        with
        last_item as (
            select
                pvh_id,
                max(pvl_seq) as last_item_seq
            from previews_lines
            where line_text like '%\t%\t%'
            group by pvh_id
        ),
        ident_lines as (
            select
                pvh_id,
                ident_line
            from previews_hdr
            where ident_typ <> 4
            and txt_ident is not null
        ), 
        pv_type_calc as (
            select
                t1.pvl_id,
                t1.pvh_id,
                case
                    when t4.pvh_id is not null         then 'IDENT'
                    when t1.line_text = ''             then 'BLANK'
                    when t1.line_text like '%\t%\t%'   then 'ITEM'
                    when t1.line_text like 'PAGE%'     then 'PAGE'
                    when t1.pvl_seq > t3.last_item_seq then 'JUNK'
                    else 'HDG'
                end as pv_type_calced,
                t2.proc_sts
            from previews_lines t1
            join previews_hdr t2
            on t2.pvh_id = t1.pvh_id
            join last_item t3
            on t3.pvh_id = t1.pvh_id
            left join ident_lines t4
            on t4.pvh_id = t1.pvh_id
            and t4.ident_line = t1.pvl_seq
        )
        update previews_lines as pl
        join pv_type_calc as ptc
        on ptc.pvl_id = pl.pvl_id
        set pl.pv_type = ptc.pv_type_calced
        where ptc.proc_sts = 'LOADED';
    """
    curs.execute(sql_stmt)

    sql_stmt = """
        with
        pv_type_not_assgnd as (
            select distinct
            pvh_id
            from previews_lines
            where pv_type is NULL
        )
        update previews_hdr as ph
        left join pv_type_not_assgnd as ptna
        on ptna.pvh_id = ph.pvh_id
        set ph.proc_sts = 'TYPED'
        where ph.proc_sts = 'LOADED'
        and ptna.pvh_id is NULL;
    """
    curs.execute(sql_stmt)
    db_conn.commit()
    curs.close()
    db_conn.close()

    return 0


def set_page_nbr():
    db_conn = get_db_conn()
    curs = db_conn.cursor()
    sql_stmt = """
        with
        pages as (
            select
                pvh_id,
                pvl_seq,
                line_text
            from previews_lines
            where pv_type = 'PAGE'
        ),
        max_lines as (
            select
                pvh_id,
                max(pvl_seq) as max_line
            from previews_lines
            group by pvh_id
        ),
        page_ranges as (
            select
                t1.pvh_id,
                t1.pvl_seq as start_line,
                ifnull(lead(t1.pvl_seq) over(partition by t1.pvh_id order by t1.pvl_seq) - 1, t2.max_line)  as end_line,
                substr(t1.line_text, 6) + 0 as pg_nbr
            from pages as t1
            join max_lines as t2 on t2.pvh_id = t1.pvh_id
        ),
        page_nbr_calced as (
            select
                t1.pvl_id,
                ifnull(t3.pg_nbr, 0) as pg_nbr,
                t2.proc_sts
            from previews_lines as t1
            join previews_hdr t2
            on t1.pvh_id = t2.pvh_id
            left join page_ranges as t3
            on t1.pvh_id = t3.pvh_id
            and t1.pvl_seq between t3.start_line and t3.end_line
        )
        update previews_lines as t1
        join page_nbr_calced as t2
        on t2.pvl_id = t1.pvl_id
        set t1.pg_nbr = t2.pg_nbr
        where t2.proc_sts = 'TYPED';
    """
    curs.execute(sql_stmt)

    sql_stmt = """
        with
        page_not_assgnd as (
            select distinct
            pvh_id
            from previews_lines
            where pg_nbr is NULL
        )
        update previews_hdr as ph
        left join page_not_assgnd as pna
        on pna.pvh_id = ph.pvh_id
        set ph.proc_sts = 'PAGED'
        where ph.proc_sts = 'TYPED'
        and pna.pvh_id is NULL;
    """
    curs.execute(sql_stmt)
    db_conn.commit()
    curs.close()
    db_conn.close()


def resolve_heading_hierarchy(pvh_id):
    # each time the previews_hdg_hrch table is updated with new hrch level numbers, we can re-run this
    # function to validate that all headings in the previews_lines table are matched to a path in the
    # previews_hdg_hrch table.
    #
    # step 1    - get the filename period for the given pvh_id
    # step 2    - reset previously assigned pvhh_id's on the previews_lines table
    # step 3    - execute driver query that selects the heading records from the previews_lines table
    # step 4        - attempt to lookup the matching path for the "current headings state"
    # step 5        - if not found, continue at the top of the loop
    # step 6        - update the "current headings state" with the new heading
    # step 7        - update the previews_lines record (pvl_id) for the matched path (pvhh_id)
    # step 8    - check if there are any headings that have not been matched to a path
    # step 9    - if all headings were matched then return
    # step 10   - else extract the heading lines to a file, so unmatched headings can can be updated with level numbers
    #

    db_conn = get_db_conn()
    curs_dict = db_conn.cursor(dictionary=True)
    curs = db_conn.cursor()

    db_conn_work = get_db_conn()
    curs_dict_work = db_conn_work.cursor(dictionary=True)
    curs_work = db_conn_work.cursor()

    # lookup the time period for the given pvh_id that is be processed
    sql_stmt = "select fn_period from previews_hdr where pvh_id = %(pvh_id)s;"
    params = {'pvh_id': pvh_id}
    curs_dict.execute(sql_stmt, params)
    row = curs_dict.fetchone()
    if not row:
        raise Exception(f'could not find file name period for pvh_id: {pvh_id}')
    fn_period = row['fn_period']

    # on the previews_lines table, reset any previously assigned pvhh_id's
    sql_stmt = "update previews_lines set pvhh_id = NULL where pvh_id = %(pvh_id)s;"
    params = {'pvh_id': pvh_id}
    curs_dict_work.execute(sql_stmt, params)

    # driver query, get the records to process in the loop from previews_lines table
    sql_stmt = """
        select
            pvl_id as curr_pvl_id,
            line_text as curr_hdg
        from previews_lines
        where pv_type = 'HDG'
        and pvh_id = %(pvh_id)s
        order by pvl_seq;
    """
    params = {'pvh_id': pvh_id}
    curs.execute(sql_stmt, params)

    # for each previews_lines record, attempt to lookup the matching path for the current headings state
    hrch_srch_stmt = """
        with recursive hrch (pvhh_id, heading_nm, hrch_level, path) as (
            select
                t0.pvhh_id,
                t0.heading_nm,
                t0.hrch_level,
                concat('| ', t0.heading_nm) as path
            from previews_hdg_hrch t0
            where t0.parent_pvhh_id = 0
            and %s between t0.valid_from and t0.valid_to
            union all
            select
                t1.pvhh_id,
                t1.heading_nm,
                t1.hrch_level,
                concat(t2.path, ' | ', t1.heading_nm) as path
            from previews_hdg_hrch as t1
            join hrch t2
            on t2.pvhh_id = t1.parent_pvhh_id
            where %s between t1.valid_from and t1.valid_to
        ) 
        select 
            pvhh_id, 
            hrch_level 
        from hrch
        where path in ({}) 
        order by hrch_level desc, path
        limit 1;
    """

    # if a matching path is found, then update the previews_lines record with the corresponding pvhh_id
    updt_sql_stmt = "update previews_lines set pvhh_id = %(pvhh_id)s where pvl_id = %(curr_pvl_id)s;"

    curr_path = []
    curr_lvl = 0
    for curr_pvl_id, curr_hdg in curs:

        search_for_paths_list = []
        for hdg_index in range(len(curr_path), -1, -1):
            path_to_try = curr_path[:hdg_index] + [curr_hdg]
            search_for_paths_list.append(path_to_try)
        path_in_list = ['| ' + ' | '.join(hdgs) for hdgs in search_for_paths_list]

        params = [fn_period, fn_period] + path_in_list
        path_in_placeholders = ', '.join(['%s'] * len(path_in_list))
        sql_stmt = hrch_srch_stmt.format(path_in_placeholders)
        curs_work.execute(sql_stmt, params)
        row = curs_work.fetchone()
        if not row:
            print(f'no match found for {curr_pvl_id} {curr_hdg}')
            continue
        pvhh_id, hrch_lvl = row
        print(f'found match {pvhh_id} {hrch_lvl}')

        if hrch_lvl > curr_lvl:
            curr_path.append(curr_hdg)
        else:
            slice_index = hrch_lvl - curr_lvl - 1
            curr_path = curr_path[:slice_index] + [curr_hdg]
        curr_lvl = hrch_lvl

        params = {'pvhh_id': pvhh_id, 'curr_pvl_id': curr_pvl_id}
        curs_dict_work.execute(updt_sql_stmt, params)

    db_conn_work.commit()
    curs_dict_work.close()
    curs_work.close()
    db_conn_work.close()

    # check if there are any remaining heading lines that have not been matched to a path
    sql_stmt = """
        select count(1) as cnt 
        from previews_lines 
        where pvh_id = %(pvh_id)s 
        and pv_type = 'HDG' 
        and pvhh_id is null;
    """
    params = {'pvh_id': pvh_id}
    curs_dict.execute(sql_stmt, params)
    row = curs_dict.fetchone()
    hdgs_not_found_count = row['cnt']

    if hdgs_not_found_count == 0:
        print(f'All headings were matched for pvh_id: {pvh_id}')
        return 0
    print(f'{hdgs_not_found_count} heading were not found... generating extract file')

    # query to extract the data to assign hrch hdg level numbers to unmatched headings
    sql_stmt = """
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
        ), valid_hdg_hrc as (
            select pvhh_id, hrch_level, detail_items_ind 
            from previews_hdg_hrch 
            where %(fn_period)s between valid_to and valid_from
        )
        select
            t1.row_num - 1 as row_num,     -- A  A
            t2.fn_name,                  -- B  B
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
        where t1.pvh_id = %(pvh_id)s
        order by t1.pvh_id, t1.pvl_seq;
    """
    params = {'pvh_id': pvh_id, 'fn_period': fn_period}
    curs.execute(sql_stmt, params)

    export_fn = export_dir + \
        '/' + f'hdg_hrch_export_{str(pvh_id).zfill(4)}_{datetime.now().strftime("%Y%m%d%H%M%S")}.txt'
    with open(export_fn, 'w') as fd:
        fd.write('row\tfile\tpvh_id\tpvl_seq\tpvl_id\tpg_nbr\thdg_lvl\t'
                 'DI_ind\tdup_pvl_id\tpvhh_id\theading\tformatted\n')
        for row in curs:
            fd.write('\t'.join([str(col or '') for col in row]) + '\n')

    curs_dict.close()
    curs.close()
    db_conn.close()

    print(f'the name of the extract file is:\n{export_fn}')

    return 0


def import_hdg_hrch_lvls_file(fn_name):
    #
    # step 1 - read file and insert into the import table, eg: hdg_hrch_import__0001_20200302220146
    # step 2 - on the import table, set the parent_pvl_id
    # step 3 - populate the previews_hdg_hrch table
    #           - for each rec in the import table
    #           - get the next pvhh_id from the simulated sequence object
    #           - lookup the corresponding parent_pvhh_id (previews_hdg_hrch) for the parent_pvl_id (import table)
    #           - insert the date from the import table into the previews_hdg_hrch table
    #
    # if you want to reprocess a given file, you will need to delete the data for that file from the
    #       previews_hdg_hrch table first. Else you'll bet some error like:
    #       mysql.connector.errors.InternalError: Unread result found

    db_conn = get_db_conn()
    curs = db_conn.cursor()

    table_nm = f'hdg_hrch_import_{fn_name[15:35]}'

    # drop the hdg_hrch_import_<pvh-id>_<dt-tm> if it exists (to make reprocessing easier)
    sql_stmt = f"drop table if exists {table_nm};"
    curs.execute(sql_stmt)

    # create hdg_hrch_import_<pvh-id>_<dt-tm>
    sql_stmt = f"create table {table_nm} like hdg_hrch_import_template;"
    curs.execute(sql_stmt)

    #
    # Step 1 open the updated export file and insert each line into the import table
    #

    # insert into the hdg_hrch_import_<pvh-id>_<dt-tm>
    sql_stmt = f"""
        insert into {table_nm} (
            row_num,
            fn_name,
            pvh_id,
            pvl_seq,
            pvl_id,
            pg_nbr,
            hdg_lvl,
            detail_items_ind,
            dup_pvl_id,
            pvhh_id,
            line_text,
            indent
        ) values (
            %(row_num)s, 
            %(fn_name)s, 
            %(pvh_id)s, 
            %(pvl_seq)s, 
            %(pvl_id)s, 
            %(pg_nbr)s, 
            %(hdg_lvl)s, 
            %(detail_items_ind)s, 
            %(dup_pvl_id)s, 
            %(pvhh_id)s, 
            %(line_text)s, 
            %(indent)s
        );
    """

    # open the updated export file that will be loaded into the import table
    import_fn = export_dir + '/' + fn_name
    with open(import_fn, 'r') as fh:
        fh.readline()
        for line in fh:
            row = line.strip('\n').split('\t')

            # the following is needed if using Excel to save the updated file
            # as it will put quotes around a field if it contains a comma,
            # even though the file is tab separated (TSV) and not comma separated (CSV)
            if row[10][:1] == '"' and row[10][-1:] == '"' and ',' in row[10]:
                row[10] = row[10][1:-1]
                row[11] = row[11][1:-1]

            detail_items_ind = row[7].upper() or None
            if detail_items_ind == 'Y':
                detail_items_ind = True
            elif detail_items_ind == 'N':
                detail_items_ind = False

            # insert the line from the file to the hdg_hrch_import_<pvh-id>_<dt-tm> table
            params = {
                'row_num': row[0],
                'fn_name': row[1],
                'pvh_id': row[2],
                'pvl_seq': row[3],
                'pvl_id': row[4],
                'pg_nbr': row[5],
                'hdg_lvl': row[6] or None,
                'detail_items_ind': detail_items_ind,
                'dup_pvl_id': row[8] or None,
                'pvhh_id': row[9] or None,
                'line_text': row[10],
                'indent': row[11] or None
            }
            curs.execute(sql_stmt, params)

    #
    # Step 2 - set the parent column on the loaded export file
    #

    # SQL to update the parent_pvl_id (on the import table) based on the hdg_lvl and row_num
    sql_stmt = f"""
        with base as (
            select
                pvl_id,
                hdg_lvl,
                row_num
            from {table_nm}
            where hdg_lvl is not null
        ), candidates as (
            select
                b1.pvl_id,
                ifnull(b2.pvl_id, 0) as cand_parent_pvl_id,
                ifnull(b2.row_num, 0) as cand_parent_row_num
            from base b1
            left join base b2
            on b2.row_num < b1.row_num and b2.hdg_lvl = b1.hdg_lvl - 1
        ), parents as (
            select distinct
            pvl_id,
            first_value(cand_parent_pvl_id) over (
                partition by pvl_id order by cand_parent_row_num desc
            ) as parent_pvl_id
            from candidates
        )
        update {table_nm} t1
        join parents t2 on t2.pvl_id = t1.pvl_id
        set t1.parent_pvl_id = t2.parent_pvl_id;
    """
    curs.execute(sql_stmt)

    db_conn.commit()

    db_conn_slct = get_db_conn()
    curs_slct = db_conn_slct.cursor(dictionary=True)

    #
    # Step 3 - for each hdg_hrch_import record, look up the corresponding pvhh_id for the given pvl_id and insert
    #           into the previews_hdg_hrch table
    #

    # select statement for the hdg_hrch_import_<pvh-id>_<dt-tm> table that gets the needed data
    sql_stmt = f"""
        select
            t1.pvl_id,
            t1.parent_pvl_id,
            t1.hdg_lvl as hrch_lvl,
            t1.line_text as heading_nm,
            t1.detail_items_ind,
            t2.fn_period
        from {table_nm} t1
        join previews_hdr t2 on t1.pvh_id = t2.pvh_id
        where t1.pvhh_id is null
        and t1.hdg_lvl is not null
        order by t1.pvh_id, t1.pvl_seq;
    """
    curs_slct.execute(sql_stmt)

    # sql statement that simulates a database sequence object (not available in MySQL)
    get_pvhh_seq_sql_stmt = "update pvhh_seq set id = last_insert_id(id + 1);"

    # lookups the corresponding parent_pvhh_id on the previews_hdg_hrch table based on the parent_pvl_id on import table
    lookup_pvhh_id_sql_stmt = """
        select pvhh_id as parent_pvhh_id
        from previews_hdg_hrch
        where pvl_id = %(parent_pvl_id)s
        and %(fn_period)s between valid_from and valid_to;
    """

    # insert statement for the previews_hdg_hrch table
    ins_hdg_hrch_sql_stmt = """
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
            DEFAULT,               -- auto increment
            %(pvl_id)s,            -- from main select
            %(pvhh_id)s,           -- from pvhh_seq table
            %(parent_pvhh_id)s,    -- from separate lookup based on parent_pvl_id
            %(hrch_lvl)s,          -- from main select
            %(heading_nm)s,        -- from main select
            %(detail_items_ind)s,  -- from main select
            %(valid_from)s,        -- from main select
            '9999-12-31'
        );
    """

    # for each record in the hdg_hrch_import_<pvh-id>_<dt-tm> table
    for row in curs_slct:
        # get the next sequence value
        curs.execute(get_pvhh_seq_sql_stmt)
        pvhh_id = curs.lastrowid

        if row['parent_pvl_id'] == 0:
            # if the parent pvl id is 0, then the parent pvhh id will be 0
            parent_pvhh_id = 0
        else:
            params = {
                'parent_pvl_id': row['parent_pvl_id'],
                'fn_period': row['fn_period']
            }
            # lookup the parent pvvh id based on the parent_pvl_id and period
            curs.execute(lookup_pvhh_id_sql_stmt, params)
            parent_pvhh_id = curs.fetchone()[0]

        # insert the date into the previews_hdg_hrch (from the hdg_hrch_import_<pvh-id>_<dt-tm> table)
        params = {
            'pvl_id': row['pvl_id'],
            'pvhh_id': pvhh_id,
            'parent_pvhh_id': parent_pvhh_id,
            'hrch_lvl': row['hrch_lvl'],
            'heading_nm': row['heading_nm'],
            'detail_items_ind': row['detail_items_ind'],
            'valid_from': row['fn_period']
        }
        curs.execute(ins_hdg_hrch_sql_stmt, params)

    db_conn.commit()

    curs.close()
    curs_slct.close()
    db_conn.close()
    db_conn_slct.close()


def update_pv_item_lines_with_hhid():
    sql_stmt = """
        with
        headings as (
            select
                pvh_id,
                pvl_seq,
                pvhh_id
            from previews_lines
            where pvh_id = 1
            and pv_type = 'HDG'
        ),
        max_lines as (
            select
                pvh_id,
                max(pvl_seq) as max_line
            from previews_lines
            group by pvh_id
        ),
        ranges as (
            select * from (
                select
                    t1.pvh_id,
                    t1.pvl_seq as start_line,
                    ifnull(lead(t1.pvl_seq) over(order by t1.pvl_seq) - 1, t2.max_line)  as end_line,
                    t1.pvhh_id
                from headings as t1
                join max_lines as t2 on t2.pvh_id = t1.pvh_id
            ) t0 where end_line - start_line > 0
        ),
        calculated as (
            select
                t1.pvl_id,
                t2.pvhh_id
            from previews_lines as t1
            join ranges as t2
            on t1.pvh_id = t2.pvh_id
            and t1.pvl_seq between t2.start_line + 1 and t2.end_line
        )
        update previews_lines t1
        join calculated t2
        on t2.pvl_id = t1.pvl_id
        set t1.pvhh_id = t2.pvhh_id
        where t1.pv_type = 'ITEM';
    """
    print(sql_stmt)


def explode_line_text():
    db_conn = get_db_conn()
    curs = db_conn.cursor()

    db_conn_work = get_db_conn()
    curs_work = db_conn_work.cursor()

    sql_stmt = "select pvl_id, line_text from previews_lines where pv_type = 'ITEM' order by pvh_id, pvl_seq;"

    insrt_sql_stmt = """
        insert into previews_basic_dtl (
            pvb_id,
            pvl_id,
            promo_cd,
            sol_code,
            sol_text,
            release_dt,
            unit_price_raw        
        ) values (
            DEFAULT,
            %(pvl_id)s,
            %(promo_cd)s,
            %(sol_code)s,
            %(sol_text)s,
            %(release_dt)s,
            %(unit_price_raw)s
        );
    """

    curs.execute(sql_stmt)
    for pvl_id, line_txt in curs:
        try:
            fields = line_txt.split('\t')

            # due to an error on diamonds part, there are some months where there is an extra blank field
            # between the sol-code and sol-text fields, we need to remove it.
            if len(fields) > 7:
                fields = fields[:2] + fields[3:]

            # apparently, diamond forgets to put in the release date column for some items, so we need to add
            # a blank release date field for these lines.
            if len(fields) == 6 and ('$' in fields[3] or 'PI' in fields[3]):
                fields = fields[:3] + [''] + fields[4:]

            if len(fields) >= 4:
                promo_cd = fields[0] if len(fields[0]) != 0 else None
                sol_code = fields[1][:5] + fields[1][6:]
                sol_text = fields[2]
                if len(fields[3]) != 0:
                    release_dt = f'20{fields[3][6:]}-{fields[3][:2]}-{fields[3][3:5]}'
                else:
                    release_dt = None
                unit_price_raw = fields[4] if len(fields[4]) != 0 else None
            else:
                print(f'malformed item line text {pvl_id}')
                for i, f in enumerate(fields, 1):
                    print(f'\t[{i}  {f}')
                continue

            params = {
                'pvl_id': pvl_id,
                'promo_cd': promo_cd,
                'sol_code': sol_code,
                'sol_text': sol_text,
                'release_dt': release_dt,
                'unit_price_raw': unit_price_raw
            }
            curs_work.execute(insrt_sql_stmt, params)
        except Exception as err:
            print(err)
            raise

    db_conn_work.commit()

    curs.close()
    curs_work.close()
    db_conn.close()
    db_conn_work.close()


def get_db_conn() -> mysql.connector:
    db_conn = mysql.connector.connect(
        user='root', password='dothedew', host='127.0.0.1', database='previews'
    )
    return db_conn


def pivot_pv_type_counts():
    sql = """
        with counts as (
            select 
                pvh_id, 
                pv_type, 
                count(1) as cnt 
            from previews_lines 
            group by pvh_id, pv_type
        )
        select pvh_id,
            MAX(case pv_type when 'IDENT'  then cnt else null end) as IDENT,
            MAX(case pv_type when 'HDG'    then cnt else null end) as HDG,
            MAX(case pv_type when 'PAGE'   then cnt else null end) as PAGE,
            MAX(case pv_type when 'ITEM'   then cnt else null end) as ITEM,
            MAX(case pv_type when 'BLANK'  then cnt else null end) as BLANK,
            MAX(case pv_type when 'JUNK'   then cnt else null end) as JUNK
        from counts
        group by pvh_id
        order by pvh_id;
    """
    print(sql)


def list_txt_fn_inconsistencies():
    sql_stmts = """
        select * from previews_hdr where txt_mo is null or txt_mo <> fn_mo order by fn_period;
        select * from previews_hdr where txt_yr <> fn_yr order by fn_period;
        select * from previews_hdr where txt_volume <> fn_volume order by fn_period;
        select * from previews_hdr where txt_vol_issue <> fn_vol_issue order by fn_period;
        select * from previews_hdr where txt_issue <> fn_issue order by fn_period;
        select * from previews_hdr where txt_period <> fn_period order by fn_period;
        select * from previews_hdr where txt_name <> fn_name order by fn_period;
    """
    print(sql_stmts)


if __name__ == '__main__':
    # sys.exit(convert_files_encoding())
    t0 = datetime.now()
    log_cof_files()
    t1 = datetime.now()
    print(f'log_cof_files completed: {t1 - t0}')
    # load_line()
    # t2 = datetime.now()
    # print(f'load_line completed: {t2 - t1}')
    # set_pv_type()
    # t3 = datetime.now()
    # print(f'set_pv_type completed: {t3 - t2}')
    # set_page_nbr()
    # t4 = datetime.now()
    # print(f'set_page_nbr completed: {t4 - t3}')
    # resolve_heading_hierarchy(1)
    # t5 = datetime.now()
    # print(f'resolve_heading_hierarchy completed: {t5 - t4}')
    # import_hdg_hrch_lvls_file('hdg_hrch_export_0001_20200305203825.txt')
    # t6 = datetime.now()
    # print(f'import_hdg_hrch_lvls_file completed: {t6 - t5}')
    # explode_line_text()
    sys.exit(0)
