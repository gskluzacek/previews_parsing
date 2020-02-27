import os
import sys
import re
from datetime import date

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


cof_dir = '/Users/gregskluzacek/Documents/Development/github_repos/previews_parsing/cof_files'


def main():
    fn = '/Users/gregskluzacek/Downloads/FEB20_COF.txt'
    with open(fn, 'r') as f:
        for line in f:
            fields = line.strip('\n').split('\t')
            print(fields)


def log_cof_files() -> int:
    regex1 = r'^PREVIEWS (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).* V(?:OL)?\.? ?(\d\d) #(\d\d?)$'
    cregex1 = re.compile(regex1)

    regex3 = r'^(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).* V(?:OL)?\.? ?(\d\d) #(\d\d?)$'
    cregex3 = re.compile(regex3)

    regex4 = r'^PREVIEWS (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC).* (\d\d\d\d)$'
    cregex4 = re.compile(regex4)
    regex5 = r'^ISSUE #(\d\d\d) \(VOL\. (\d\d) #(\d\d?)\)$'
    cregex5 = re.compile(regex5)

    cof_files, sorted_cof_file = get_cof_files_in_sorted_order(cof_dir)

    db_conn = get_db_conn()
    db_conn.autocommit = True
    curs = db_conn.cursor(dictionary=True)

    for cof_file in sorted_cof_file:
        yr_nbr = cof_file.year
        mo_nbr = cof_file.month
        mo_str = cof_file.strftime('%b').upper()
        ident_line = None
        raw_ident = None
        ident_str = None
        try:
            with open(cof_dir + '/' + cof_files[cof_file], 'r') as fh:
                for i, line in enumerate(fh, 1):
                    line = line.strip()
                    if line == 'PREVIEWS PUBLICATIONS':
                        ident_typ = 40
                        ident_line = i
                        raw_ident = None
                        id_mo = mo_str
                        id_yr = str(yr_nbr)
                        id_vol = str(yr_nbr - 1990)
                        id_iss = str(mo_nbr).zfill(2)
                        id_run_iss = (int(id_vol) - 1) * 12 + int(id_iss) + 27
                        ident_str = f'PREVIEWS {id_mo}-{id_yr} ISSUE #{id_run_iss} (VOL {id_vol} #{id_iss})'
                        break
                    if line == 'PREVIEWS ORDER FORM':
                        line = fh.readline()
                        line = line.strip()
                        m = cregex3.fullmatch(line)
                        if m:
                            ident_typ = 20
                            ident_line = i
                            raw_ident = line
                            id_mo = m.group(1)
                            id_yr = yr_nbr
                            id_vol = m.group(2)
                            id_iss = m.group(3).zfill(2)
                            id_run_iss = (int(id_vol) - 1) * 12 + int(id_iss) + 27
                            ident_str = f'PREVIEWS {id_mo}-{id_yr} ISSUE #{id_run_iss} (VOL {id_vol} #{id_iss})'
                        else:
                            ident_typ = 2
                        break
                    m = cregex1.fullmatch(line)
                    if m:
                        ident_typ = 10
                        ident_line = i
                        raw_ident = line
                        id_mo = m.group(1)
                        id_yr = yr_nbr
                        id_vol = m.group(2)
                        id_iss = m.group(3).zfill(2)
                        id_run_iss = (int(id_vol) - 1) * 12 + int(id_iss) + 27
                        ident_str = f'PREVIEWS {id_mo}-{id_yr} ISSUE #{id_run_iss} (VOL {id_vol} #{id_iss})'
                        break
                    m = cregex4.fullmatch(line)
                    if m:
                        line2 = fh.readline()
                        line2 = line2.strip()
                        m1 = cregex5.fullmatch(line2)
                        if m1:
                            ident_typ = 30
                            ident_line = i
                            raw_ident = line + '|' + line2
                            id_mo = m.group(1)
                            id_yr = m.group(2)
                            id_run_iss = m1.group(1)
                            id_vol = m1.group(2)
                            id_iss = m1.group(3).zfill(2)
                            ident_str = f'PREVIEWS {id_mo}-{id_yr} ISSUE #{id_run_iss} (VOL {id_vol} #{id_iss})'
                        else:
                            ident_typ = 3
                        break
                print(f'{cof_files[cof_file]}\t{cof_file.strftime("%b-%Y")}\t{ident_typ}\t{ident_line}\t'
                      f'{raw_ident}\t{ident_str}')
                sql_stmt = """
                    insert into previews_hdr values (
                        DEFAULT, 
                        %(ident_type)s, 
                        %(ident_line)s, 
                        %(raw_indent)s, 
                        %(ident_str)s, 
                        %(ident_mo)s,
                        %(ident_yr)s,
                        %(running_issue)s,
                        %(volume)s,
                        %(issue)s,
                        %(fn_period)s,
                        %(file_name)s,
                        'LOGGED',
                        %(file_path)s
                    );
                """
                params = {
                    'ident_type': ident_typ,
                    'ident_line': ident_line,
                    'raw_indent': raw_ident,
                    'ident_str': ident_str,
                    'ident_mo': id_mo,
                    'ident_yr': id_yr,
                    'running_issue': id_run_iss,
                    'volume': id_vol,
                    'issue': id_iss,
                    'fn_period': cof_file,
                    'file_name': cof_files[cof_file],
                    'file_path': cof_dir
                }
                try:
                    curs.execute(sql_stmt, params)
                except Exception as err:
                    print(f'error on insert {err}')
                    return -1
        except Exception as err:
            print(f'got error on file {cof_files[cof_file]}')
            print(err)
    curs.close()
    db_conn.close()
    return 0


def get_cof_files_in_sorted_order(cof_files_dir):
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
                    raise Exception(f'invalid month string {mo_str}')
    sorted_cof_file = sorted(cof_files.keys())
    return cof_files, sorted_cof_file


def load_line() -> int:
    db_conn_wrk = get_db_conn()
    curs_wrk = db_conn_wrk.cursor()
    sql_stmt_ins = """
        insert into previews_lines values (DEFAULT, %(pvh_id)s, %(pvl_seq)s, %(line_txt)s);
    """
    sql_stmt_updt = """
        update previews_hdr set proc_sts = 'LOADED' where pvh_id = %(pvh_id)s;
    """

    db_conn = get_db_conn()
    curs = db_conn.cursor(dictionary=True)
    sql_stmt = """
        select pvh_id, ident_str, file_path, file_name from previews_hdr where proc_sts = 'LOGGED' order by fn_period;
    """
    curs.execute(sql_stmt)
    for row in curs:
        print(f'processing: {row["ident_str"]}, file: {row["file_name"]}')
        with open(row['file_path'] + '/' + row['file_name']) as fh:
            try:
                for pvl_seq, line in enumerate(fh, 1):
                    params = {
                        'pvh_id': row["pvh_id"],
                        'pvl_seq': pvl_seq,
                        'line_txt': line.strip()
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
    cof_files, sorted_cof_file = get_cof_files_in_sorted_order(cof_dir)
    for cof_file in sorted_cof_file:
        fn = cof_dir + '/' + cof_files[cof_file]
        encoding = get_encoding_type(fn, detector)
        print(cof_files[cof_file], encoding)
        if encoding != 'utf-8':
            with codecs.open(fn, 'rU', encoding) as sourceFile:
                with codecs.open(cof_dir + '/converted/' + cof_files[cof_file], 'w', 'utf-8') as targetFile:
                    for line in sourceFile:
                        targetFile.write(line)

    return 0


def db_tut():
    db_conn = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='previews')
    curs = db_conn.cursor(dictionary=True)
    curs.execute("UPDATE pvhh_seq SET id=LAST_INSERT_ID(id+1);")
    pvhh_id = curs.lastrowid
    print(pvhh_id)
    db_conn.commit()
    curs.close()
    db_conn.close()


def get_db_conn() -> mysql.connector:
    db_conn = mysql.connector.connect(
        user='root', password='root', host='127.0.0.1', database='previews'
    )
    return db_conn


if __name__ == '__main__':
    # sys.exit(convert_files_encoding())
    # sys.exit(log_cof_files())
    sys.exit(load_line())
