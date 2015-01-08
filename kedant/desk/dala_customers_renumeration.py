"""
Quick and dirty scripts for customer analysis

  : Source of data is InterSwitch excel file. 
"""
import os, sys
import sqlite3
import pyodbc

from dant.data import XlSheet


# settings
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..','..')
)
TEST_DATA_DIR = os.path.join(BASE_DIR, 'test-data')



# create database (sqlite3)
DB_PATH = os.path.join(TEST_DATA_DIR, 'cust-db.sqlite3')

def create_db(db_path, force=False):
    if os.path.exists(db_path):
        if not force: return
        else: os.remove(db_path)
    
    DB_SCRIPT = """
    CREATE TABLE cust_active (
        id            INTEGER PRIMARY KEY,
        sn            INT,
        acctno        VARCHAR(20),
        acctname      VARCHAR(100),
        address       VARCHAR(250),
        meterno       VARCHAR(15),
        tariff        VARCHAR(2),
        mobile        VARCHAR(11),
        email         VARCHAR(50)
    );
    """
    conn = sqlite3.connect(db_path)
    conn.executescript(DB_SCRIPT)


def load_xl2db(xlfilepath, sheetname, header_cols, insfunc, start_row=0):
    """Extracts data from an Excel sheet and loads into a database table.
    
    insfunc: insert function used to load data into database
    """
    def is_header(row):
        hText = ('!!'.join(header_cols)).lower()
        rText = ('!!'.join([str(r) for r in row[:len(header_cols)]])).lower()
        return hText == rText
    
    #load sheet & file header
    sheet = XlSheet(xlfilepath, sheetname)
    row = sheet.getrow()
    while not is_header(row):
        row = sheet.getrow()
    
    # now perform data load
    for row in sheet.getrows(start_row=(start_row or 0)):
        insfunc(norm_row(row))


def norm_row(row):
    if row and row[0] != '' and not row[1].startswith('Total'):
        new_row = [int(row[0])]
        
        new_row.extend(
            [r.strip() for r in row[1:]]
        )
        print(new_row)
        return new_row
    return None


def do4sqlite3(dbpath, xlfilepath, sheetname, header_cols):
    # create the database
    create_db(dbpath, force=True)
    
    conn = sqlite3.connect(dbpath)
    text = ("INSERT INTO cust_active "
            "(sn, acctno, acctname, address, meterno, tariff, mobile, email) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    
    try:
        with conn:
            load_xl2db(
                xlfilepath, sheetname, header_cols,
                lambda r: conn.execute(text, r) if r else None
            )
    except Exception as ex:
        print('Error encountered: %s' % ex)
    finally:
        conn.close()
        print('Done!')


def do4books(xlfilepath, sheetname, header_cols, table):
    # connect to database
    conn = pyodbc.connect('driver={sql server};server=.\sqlexpress;'
                          'database=kedco;trusted_connection=yes;')
    text = "INSERT INTO %s (book) VALUES (?)" % (table,)
    try:
        with conn:
            load_xl2db(
                xlfilepath, sheetname, header_cols,
                lambda r: conn.execute(text, r[1:]) if r else None
            )
    except Exception as ex:
        print('Error encountered: %s' % ex)
    finally:
        conn.close()
        print('Done!')


def do4mssql(xlfilepath, sheetname, header_cols, table, isactive, bUnit):
    # connect to database
    conn = pyodbc.connect('driver={sql server};server=.\sqlexpress;'
                          'database=kedco;trusted_connection=yes;')
    text = "INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, %s, '%s')" % (
                table, (1 if isactive else 0), bUnit
           )
    try:
        with conn:
            load_xl2db(
                xlfilepath, sheetname, header_cols,
                lambda r: conn.execute(text, r) if r else None
            )
    except Exception as ex:
        print('Error encountered: %s' % ex)
    finally:
        conn.close()
        print('Done!')


def do4mssql_orbis(xlfilepath, sheetname, header_cols, table, start_row=0):
    def filter_row_cols(row):
        if not row: return
        new_row = row[:32] + [row[46]] + [row[49]]
        new_row[25] = new_row[25][:25]
        
        # handle combined phone numbers
        mobiles = new_row[20]
        if mobiles.find(',') != -1:
            mobile_list = [m.strip() for m in mobiles.split(',')]
            new_row[20] = mobile_list[0]
            new_row[21] = mobile_list[1]
        
        conn.execute(text, new_row)
    
    # connect to database
    conn = pyodbc.connect('driver={sql server}; server=.\sqlexpress;'
                          'database=kedco; trusted_connection=yes;')
    text = (("INSERT INTO %s VALUES "
             "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
             " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
             " ?,?,?,?)") % table)
    try:
        with conn:
            load_xl2db(
                xlfilepath, sheetname, header_cols,
                filter_row_cols, start_row=start_row
            )
    except Exception as ex:
        conn.commit()
        print('Error encountered: %s' % ex)
        sys.exit()
    finally:
        conn.close()
        print('Done!')


if __name__ == '__main__':
    BASE_DIR = "C:\Users\Klone\Documents\WorkDocuments\KEDCO\Dala Customers"
    
    # load active customers
    for x in ("NEW_BOOKS_FOR_DALA_ACCTS.xls",):
        xlfilepath = os.path.join(BASE_DIR, x)
        do4books(xlfilepath, 'new books', ['SN', 'BOOK'], '[tmp].[Books]')
        print('Done For %s!' % x)




