"""
Normalizes information from the Quad Orbis Dala Customer Renumeration exercise
into a form suitable for bill production.
"""
from __future__ import print_function
from __future__ import division


import os
import sys
import pyodbc

from os import path


# settings
BASE_DIR = path.abspath(path.join(path.dirname(__file__), '..', '..'))
sys.path.insert(0, path.join(BASE_DIR, 'library'))
for f in os.listdir(path.join(BASE_DIR, 'library')):
    sys.path.insert(0, path.join(BASE_DIR, 'library',f))


#+=============================================================================
#| CONSTANTS
#+=============================================================================

# for records not having number of rooms, we assume a default of 4
DEFAULT_ROOM_COUNT = 4

# invalid cell values
BAD_CELL_VALUES = (
    '.','-','--','_','-`','=','-=','=-','=', '&', '0','-0','0-'
)

# connection string
CONN_Q1 = pyodbc.connect('driver={sql server};server=.\sqlexpress;'
                      'database=kedco;trusted_connection=yes;')

CONN_Q2 = pyodbc.connect('driver={sql server};server=.\sqlexpress;'
                      'database=kedco;trusted_connection=yes;')

CONN_D = pyodbc.connect('driver={sql server};server=.\sqlexpress;'
                      'database=kedco;trusted_connection=yes;')

#+=============================================================================


from dolfin import Storage as _


# Tariff Table
_mkt = lambda cd, ch, r: _(code=cd, fixed_charge=ch, rate=r)

## MYTO 2014 Rates
Tariffs = (     # Tariffs list
    _mkt('R1',         0,  4.00),
    _mkt('R2',    666.89, 16.01),
    _mkt('R3',  30031.48, 25.80),
    _mkt('R4', 156407.47, 25.80),
    _mkt('C1',    666.89, 17.46),
    _mkt('C2',  27224.96, 23.98),
    _mkt('C3', 141794.66, 23.98),
    _mkt('D1',    866.95, 19.35),
    _mkt('D2', 139511.91, 25.14),
    _mkt('D3', 141794.66, 25.14),
    _mkt('A1',    666.89, 18.52),
    _mkt('A2',  62520.57, 18.52),
    _mkt('A3',  83360.76, 18.52),
    _mkt('L1',    866.95, 14.22)
)

Tariff = _({    # Tariffs dict
    t.code: t for t in Tariffs 
})


def generate_acct_number(book_number, start=0):
    bookno = book_number.replace('-','').replace('/','')
    if not bookno or len(bookno) != 6:
        raise ValueError("book_number cannot be empty or not of 6 digits")
    
    if not bookno.isdigit():
        raise ValueError("Invalid book number provided")
    
    start = 0 if None else (999 if start > 999 else start)
    for i in range(start, 1000):
        acctno = "{}{:0>3}".format(bookno, i)
        acctno = "%s%s" % (acctno, get_acct_number_seal(acctno))
        yield "{}/{}/{}/{}-01".format(
            acctno[:2], acctno[2:4], acctno[4:6], acctno[6:10]
        )


def get_acct_number_seal(acct_number):
    acctno = acct_number.replace('-','').replace('/','')
    if not acctno or len(acctno) != 9:
        raise ValueError("acct_number cannot be empty or not of 9 digits")
    
    if not acctno.isdigit():
        raise ValueError("Invalid account number provided")
    
    weight = 0
    for entry in zip(range(1, 10), acctno[:9]):
        weight += (entry[0] * int(entry[1])) 
    
    return str(weight % 10)


def _get_cust_name(fname, mname, lname):
    def _norm(name_):
        name = name_.strip().lower()
        
        if name in BAD_CELL_VALUES:
            return ""
        if name.find('&') != -1 or name.find('other') != -1:
            return ""
        
        # check if numeric
        try: float(name); return ""
        except: pass
        
        return name_.replace('=','').replace('/', '').strip()
    
    fullname = " ".join(
        [f for f in (_norm(fname), _norm(mname), _norm(lname)) if f]
    )
    return fullname.strip()


def _get_cust_address(build_num, street, settlement, ward):
    def _norm(value_):
        value = value_.strip().lower()
        if value in BAD_CELL_VALUES:
            return ''
        
        return (value_.replace('/','').replace(';', '')
                      .replace('"','').replace('-', '')
                      .replace('-`', '').strip())
    
    def _norm2(value):
        value = _norm(value)
        
        try: float(value); return ''
        except: pass
        
        return value
    
    address = ' '.join(
        [x for x in (_norm(build_num), _norm2(street), _norm2(settlement),
                     _norm2(ward)) if x]
    )
    return "%s, Kano, Kano State" % address


def _get_room_count(room_count):
    try: room_count = int(room_count)
    except: room_count = DEFAULT_ROOM_COUNT
    return abs(room_count or DEFAULT_ROOM_COUNT)


def _get_tariff(name, cust_type, room_count):
    """Returns the tariff based on provided customer type and number of rooms.
    
    Rules for customer type:
        a) Residential >>> R1: for {0 > #Rooms <= 2};  R2: for {#Rooms > 2}
        b) Commercial  >>> C1
        c) Others      >>> R2
        d) Industrial  >>> D1
        e) Government  >>> C1; ... A1: for schools/hospitals/water board 
    """
    room_count = _get_room_count(room_count)
    
    if cust_type.lower() == 'residential':
        if room_count > 0 and room_count <= 2:
            return Tariff.R1
        if room_count > 2:
            return Tariff.R2
    elif cust_type.lower() == 'commercial':
        return Tariff.C1
    elif cust_type.lower() == 'industrial':
        return Tariff.D1
    elif cust_type.lower() == 'government':
        if name.lower().find('school') != -1:
            return Tariff.A1
        return Tariff.C1
    else:
        return Tariff.R2


def _get_phone(phone1, phone2, mobile, street):
    def _norm(phone):
        phone = phone.replace('.','').replace('-','')
        
        # check if composed of digits
        if phone and not phone.isdigit() and phone[0] != "+":
            return ""
        
        while phone[:2] == '00':
            phone = phone[1:]
        
        if phone[:3] in ('070','080','081'):
            return (phone if len(phone) == 11 else
                        phone[:11] if len(phone) > 11 else "")
        
        if phone[:4] in ('0070', '0080', '0081'):
            return (phone[1:] if len(phone) == 12 else
                        phone[1:12] if len(phone) > 12 else "")
        
        if phone and phone[0] == "+":
            return phone[:15]
        
        if len(phone) > 11:
            return phone[:11]
        
        return ("" if len(phone) < 9 else phone)
    return (_norm(phone1) or _norm(phone2) or _norm(mobile) or _norm(street))


def _get_metern_number(meterno):
    meterno = meterno.strip()
    if meterno in BAD_CELL_VALUES or meterno.isalpha():
        return ""
    
    if not meterno.isalpha():
        m = meterno.replace('-','').replace('/','')
        if m.isalpha() and m.find('A') != -1 and m.find('V') != -1:
            return ""
    
    return meterno


def _fetch_cust_name(row):
    return _get_cust_name(
        row['FirstName'], row['MiddleName'], row['LastName']
    )


def _fetch_cust_address(row):
    return _get_cust_address(
        row['Building#'], row['Street'], row['Settlement'], row['Ward']
    )


def _fetch_room_count(row):
    return _get_room_count(row['#Rooms'])


def _fetch_tariff(row):
    return _get_tariff(
        _fetch_cust_name(row), row['CustType'], row['#Rooms']
    )


def _fetch_phone(row):
    return _get_phone(
        row['Phone1'], row['Phone2'], row['Mobile'], row['Street']
    )


def _provider(conn, table, columns=None, extra_clause=None, count=None):
    # build query text
    text = 'SELECT %s FROM %s' % (
        '*' if not columns else ', '.join(columns),
        table
    )
    
    if extra_clause:
        text += extra_clause
    
    def read_rows():
        # execute query
        cur = conn.cursor()
        cur.execute(text)
        
        lg_desc = cur.description
        fields = [f[0] for f in lg_desc]
        
        records = cur.fetchmany(count) if count else cur.fetchall()
        for r in records:
            yield dict(zip(fields, r))
    
    return read_rows()


def _acctno_provider(bk_provider):
    class Acct: 
        index, gen = (1000, None)
    
    def f():
        if Acct.index >= 999:
            Acct.index = -1
            book = bk_provider.next()
            Acct.gen = generate_acct_number(book["Book"], start=0)
        
        Acct.index += 1
        return Acct.gen.next()
    
    return f


#+=============================================================================
#| scripts functions 
#+=============================================================================


def sample_qorbis_table():
    for r in _provider(CONN_Q1, 'tmp.quadorbis'):
        print('%s >>> %s >>> %s >>> %s' % (
            _fetch_cust_name(r).title(),
            _fetch_cust_address(r).title(),
            _fetch_phone(r),
            _fetch_tariff(r).values()
        ))


def dml_runner(dml_provider):
    if not dml_provider:
        raise ValueError('dml_provider must be provided')
    
    # storage for operation results summary
    results = _(failed=0, passed=0, errors=[])
    print('')
    
    ln_count = 0
    cur = CONN_D.cursor()
    for dml in dml_provider():
        try:
            cur.execute(dml)
            results.passed += 1
            print('.', sep='', end='')
        except Exception as ex:
            results.failed += 1
            results.errors.append([ex, dml])
            print('F', sep='', end='')
        
        ln_count += 1
        if ln_count % 100 == 0:
            print('')
        if ln_count % 1000 == 0:
            print('{0} {1}'.format(ln_count, "*" * 80))
            CONN_D.commit()
            print(' '.join([x.strip() for x in dml.split('\n')]))
    
    CONN_D.commit()
    
    print("\nCount: %s | Passed: %s | Failed: %s" % (
        results.passed + results.failed, 
        results.passed, results.failed
    ))
    
    if results.errors:
        print("*" * 80)
        for entry in results.errors:
            print(str(entry[0]))
            print(entry[1])
            print("-" * 10 + "\n")


def dml_provider_builder(row_provider, dml_builder):
    def dml_generator():
        for row in row_provider:
            try:
                dml = dml_builder(row)
                yield dml
            except Exception as ex:
                print(ex)
                raise ex
    return dml_generator


#+============================================================================+


def _extract_all_qorbis_data_with_acctno_added():
    # providers
    bk_prov = _provider(CONN_Q1, 'tmp.Books', extra_clause=' ORDER BY book')
    cs_prov = _provider(CONN_Q2, 'tmp.QuadOrbis',
                        extra_clause=' WHERE (Id in (SELECT QOrbisId'
                                    +'               FROM tmp.newcustomers))'
                                    +' ORDER BY id')
    
    # acct# provider
    get_acctno = _acctno_provider(bk_prov)
    
    for cs_row in cs_prov:
        acctno = get_acctno()
        cs_row["AcctNo"] = acctno
        yield cs_row


def _build_dml_for_qorbis_data_having_acctno(row):
    name = _fetch_cust_name(row)
    tariff = _get_tariff(name, row['CustType'], row['#Rooms'])
    consumption = _fetch_room_count(row) * 25
    adc = consumption / 30
    
    idx = name.find("'")
    if idx != -1:
        name = name.replace("'", "''")
    
    return """
    UPDATE tmp.NewCustomers SET
        Name = '{1}'
      , AccountNo = '{2}'
      , Mobile = '{3}'
      , Tariff = '{4}'
      , TariffRate = '{5}'
      , FixedCharge = '{6}'
      , Consumption = '{7}'
      , ADC = '{8}'
    WHERE (QOrbisId = '{0}');
    """.format(row["Id"], 
               name, 
               row["AcctNo"],
               _fetch_phone(row), 
               tariff.code if tariff else None,
               tariff.rate if tariff else None,
               tariff.fixed_charge if tariff else None,
               consumption,
               adc)


def _extract_specific_qorbis_data_with_acctno_added(ids):
    # providers
    acctgen = generate_acct_number('32/55/42', start=74)
    cs_prov = _provider(CONN_Q2, 'tmp.QuadOrbis',
                        extra_clause=' WHERE Id in (%s) ORDER BY Id' % (
                            ', '.join(ids)
                        ))
    
    for cs_row in cs_prov:
        acctno = acctgen.next()
        cs_row["AcctNo"] = acctno 
        yield cs_row


def update_customer_info_and_tariff():
    # providers
    bk_prov = _provider(CONN_Q1, 'tmp.Books', extra_clause=' ORDER BY book')
    cs_prov = _provider(CONN_Q2, 'tmp.QuadOrbis',
                        extra_clause=' WHERE (Id in (SELECT QOrbisId'
                                    +'               FROM tmp.newcustomers))'
                                    +' ORDER BY id')
    
    def dml_generator():
        def build_dml(customer, acctno):
            name = _fetch_cust_name(customer)
            tariff = _get_tariff(name, customer['CustType'], customer['#Rooms'])
            consumption = _fetch_room_count(customer) * 25
            adc = consumption / 30
            
            idx = name.find("'")
            if idx != -1:
                name = name.replace("'", "''")
            
            return """
            UPDATE tmp.NewCustomers SET
                Name = '{1}'
              , AccountNo = '{2}'
              , Mobile = '{3}'
              , Tariff = '{4}'
              , TariffRate = '{5}'
              , FixedCharge = '{6}'
              , Consumption = '{7}'
              , ADC = '{8}'
            WHERE (QOrbisId = '{0}');
            """.format(customer["Id"], 
                       name, 
                       acctno, 
                       _fetch_phone(customer), 
                       tariff.code if tariff else None,
                       tariff.rate if tariff else None,
                       tariff.fixed_charge if tariff else None,
                       consumption,
                       adc)
        
        # get book for use in generating acct#
        get_acctno = _acctno_provider(bk_prov)
        
        for customer in cs_prov:
            acctno = get_acctno()
            try:
                dml = build_dml(customer, acctno)
                yield dml
            except Exception as ex:
                print(ex)
                raise ex
    
    dml_runner(dml_generator)
    print('Hurray! Done')


def update_specific_customer_info_and_tariff():
    ids = ['2863', '3510', '4574', '4871', '10107',
           '18985', '58360', '59590', '62595', '70814',
           '72193', '72436', '109039', '110267', '111545',
           '116517', '126654', '128169', '132809']
    
    dml_prov = dml_provider_builder(
        row_provider = _extract_specific_qorbis_data_with_acctno_added(ids),
        dml_builder = _build_dml_for_qorbis_data_having_acctno
    )
    dml_runner(dml_prov)


if __name__ == '__main__':
    update_specific_customer_info_and_tariff()






        