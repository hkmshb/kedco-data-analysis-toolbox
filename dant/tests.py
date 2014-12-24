"""
Defines unit tests for the data analysis toolbox.
"""
import os
import sqlite3
import unittest

from xlrd import open_workbook
from .data import XlSheet




TEST_DATA_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '..', 'test-data'
    )
)


class XlSheetTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        super(XlSheetTest, cls).setUpClass()
        cls._filepath = os.path.join(TEST_DATA_DIR, 'sample-cust.xls')
        cls._workbook = open_workbook(cls._filepath)
    
    def _is_generator(self, iterable):
        return (
            hasattr(iterable, '__iter__') and
            not hasattr(iterable, '__len__') 
        )
    
    def test_can_create_from_filepath(self):
        self.assertTrue(os.path.isfile(self._filepath))
        self.assertIsNotNone(XlSheet(self._filepath, 'active'))
    
    def test_can_create_from_XlrdBook(self):
        self.assertIsNotNone(self._workbook)
        self.assertIsNotNone(XlSheet(self._workbook, 'active'))
    
    def test_raises_error_for_invalid_filepath(self):
        invalid_path = r'c:\path\to\invalid\file.xls'
        self.assertFalse(os.path.isfile(invalid_path))
        with self.assertRaises(IOError):
            XlSheet(invalid_path, 'active')
    
    def test_raises_error_for_non_XlrdBook_object(self):
        invalid_book = object()
        with self.assertRaises(ValueError):
            XlSheet(invalid_book, 'active')
    
    def test_raises_error_for_invalid_sheetname(self):
        invalid_name = 'non-existing-sheet-name'
        with self.assertRaises(ValueError):
            XlSheet(self._workbook, invalid_name)
    
    def test_raises_error_for_invalid_sheetname2(self):
        invalid_name = 'bad-sheet-name'
        with self.assertRaises(ValueError):
            XlSheet(self._filepath, invalid_name)
    
    def test_can_retrieve_rows_and_cols_count(self):
        xlsheet = XlSheet(self._workbook, 'active')
        self.assertEqual(xlsheet.nrows, 11)
        self.assertEqual(xlsheet.ncols, 8)
    
    def test_can_iterate_rows_via_get_rows_generator(self):
        xlsheet = XlSheet(self._workbook, 'active')
        source = xlsheet.getrows()
        self.assertTrue(self._is_generator(source))
    
    def test_getrows_iterates_over_all_content(self):
        xlsheet = XlSheet(self._workbook, 'active')
        prev_row = None
        for row in xlsheet.getrows():
            self.assertNotEqual(prev_row, row)
            prev_row = row
    
    def test_getrow_reads_rows_one_at_a_time(self):
        xlsheet = XlSheet(self._workbook, 'active')
        row = xlsheet.getrow()
        self.assertEqual(row[0], 'KANO ELECTRICITY DISTRIBUTION COMPANY')
        
        row = xlsheet.getrow()
        self.assertEqual(row[0], 'DALA BUSINESS UNIT')
    
    def test_getrow_and_getrows_extract_from_same_pool(self):
        xlsheet = XlSheet(self._workbook, 'active')
        row = xlsheet.getrow()
        self.assertEqual(row[0], 'KANO ELECTRICITY DISTRIBUTION COMPANY')
        
        row = xlsheet.getrows().next()
        self.assertEqual(row[0], 'DALA BUSINESS UNIT')


class IntrospectingFile(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        super(IntrospectingFile, cls).setUpClass()
        cls._filepath = os.path.join(TEST_DATA_DIR, 'sample-texts.txt')
    
    def test_readline_reads_lines_one_at_a_time(self):
        with open(self._filepath) as f:
            line = f.readline()
            self.assertEqual(line.strip(), 'This is line 1')
            
            line = f.readline()
            self.assertEqual(line.strip(), 'This is line 2')
    
    def test_readline_and_readlines_extract_from_same_pool(self):
        with open(self._filepath) as f:
            line = f.readline()
            self.assertEqual(line.strip(), 'This is line 1')
            
            lines = f.readlines()
            self.assertEqual(lines[0].strip(), 'This is line 2')
            self.assertEqual(len(lines), 6)






if __name__ == '__main__':
    unittest.main()
