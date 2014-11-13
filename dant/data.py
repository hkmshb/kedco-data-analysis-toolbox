"""
Defines data structures to help with data analysis.
"""
import os
import xlrd



class XlSheet(object):
    """Represents a thin wrapper over the xlrd.sheet.Sheet object.
    
    It's especially useful for iterating over the content of a worksheet where
    rows are presented as lists
    
    :: source: this can either be a xlrd.Book object or path to an .xls file.
    """
    
    def __init__(self, source, sheet_name):
        workbook = source if type(source) is xlrd.book.Book else None
        if not workbook and type(source) is str:
            if not os.path.isfile(source):
                raise IOError('File not found: %s' % (source,))
            workbook = xlrd.open_workbook(source)
        
        if not workbook:
            raise ValueError(
                "Object not 'xlrd.book.Book'. Invalid object type: %s" %
                (type(source),)
            )
        
        if not sheet_name in workbook.sheet_names():
            raise ValueError("Sheet not found: %s" % (workbook.sheet_names()))
        self._sheet = workbook.sheet_by_name(sheet_name)
        self.sheet_name = sheet_name
        self.__rows_gen = None
    
    @property
    def nrows(self):
        return self._sheet.nrows
    
    @property
    def ncols(self):
        return self._sheet.ncols
    
    def getrows(self):
        def rows_gen():
            for i in range(self.nrows):
                row = []
                for j in range(self.ncols):
                    value = self._sheet.cell_value(i, j)
                    row.append(value.strip() if type(value) is str else value)
                yield row
        
        if self.__rows_gen is None:
            self.__rows_gen = rows_gen()
        return self.__rows_gen
     
    def getrow(self):
        return self.getrows().next()
