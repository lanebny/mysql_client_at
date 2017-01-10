"""
This module contains the logic for extracting entries 
from JSON SQL dictionaries, populating parameter settings
and using the settings to expand dictionary text into 
executable SQL statements. It doesn't depend on MySQL.
"""
from __future__ import print_function
import os.path
import sys
import glob
import json
import re
import enum
import copy
from   datetime import datetime


#                             M Y S Q L  S T A T E M E N T

class MySqlStatement(object):
    """
    A MySqlStatment object is basically a deserialized SQL dictionary entry. 
    Class methods consolidate all the json files in the SQL directory into
    a single dictionary. This is done each time the client requests a list
    of statements, so that he can edit the json and test it without having
    to restart the SQL explorer.
    """
    def __init__(self, sql_dictionary_path, statement_name, statement_dict):
        self.sql_dictionary_path = sql_dictionary_path
        self.sql_dictionary_file = os.path.basename(self.sql_dictionary_path)
        self.statement_name = statement_name
        if "statement_text" not in statement_dict:
            raise ValueError("Statement {} in {} is incomplete".
                             format(self.statement_name, self.sql_dictionary_path))
        self.statement_text = '\n'.join(statement_dict["statement_text"])
        self.description = '\n'.join(statement_dict["description"]) if "description" in statement_dict else None
        self.parameters = statement_dict.get("parameters", None)
            
    def get_parameters(self):
        """
        Create list of MySqlParameter objects from the parameter list
        imported from the json sql dictionary entry
        """
        if not self.parameters:
            return None
        parameters = []
        for param_dict in self.parameters:
            parameters.append(MySqlStatementParameter(param_dict))
        return parameters

    def get_statement_text(self, parameters):
        """
        Plug the parameter values into the statement text
        """
        executable_statement = copy.copy(self.statement_text)
        for parameter in parameters:
            if parameter.value is None:
                continue
            if parameter.param_type == MySqlStatementParameter.ParamType.MARKER:
                executable_statement, subs = re.subn(r"\?",
                                                     "{}".format(parameter),
                                                     executable_statement,
                                                     count=1) # just replace the first '?'
                if subs != 1:
                    raise ValueError("No placeholder in statement text for parameter {p.name}".
                                     format(p=parameter))
            elif parameter.param_type == MySqlStatementParameter.ParamType.SUBSTITUTE:
                parameter_value = format(parameter)
                if parameter_value.startswith("'"): # strip quotes if substituting
                    parameter_value = parameter_value[1:-1]
                executable_statement, subs = re.subn("@{p.name}".format(p=parameter),
                                                     parameter_value,
                                                     executable_statement)
                if subs == 0: # subs > 1 is OK, there may be multiple occurrences
                    raise ValueError("@{p.name} not found in statement text".
                                     format(p=parameter))
        return executable_statement

    @classmethod
    def _get_consolidated_sql_dict(cls, sql_dir, **kwargs):
        """
        Read all the json files in the SQL directory and create a single 
        dict of MySqlStatement objects, indexed by statement name. The values
        are lists, since it is possible that the same statement name may
        appear in more than one SQL dictionary
        """
        json_paths = glob.glob("{}/*.json".format(sql_dir))
        if not json_paths:
            raise ValueError("No json files found in {}".format(sql_dir))
        consolidated_sql_dict = {}
        for json_path in json_paths:
            with open(json_path) as json_file:
                sql_dict = json.load(json_file)
                if "statements" not in sql_dict:
                    print ("No 'statements' member found in {}: skipped".
                           format(json_path))
                    continue
            for statement_name, statement_dict in sql_dict["statements"].items():
                statement = MySqlStatement(json_path, statement_name, statement_dict)
                statement_list_for_name = consolidated_sql_dict.setdefault(statement_name, [])
                statement_list_for_name.append(statement)
        return consolidated_sql_dict

    @classmethod
    def get_statements(cls, pattern, sql_dir, **kwargs):
        """
        Return a sorted list of all the SQL statements in the
        consolidated dictionary whose names match the pattern
        """
        matches = []
        consolidated_sql_dict = cls._get_consolidated_sql_dict(sql_dir, **kwargs)
        for name, statements in consolidated_sql_dict.iteritems():
            if pattern is not None:
                match = re.search(pattern, name)
                if not match:
                    continue
            matches += statements
        matches.sort(key=lambda stmt: stmt.statement_name)
        return matches



#                      M Y S Q L  S T A T E M E N T  P A R A M E T E R

class MySqlStatementParameter(object):
    """
    Represents a SQL statement parameter. Populated from the JSON object
    in the 'parameters' list of SQL dictionary entry for the statement
    """
    class ParamType(enum.Enum):
        MARKER = 1,
        SUBSTITUTE = 2

    class DataType(enum.Enum):
        INT = 1,
        STRING = 2,
        FLOAT = 3,
        DATE = 4

    def __init__(self, param_dict):
        cls = type(self)
        self.name = param_dict["name"]
        self.param_type = cls.ParamType[param_dict["param_type"].upper()]
        self.data_type = cls.DataType[param_dict["data_type"].upper()]
        self.description = param_dict.get("description", None)
        self.regex = param_dict.get("regex", None)
        self.value = None

    def set_value(self, value):
        """
        Assign a value to the parameter. The value passed may be a string
        entered by the user. It's converted according to the type declared
        for the parameter.
        """
        cls = type(self)
        if self.data_type == cls.DataType.INT:
            try:
                self.value = int(value)
            except ValueError:
                raise ValueError("Attempt to assign non-integer '{!s}' to integer parameter {p.name}".
                                 format(value, p=self))
        elif self.data_type == cls.DataType.STRING:
            if self.regex is not None:
                matchesPattern = re.search(self.regex, value)
                if not matchesPattern:
                    raise ValueError("'{}' does not match pattern for {p.name}: {p.regex}".
                                     format(value, p=self))
            self.value = str(value)
        elif self.data_type == cls.DataType.FLOAT:
            try:
                self.value = float(value)
            except ValueError:
                raise ValueError("Attempt to assign non-float '{!s}' to float parameter {p.name}".
                                 format(value, p=self))
        elif self.data_type == cls.DataType.DATE:
            if isinstance(value, datetime):
                self.value = value
            else:
                try:
                    self.value = datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    raise ValueError("Unable to assign '{!s}' to date parameter {p.name}. Expect yyyy-mm-dd".
                                     format(value, p=self))

    def __format__(self, _):
        """
        Render the parameter value as a string. Used when replacing
        placeholders in the SQL statement text
        """
        cls = type(self)
        if self.value is None:
            return ''
        elif self.data_type == cls.DataType.INT:
            return str(self.value)
        elif self.data_type == cls.DataType.FLOAT:
            return str(self.value)
        elif self.data_type == cls.DataType.STRING:
            return "'{}'".format(self.value)
        elif self.data_type == cls.DataType.DATE:
            return "'{}'".format(datetime.strftime(self.value, "%Y-%m-%d"))
            
    def __repr__(self):
        logstrings = [self.name]
        if self.value is not None:
            logstrings.append(" = {}".format(self))
        return ''.join(logstrings)

