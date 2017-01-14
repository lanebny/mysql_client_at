"""
sql_explorer is an interactive script for viewing and executing
SQL statements stored in JSON sql dictionaries
"""
from __future__ import print_function
import os
import sys
import re
import enum
from   collections import OrderedDict
import cPickle
import time

from connection import MySqlConnection
from sql_statement import MySqlStatement

CONFIG = OrderedDict([("user", "root"),
                      ("password", None),
                      ("host", "127.0.0.1"),
                      ("port", 3306),
                      ("autocommit", True),
                      ("sql_dir", "../sql")])
CONFIG_PATH = "sql_explorer.cfg"

class LineColors(enum.Enum):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    GRAY = '\033[90m'
    FAIL = '\033[91m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'

def main():
    """
    Set up the config for this session, then loop
    accepting user commands
    """
    setup_config()
    try:
        input_loop()
    except Exception as err:
        print(str(err))
        sys.exit(1)

def setup_config():
    """
    Retrieve config saved by the last session, if any,
    show the settings to the user, and give him the
    opportunity to change them. Config settings include
    the directory where the SQL dictionaries are stored,
    and the credentials for connecting to MySQL.
    """
    global CONFIG
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as config_file:
            try:
                CONFIG = cPickle.load(config_file)
            except:
                pass
    config_prompt_lines = ["\n{c.HEADER.value}Current config settings are:{c.ENDC.value}"
                           .format(c=LineColors)]
    for key, value in CONFIG.items():
        config_prompt_lines.append("  {} = {!s}".format(key, value))
    config_prompt_lines.append("Enter 'm' to modify these settings: ")
    response = raw_input('\n'.join(config_prompt_lines)).strip()
    if response != 'm':
        return
    for key, current_value in CONFIG.items():
        while True:
            response = raw_input("{} (currently {!s}) : ".format(key, current_value)).strip()
            if not response:
                break
            current_value = CONFIG[key]
            try:
                if isinstance(current_value, bool):
                    new_value = response.lower().startswith('t')
                else:
                    new_value = type(current_value)(response) if current_value is not None else response
                CONFIG[key] = new_value
                break
            except ValueError:
                print("{c.FAIL.value}{} is not a valid value for {}{c.ENDC.value}"
                      .format(response, key, c=LineColors))
    with open(CONFIG_PATH, 'w') as config_file:
        cPickle.dump(CONFIG, config_file)
    
def input_loop():
    """
    Display a commented list of available options and
    prompt the user to choose one
    """
    initial_prompt = """
\n{c.HEADER.value}Options:{c.ENDC.value}
    d dbname    Connect to a database
    x [pattern] Select and execute a statement. If more than one 
                statement name contains the pattern, or the pattern 
                is omitted, you will be presented with a list
		of statements to choose from.    
Enter an option code or nothing to quit: """.format(c=LineColors)

    while True:
        response = raw_input(initial_prompt).strip()

        # connect to a database
        if response.startswith('d'):
            connect_to_database(response)
            continue

        # select and execute a SQL statement
        elif response.startswith('x'):
            statement = choose_statement(response)
            if statement is None:
                continue
            parameter_settings = get_parameter_settings(statement)
            if parameter_settings is None:
                continue
            try:
                statement_text = statement.get_statement_text(parameter_settings)
            except Exception as err:
                print("{c.FAIL.value}Error generating SQL: {!s}{c.ENDC.value}".format(err, c=LineColors))
                continue
            execute_prompt = make_execute_prompt(statement, statement_text)
            confirmed = False
            while True:
                response = raw_input(execute_prompt).strip()
                if not response:
                    break
                if response == 'y':
                    confirmed = True
                    break
            if not confirmed:
                continue
            if not MySqlConnection.get_current_connection():
                print("You are not currently connected to a database.")
                if not connect_to_database():
                    continue
            execute_statement(statement, parameter_settings, statement_text)
            continue

        # entered nothing: exit
        elif not response:
            return 0

def connect_to_database(response=None):
    """
    Let the user choose a database anc connect to it.
    Connections are cached by the MySqlConnection class,
    so we create at most one connection for any database.
    """
    if response is None:
        dbname = raw_input("Enter a database name: ").strip()
        if not dbname:
            return False
    else:
        match_dbname = re.search(r"^d\s+(\w+)", response)
        if match_dbname is None:
            return False
        dbname = match_dbname.group(1)
    try:
        MySqlConnection.connect(dbname, **CONFIG)
        print("{c.OKGREEN.value}Connected to {}{c.ENDC.value}".format(dbname, c=LineColors))
        return True
    except Exception as err:
        print("{c.FAIL.value}Error connecting to {}: {!s}{c.ENDC.value}".format(dbname, err, c=LineColors))
        return False

def choose_statement(user_input):
    """
    The user has entered 'x', to execute a SQL statement from one
    of the dictionaries. The 'x' can be followed by a pattern for
    filtering the statement names.
    We call the MySqlStatement class method 'getStatements' to get a
    list of matches, allow the user to select one, and return the
    selection.
    """
    statement = None
    xmatch = re.search(r"x\s+([\w_]+)", user_input)
    pattern = xmatch.group(1) if xmatch else None
    statements = MySqlStatement.get_statements(pattern, **CONFIG)
    if len(statements) == 0:
        print("{c.FAIL.value}No statements match '{}'{c.ENDC.value}".format(pattern, c=LineColors))
        return None
    elif len(statements) == 1:
        return statements[0]
    else:
        pick_statement_prompt = make_pick_statement_prompt(statements)
        while True:
            response = raw_input(pick_statement_prompt)
            if not response:
                return None
            try:
                index = int(response)-1
                if index < 0 or index >= len(statements):
                    raise ValueError
                statement = statements[index]
                return statement
            except:
                print("{c.FAIL.value}'{}' is not a valid statement number{c.ENDC.value}"
                      .format(response, c=LineColors))

def make_pick_statement_prompt(statements):
    """
    Generate a numbered list of the statements
    """
    prompt_lines = ["\n{c.HEADER.value}{:d} statements found:{c.ENDC.value}"
                    .format(len(statements), c=LineColors)]
    for i in range(len(statements)):
        prompt_strings = []
        prompt_strings.append("   {:<3d}: {s.statement_name}  ({s.sql_dictionary_file})"
                              .format(i+1, s=statements[i]))
        if statements[i].description is not None:
            description_lines = statements[i].description.splitlines()
            for description_line in description_lines:
                prompt_strings.append("\n{}{c.GRAY.value}{}{c.ENDC.value}".
                                      format(8*' ', description_line, c=LineColors))
        prompt_lines.append(''.join(prompt_strings))
    prompt_lines.append("Enter a statement number, or nothing to quit: ")
    return '\n'.join(prompt_lines)

def get_parameter_settings(statement):
    """
    Display a numbered list of parameters for the statement.
    When the user selects a parameter, prompt for the setting.
    Loop until all the parameters have been assigned values.
    """
    parameters = statement.get_parameters()
    while True:
        prompt = make_pick_parameter_prompt(statement, parameters)
        if prompt is None: # all parameters have values
            return parameters
        response = raw_input(prompt)
        if not response:
            return None
        elif response.strip() == 'x':
            return parameters
        else:
            parameter = None
            parameter_value = None
            try:
                parameter_index = int(response)-1
                if parameter_index < 0 or parameter_index >= len(parameters):
                    raise ValueError
                parameter = parameters[parameter_index]
            except ValueError: # not a parameter number -- must be next value
                for parameter in parameters:
                    if parameter.value is None:
                        break
                if parameter is None:
                    print("{c.FAIL.value}'{}' is not a valid parameter number{c.ENDC.value}".format(response, c=LineColors))
                    continue
                else:
                    parameter_value = response
            if parameter_value is None:
                parameter_value = raw_input("Enter a value for {p.name}: ".format(p=parameter))
            try:
                parameter.set_value(parameter_value)
            except ValueError as err:
                print(LineColors.FAIL.value + str(err) + LineColors.ENDC.value)

def make_pick_parameter_prompt(statement, parameters):
    """
    Generate prompt for parameter setting. First choice is to assign a value
    to the first parameter that does not already have a value. The user can
    also select a parameter by number, or execute the statement with the
    current values. Return None if all the parameters have values
    """
    prompt_lines = []
    next_parameter = None
    for i in range(len(parameters)):
        parameter = parameters[i]
        if next_parameter is None and parameter.value is None:
            next_parameter = parameter
        prompt_lines.append("   {:<2d}: {!r}".format(i+1, parameter))
    input_line = []
    if next_parameter is None:
        return None
    else:
        input_line.append("Enter {c.OKGREEN.value}{p.name}{c.ENDC.value}: "
                          .format(p=next_parameter, c=LineColors))
    input_line.append("a parameter number, 'x' to execute, or nothing to quit: ")
    prompt_lines.append(''.join(input_line))
    prompt = "\n{c.HEADER.value}{s.statement_name} takes {0:d} parameter{1}:{c.ENDC.value}\n". \
             format(len(parameters), ('' if len(parameters) == 1 else 's'), c=LineColors, s=statement)
    return prompt + '\n'.join(prompt_lines)

def make_execute_prompt(statement, statement_text):
    """
    Display the statement text and have the user confirm that
    he wants to execute it
    """
    prompt_lines = []
    prompt_lines.append("\n{c.HEADER.value}Ready to execute {s.statement_name}".
                        format(c=LineColors, s=statement))
    conn = MySqlConnection.get_current_connection()
    if conn is not None:
        prompt_lines.append(" on {conn.dbname}".format(conn=conn))
    prompt_lines.append(":\n\n{c.ENDC.value}".format(c=LineColors))
    prompt_lines.append("{c.OKGREEN.value}{}{c.ENDC.value}".format(statement_text, c=LineColors))
    prompt_lines.append("\n\nEnter 'y' to execute, nothing to quit: ")
    return ''.join(prompt_lines)

def execute_statement(statement, parameter_settings, statement_text):
    """
    Pass the text of the statement, with all placeholders replaced with
    the user's values, to the current connection for execution. The connection
    returns a named tuple that includes members 'rows_affected', 'rows_returned',
    and 'rows'. The 'rows' member is a list of sequences, starting with a
    list of column names, and continuing with value tuples. Errors are
    passed back as exceptions.
    """
    connection = MySqlConnection.get_current_connection()
    try:
        start = time.time()
        results = connection.execute_statement(statement_text)
        elapsed = time.time() - start;
        if results.rows_returned:
            print_rows(results.rows)
        print("{c.OKGREEN.value}{:.3f} sec. Rows affected {r.rows_affected:d}, rows returned {r.rows_returned:d}{c.ENDC.value}"
              .format(elapsed, r=results, c=LineColors))
    except Exception as err:
        print("{c.FAIL.value}Error executing {s.statement_name}: {!s}{c.ENDC.value}".
              format(err, s=statement, c=LineColors))

def print_rows(rows):
    """
    Render a row set as grid if rows are not too wide,
    otherwise as blocks, each column on a separate line
    """
    if len(rows) == 0:
        return
    column_names = rows[0]
    column_count = len(column_names)
    hidden_columns = [cn for cn in column_names if cn.startswith('_')]
    color_column = column_names.index("_color") if "_color" in column_names else None
    # first pass through rows to determine column widths
    visible_columns = OrderedDict([(column_name, [len(column_name), column_names.index(column_name)])
                      for column_name in column_names if column_name not in hidden_columns ])
    max_column_name_width = max((v[0] for v in visible_columns.values()))
    for row in rows[1:]:
        for column_name in column_names:
            if column_name in visible_columns:
                width, index = visible_columns[column_name]
                visible_columns[column_name][0] = max(width, len(str(row[index])))
    line_length = sum([v[0] for v in visible_columns.values()]) + 2 * len(visible_columns)
    row_strings = []
    # print as grid
    if line_length < 180:
        heading_strings = ["\n"]
        for column_name, [width, index] in visible_columns.items():
            padding = (width - len(column_name))*' '
            heading_strings.append(" {}{} ".format(column_name, padding))
        row_strings.append(''.join(heading_strings))
        row_strings.append('-' * line_length)
        for row in rows[1:]:
            value_strings = []
            color = row[color_column] if color_column is not None else None
            if color == "red":
                value_strings.append("{c.FAIL.value}".format(c=LineColors))
            elif color == "green":
                value_strings.append("{c.OKGREEN.value}".format(c=LineColors))
            for column_name, [width, index] in visible_columns.items():
                value = row[index]
                value_string = str(value) if value is not None else ''
                padding = (width - len(value_string))*' '
                if isinstance(value, int) or isinstance(value, float):
                    value_strings.append(" {}{} ".format(padding, value_string))
                else:
                    value_strings.append(" {}{} ".format(value_string, padding))
            if color is not None:
                value_strings.append("{c.ENDC.value}".format(c=LineColors))
            row_strings.append(''.join(value_strings))
    # print column-per-line
    else:
        for row in rows[1:]:
            row_strings.append("\n")
            color = row[color_column] if color_column is not None else None
            color_start = ''
            if color == "red":
                color_start = "{c.FAIL.value}".format(c=LineColors)
            elif color == "green":
                color_start = "{c.OKGREEN.value}".format(c=LineColors)
            color_end = "{c.ENDC.value}".format(c=LineColors) if color else ''
            for column_name, [width, index] in visible_columns.items():
                if row[index] is None or row[index] == '':
                    continue
                value_strings = [color_start, column_name]
                value_strings.append((max_column_name_width - len(column_name))*' ')
                value_strings.append("  {!s}".format(row[index]))
                value_strings.append(color_end)
                row_strings.append(''.join(value_strings))
    print ('\n'.join(row_strings))

if __name__ == "__main__":
    sys.exit(main())
