
# MySQL Client AT #

**A high-level MySQL client api with plug-in support for auditing and unit-testing.**   

Writing database applications often involve compromises with what many would view as best practices. It would be nice to have clean separation between interface and implementation, a complete, searchable audit trail, and robust, comprehensive tests, but who has the time? So it's common to find applications where SQL mingles with procedural code, where tracing a database change back to the agent that caused it involves hunting through logs, and where tests, if they exist at all, use hand-coded mocks that often fail to reflect the current database structure.

MySQL Client AT is a lightweight,  easy-to-use framework for building C++ applications that comply with best practices. The framework uses a plug-in architecture, so that applications only pay for the functionality they use. 

##Separation of Interface and Implementation##
All SQL statements are stored in JSON files called *SQL dictionaries*. A dictionary entry includes the statement's name and description, the SQL text, and a list of parameters. When an application sets up a database connection, it supplies a database name and SQL dictionary path, and it executes a statement by passing in its name and a list of parameter settings. 

##Audit##
![Audit Summary](https://github.com/lanebny/mysql_client_at/python/audit_summary.PNG)


##Other Features##

* **Built on the efficient binary (prepared-statement) interface**. All SQL resides in JSON statement dictionaries. The client executes a statement by passing the statement name and parameter settings to the framework.
* **Handles parameter binding automatically**. A statement's parameters are defined in the JSON dictionary. 
* **Supports text-substitution parameters**. Both MySql and text-substitution parameters are supported. A substitution parameter specifies a token in the SQL text that is replaced by the caller's value.
* **Automatically re-uses statements** If the same statement is executed more than once in a program, the statement handle will automatically be re-used, so that no statement is prepared more than once.
* **Prevents SQL injection**. A parameter declaration can include a regular expression that the value must match.
* **Runs in synchronous or asynchronous mode**: The API is the same, only in asynchronous mode, execute calls don't block. (Results-retrieval calls can block.) 

##Installing and testing##

###Requirements###
**For framework**  
Unix. Tested with Cygwin 2.876  
MySQL. Tested with MariaDB 15.1  
cmake. Tested with  v3.6.2  
boost libraries  
&nbsp; &nbsp;log   
&nbsp; &nbsp;thread  
&nbsp; &nbsp;regex   
&nbsp; &nbsp;program\_options   
&nbsp; &nbsp;filesystem   
&nbsp; &nbsp;datetime   
&nbsp; &nbsp;log\_setup   
&nbsp; &nbsp;system   
&nbsp; &nbsp;chrono   
&nbsp; &nbsp;atomic   
Tested with  v1.60.0  

**For testing**  
[MySQL Employees sample database](https://dev.mysql.com/doc/employee/en/employees-installation.html)  
Python 2.7 or higher  
MySQL Connector/Python. Tested with v2.1.5  

Google Test and rapidjson are copied from github during the install process  

###To install###
` git clone https://github.com/lanebny/mysql_client_at`  
 `cd mysql_client_at`   
 `git clone https://github.com/miloyip/rapidjson`   
`git clone https://github.com/google/googletest`   

The project sets up cmake for *out-of-source* builds. That is, the build-related files -- make files, libraries, and executables -- do not go into source directories, rather into new directories with the same names under a parent `build` directory.   
 
`mkdir build`  
`cd build`  
`cmake ..`    
`make`  

<a name="python"></a>
###Trying out SQL statements###
The project includes a Python script called `sql_explorer` that allows you to browse the JSON SQL libraries and to interactively execute SQL statements. The libraries are reloaded after each execution so you can write new statements and test existing ones without leaving the explorer.

`cd ../python`  
`python sql_explorer.py`
 
You will be asked to supply MySQL connection information (user, password, host). These will be saved for future runs.  
Then this prompt will appear:  
>
`Options:`  
&nbsp; &nbsp;`d dbname    Connect to a database`      
&nbsp; &nbsp;`x [pattern] Select and execute a statement. If more than one`  
&nbsp; &nbsp;&nbsp; &nbsp;&nbsp;`statement name contains the pattern, or the pattern`   
&nbsp; &nbsp;&nbsp; &nbsp;&nbsp;`is omitted, you will be presented with a list  
		of statements to choose from.`     
`Enter an option code or nothing to quit:`  

Enter `d employees` to select the test database, then enter `x` to see a list of all the SQL statements that have been installed with framework. A good query to start with is `sample\_employees`, which returns the first N rows in the `employees` table.   

The audit queries take a table-name as a parameter. After you run the gtest in the next section the database will contain a table of audit records called `audit_test`. You can run the audit queries against this table.

###Exercising the framework###

It's good practice to package database operations in libraries that hide the SQL and expose high-level business functions.  Besides providing intuitive APIs and re-usable functionality, this allows tests to exercise exactly the same code that the production applications use.  

The installation process generated one such library, `libemployees_db.a` from `examples/employees_db.cpp` . This library implements a single business function, `addEmployee`, which accepts information about a new hire, performs a number of validation queries (Is the employee number already in use? Is the department name valid? Does the salary make sense given the current salary range?) and then executes three INSERT statements as a single transaction to set up the employee.

The Google testcase for this library is in `tests/test_employee_db.cpp`. It confirms that all erroneous calls (e.g. adding an existing employee) fail as expected, then inserts a new employee. 

The testcase derives from `MySqlGtest`, defined in `include/gtest.h`. This class provides a command-line interpreter and a fixture that manages framework objects. One thing the fixture does is read connection information from a test-input JSON file. As installed, the input file specifies the user as `root`, with no password, connecting to MySQL on localhost. If you want to change any of this information, edit `tests/test_employees_db_input.json` and then `make` to copy the revised file to `build/tests`.

Run the test as an integration test, meaning it connects to a MySQL service and accesses the actual database.

`cd build/tests`  
`./test_employees_db.exe --test_type integration --test_input test_employees_db_input.json`

This test should succeed. The messages generated by the expected errors will appear in the console output.  The test will generate an audit trail -- that is, a record of every statement execution will be stored in a new table `audit_test` in the employees database. You can view the audit trail by running audit queries in [`sql_explorer`](#python).

Now run the same test as a unit test (no MySQL interaction)

`./test_employees_db.exe --test_type unit --test_input test_employees_db_input.json`





