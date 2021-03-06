
#MySQL Client AT

##A high-level MySQL client api with plugin support for auditing and testing  
Getting database applications into production often involves making compromises with what many would view as best practices. It would be nice to have clean separation between interface and implementation, a complete, searchable audit trail, and robust, comprehensive tests, but who has the time? So it's common to find applications where SQL mingles with procedural code, where tracing database changes requires long, often inconclusive, hunts through multiple logs, and where tests, if they exist at all, use hand-coded mocks that can easily become obsolete as schemas change.

MySQL Client AT is a lightweight, flexible C++ framework for implementing high-quality database applications, providing a SQL repository and plugins for auditing  and testing. 

##Separation of Interface and Implementation
The framework is based on *SQL dictionaries*, self-documenting JSON files of named, parameterized SQL statements. A C++ application creates a connection object and installs one or more SQL dictionaries on it. Then it can execute dictionary statements by name, leaving the code SQL-free. Results are returned in JSON documents. 

A Python utility, `sql_explorer`, renders the collection of SQL dictionaries as a single repository. With the explorer you can execute existing statements and create new ones, so you can interactively test and tune your SQL statements without running the C++ modules that use them.   

##Auditing##
Auditing is handled by plugins that can be dynamically installed and removed at run-time . There can be multiple audit plugins installed at one time. Different audit tables may have different record layouts -- plugins compose INSERT statements based on the table definitions. For example, you might want to maintain a complete audit trail for certain critical functions, saving everything including parameter settings and results, while a routine audit might save only the statement name and time-of-day. 

The framework provides a number of ways to tag audit records to make searching easier. 
Any statement execution can include a comment, which will be included in any audit table that has a `comment` column. Also, you can group SQL statements together into *programs*, and record the program in the audit. Programs can be nested.  

The `audit` SQL dictionary includes statements meant to be run from the SQL explorer, to allow interactive audit search. Here is an example of the `audit_summary` query:

![Audit Summary](https://github.com/lanebny/mysql_client_at/blob/master/image/audit_summary.png)

##Testing
Testing for database applications starts with testable design, basically meaning a design that packages business functions in libraries. Given libraries of business functions, it is relatively easy to produce integration tests (tests that go against live databases) by linking the libraries into a test framework like Google Test, but not so easy to implement unit tests (fast, focused tests that don't require a database). Unit tests are important because they can be run after every commit, so that side-effect bugs can be caught as soon as they are introduced. 

MySQL Client AT solves the unit-test problem by allowing you to re-run any successful integration test as a unit test. If you install the `capture` plugin when you run an integration test it will serialize statement executions into JSON files, which can then be used to run the same test without connecting to MySQL. The framework provides a Google Test fixture which allows you to run the same binary as an integration test or a unit test just by changing a command-line option.


##Other Features##

* **Built on the efficient binary (prepared-statement) interface**. The framework  takes care of MySQL bindings, parameter buffers and data buffers.    
* **Handles parameter binding automatically**. The caller passes in tag-value pairs. The framework matches them against the parameter declarations in the SQL dictionary entry for the statement and creates the bindings.  
* **Supports text-substitution parameters**. Both MySQL (place-holder) and text-substitution parameters are supported. A substitution parameter specifies a token in the SQL text that is replaced by the caller's value.
* **Automatically re-uses statements** If the same statement is executed more than once in a program, the statement handle will automatically be re-used, so that no statement is prepared more than once.
* **Prevents SQL injection**. A parameter declaration can include a regular expression that the value must match.
* **Runs in synchronous or asynchronous mode**: The API is the same, only in asynchronous mode, execute calls don't block. By default, the audit plugin creates an asynchronous connection to write audit records.  
* **Connections can be debugged dynamically**: Attaching the `debug` plugin to a connection causes the inputs and outputs of every statement execution to be traced out.

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

Google Test and rapidjson are cloned from github during the install process  

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
    
 <img src="https://github.com/lanebny/mysql_client_at/blob/master/image/sql_prompt.png" width="500"  hspace="100"  /> 

Enter `d employees` to select the test database, then enter `x` to see a list of all the SQL statements that have been installed with framework. The SQL dictionary `employees.json` contain statements aimed at the `employees` test database. A good query to start with is `sample_employees`, which returns a random sample of rows in the `employees` table.   

The SQL dictionary `audit.json` contains statements that create, populate and query audit tabkes. After you run the gtest in the next section,9 the database will contain a table of audit records called `audit_test`. You can run the audit queries against this table.

###Exercising the framework###

It's good practice to package database operations in libraries that hide the SQL and expose high-level business functions.  APIs are intuitive, functionality is reusable, and tests can exercise exactly the same code that the production applications use.  

The installation process generated one such library, `libemployees_db.a` from `examples/employees_db.cpp` . Currently this library implements a single business function, `addEmployee`, which accepts information about a new hire, performs a number of validation queries (Is the employee number already in use? Is the department name valid? Does the salary make sense given the current salary range?) and then executes three INSERT statements as a single transaction to set up the employee.

The Google testcase for this library is in `tests/test_employee_db.cpp`. It confirms that all erroneous calls (e.g. adding an existing employee) fail as expected, then inserts a new employee. 

The testcase derives from `MySqlGtest`, defined in `include/gtest.h`. This class provides a command-line interpreter and a fixture that manages framework objects. One thing the fixture does is read connection information from a test-input JSON file. As installed, the input file specifies the user as `root`, with no password, connecting to MySQL on localhost. If you want to change any of this information, edit `tests/test_employees_db_input.json` and then `make` to copy the revised file to `build/tests`.

Run the test as an integration test, meaning it connects to a MySQL service and accesses the actual database.

`cd build/tests`  
`./test_employees_db.exe --test_type integration --test_input test_employees_db_input.json`

This test should succeed. The messages generated by the expected errors will appear in the console output.  The test will generate an audit trail -- that is, a record of every statement execution will be stored in a new table `audit_test` in the employees database. You can view the audit trail by running audit queries in [`sql_explorer`](#python).

Now run the same test as a unit test (no MySQL interaction)

`./test_employees_db.exe --test_type unit --test_input test_employees_db_input.json`





