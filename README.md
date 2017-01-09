
# MySQL Client AT #

**mysql\_client\_at** is a high-level MySql client api that features support for auditing and testing.   

It provides solutions to a number of problems associated with conventional applications

##Features##

* **Separates code and SQL**. All SQL resides in JSON statement dictionaries. The client executes a statement by passing the statement name and parameter settings to the framework.
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
rapidjson. Tested with v1.1.0  
boost libraries  log, thread. regex, program\_options, filesystem, datetime, log\_setup, system, chrono, atomic.  Tested with  v1.60.0-2  
cmake. Tested with  v3.6.2  

**For testing**  
MySQL Employees sample database  
Python 2.7 or higher  
MySQL Connector/Python. Tested with v2.1.5  

###To install###
` git clone https://github.com/lanebny/mysql_client_at`  
 `cd mysql_client_at`   
 `git clone https://github.com/google/googletest`  
`mkdir build`  
`cd build`  
`cmake ..`    
`make`  

###To explore SQL libraries###
`cd ../python`  
`python sql_explorer.py`
 
You will be asked to supply MySQL connection information (user, password, host). These will be saved for future runs.  
At this prompt:  
>
`Options:`  
&nbsp; &nbsp;`d dbname    Connect to a database`      
&nbsp; &nbsp;`x [pattern] Select and execute a statement. If more than one`  
&nbsp; &nbsp;&nbsp; &nbsp;&nbsp;`statement name contains the pattern, or the pattern`   
&nbsp; &nbsp;&nbsp; &nbsp;&nbsp;`is omitted, you will be presented with a list  
		of statements to choose from.`     
`Enter an option code or nothing to quit:`


