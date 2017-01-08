#include <cstdio>
#include <cassert>
#include <iostream>
#include <sstream>
#include <fstream>

#include <rapidjson/error/error.h>
#include <rapidjson/error/en.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "connection.h"
#include "connection_impl.h"
#include "execution.h"



//                            M Y S Q L  C O N N E C T I O N  I M P L

MySqlConnectionImpl::MySqlLibrary library_;  // singleton do MySQL library setup and cleanup


// Class that wraps the MySQL-specifc logic
MySqlConnectionImpl::MySqlConnectionImpl(MySqlConnection * conn,
                                         const char *      databaseName, 
                                         const char *      statementPath,
                                         const char *      user,
                                         const char *      password,
                                         const char *      host,
                                         int               port,
                                         const char *      socket,
                                         unsigned long     flags)

:   conn_(conn),
    db_(NULL),
    databaseName_(databaseName),
    statementPath_(statementPath),
    user_(user),
    password_(password),
    host_(host),
    port_(port),
    socket_(socket),
    flags_(flags),
    isAutoCommit_(true),
    statementsLoaded_(false),
    isOpen_(false)
{}

MySqlConnectionImpl::~MySqlConnectionImpl()
{
    close();
}

const Document &
MySqlConnectionImpl::getStatements()
{
    if (!statementsLoaded_) 
        loadStatements();
    return statementDict_;
}

MYSQL *
MySqlConnectionImpl::getdb()
{
    if (db_ == NULL) open();
    return db_;
}

int
MySqlConnectionImpl::loadStatements()
{
    if (statementsLoaded_) return 0;
    statementsLoaded_ = true;
    stringstream errorMsg;

    FILE* fp = fopen(statementPath_, "r");
    if (!fp)
    {
        errorMsg << "Unable to open " << statementPath_;
        perror(errorMsg.str().c_str());
        return 1;
    }
    CONN_LOG(conn_, info) << "Loading SQL dictionary from " << statementPath_;
    char readBuffer[65536];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));

    ParseResult ok = statementDict_.ParseStream(is);
    fclose(fp);
    if (!ok)
    {
        CONN_LOG(conn_, error) << "Error parsing " << statementPath_ << ": " 
                               << GetParseError_En(ok.Code()) 
                               << " (" << ok.Offset() << ")";
        return 1;
    }
    return 0;
}

int
MySqlConnectionImpl::open()
{
    if (db_ != NULL) return 0;

    stringstream errorMessage;

    CONN_LOG(conn_, info) << "Creating " << (conn_->isAsync() ? "async " : "")
                          << "MySql connection to " << databaseName_
                          << ": SQL dictionary " << statementPath_
                          << ", user " << user_
                          << ", host " << host_;
    MYSQL * initialConn = mysql_init(NULL);
    if (initialConn == NULL)
    {
        errorMessage << "Failed to create connection to MySql server";
	return conn_->reportError(errorMessage);
    }
    db_ = mysql_real_connect(initialConn,
	                     host_,   
			     user_,     
			     password_, 
			     databaseName_,
			     port_,    
			     socket_,  
			     flags_);  
    if (db_ == NULL)
    {
	errorMessage << "Failed to connect: " << mysql_error(initialConn);
	return conn_->reportError(errorMessage);
    }
    setAutoCommit(true);
    isOpen_ = true;
    return 0;
}

void
MySqlConnectionImpl::startMySqlThread()
{
    mysql_thread_init();
}

void
MySqlConnectionImpl::endMySqlThread()
{
    mysql_thread_end();
}

// Look for a previous execution that matches the caller's, and that is 'live',
// meaning the statement handle and parameter MYSQL_BIND array are retained. Called
// when looking for a statement handle to re-use.
MySqlExecution *
MySqlConnectionImpl::findLivePriorExecution(MySqlExecution * execution) const
{
    for (MySqlConnection::ExecutionList::reverse_iterator itr = conn_->executions_.rbegin();
         itr != conn_->executions_.rend();
         ++itr)
    {
        shared_ptr<MySqlExecution> & previousExecution = *itr;
        if (   previousExecution->statementHandle_ != NULL
	    && execution->isSameStatementAs(previousExecution.get()))
        {
            return previousExecution.get();
        }
    }
    return NULL;
}

// Turn off auto-commit to start a transaction, turn it back on after commit or rollback.
// Framework defaults to auto-commit on.
int
MySqlConnectionImpl::setAutoCommit(bool isAutoCommit)
{
    MYSQL * db = getdb();  // can create initial connection
    if (db == NULL) return 1;
    mysql_autocommit(db, isAutoCommit);
    isAutoCommit_ = isAutoCommit;
    return 0;
}

// Make sure auto-commit is off, commit, and turn on auto-commit
int
MySqlConnectionImpl::commit()
{
    CONN_LOG(conn_, trace) << "committing transaction ";

    stringstream errorMessage;
    if (db_ == NULL)
    {
        errorMessage << "commit called with no MySQL connection";
        return conn_->reportError(errorMessage);
    }    
    if (isAutoCommit())
    {
        errorMessage << "commit called with no transaction in progress";
        return conn_->reportError(errorMessage);
    }    
    int rc = mysql_commit(db_);
    if (rc != 0)
    {
        errorMessage << "committing transaction";
        reportMySqlError(errorMessage);
    }  
    setAutoCommit(true);
    return rc;  
}

// Make sure auto-commit is off, do the rollback,
// and turn on auto-commit
int
MySqlConnectionImpl::rollback()
{
    if (db_ == NULL) return 0;
    if (isAutoCommit()) return 0;

    stringstream errorMessage;

    int rc = mysql_rollback(db_);
    if (rc != 0)
    {
        errorMessage << "rolling back transaction";
        reportMySqlError(errorMessage);
    } 
    setAutoCommit(true); 
    return rc;  
}

// Roll back any open transaction and close the MySQL database
void
MySqlConnectionImpl::close()
{
    isOpen_ = false;
    rollback();
    if (db_)
    {
        mysql_close(db_);
	CONN_LOG(conn_, info) << "Closed MySQL connection to " << databaseName_;
	db_ = NULL;
    }
}

// Caller passes in a context (e.g. "committing transaction"). We extract
// the MySQL error number and error message, append them to the context,
// and fail. 
int
MySqlConnectionImpl::reportMySqlError(const stringstream & context)
{
    stringstream errorMessage;
    errorMessage << "MySql error " << context.str()
                 << ": " << mysql_error(db_)
                 << " (" << mysql_errno(db_) << ")";
    return conn_->reportError(errorMessage, mysql_errno(db_));
}

// Serialize a value from a rapidjson DOM into a stream
void
MySqlConnectionImpl::printValue(const Value & val, ostream & outputStream) 
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<StringBuffer> writer(buffer);
    val.Accept(writer);
    outputStream << buffer.GetString();
}


//                        M Y S Q L  L I B R A R Y

// Static singleton struct that performs MySQL initialization at startup 
// and cleans up at shutdown
MySqlConnectionImpl::MySqlLibrary::MySqlLibrary()
{
    if (mysql_library_init(0, NULL, NULL))
    {
	throw std::runtime_error("Failed to initialize MySQL library");
    }
}

MySqlConnectionImpl::MySqlLibrary::~MySqlLibrary()
{
    mysql_library_end();
}



