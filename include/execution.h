#ifndef __execution_h__
#define __execution_h__

#include <cstdarg>
#include <cassert>
#include <iostream>
#include <sstream>

#include <boost/function.hpp>
#include <boost/unordered_map.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <mysql.h>

#include <rapidjson/document.h>

#include "connection.h"

using namespace boost;
using namespace rapidjson;

class MySqlConnectionImpl;

 
//                                M Y S Q L  E X E C U T I O N

class MySqlExecution 
{
    friend class MySqlConnectionImpl;
    friend class ExecutionThread;
    friend class ReplayObserver;
    friend class DebugObserver;
  
public:

    enum ExecutionState
    {
        NO_STATE,
        INITIAL_STATE,
        STATEMENT_VALID_STATE,
        SETTINGS_CREATED_STATE,
        SQL_GENERATED_STATE,
	MYSQL_STMT_CREATED_STATE,
        BINDINGS_PREPARED_STATE,
        STATEMENT_PREPARED_STATE,
        EXECUTION_COMPLETE_STATE,
        RESULTS_RETRIEVED_STATE,
        STATEMENT_COMPLETE_STATE,
        ERROR_STATE  
    };

    enum ParamType
    {
        MARKER,
        SUBSTITUTE
    };

    typedef boost::function<int(MySqlExecution*)> StateFunction;
    typedef boost::unordered_map<ExecutionState, StateFunction> StateFunctionMap;
    typedef ExecutionThread::RequestSequence RequestSequence;

public:
    MySqlExecution(const char *          statementName, 
		   va_list &             args,
                   MySqlConnection *     conn,
                   MySqlConnectionImpl * impl);
    virtual ~MySqlExecution();

public:
    void              setParameterValues(const Document * args)     { argDoc_ = args; }
    int               getHandle() const                             { return executionHandle_; }
    void              setRequestSequence(RequestSequence seq)       { requestSequence_ = seq; }
    RequestSequence   getRequestSequence() const                    { return requestSequence_; }
    int               prepareToExecute();
    int               execute();
    int               crankStateMachine(MySqlExecution::ExecutionState exitState=NO_STATE);
    int               getReturnCode()                               { return rc_; }
    void              setState(ExecutionState newState)             { state_ = newState; }
    bool              isTerminalState(ExecutionState state);
    const string &    getStatementName() const                      { return statementName_; }
    const string &    getStatementText() const                      { return statementText_; } 
    const Document &  getSettings() const                           { return settings_; } 
    int               getRowCount() const                           { return rowCount_; }
    int               getRowsAffected() const                       { return rowsAffected_; }
    const Document &  getResults() const                            { return results_; }
    Document &        getResults()                                  { return results_; }          
    int               reportMySqlError(MYSQL_STMT * statementHandle, const stringstream & context);
    int               reportError(const stringstream & errorMessage, int errorNo=1);
    int               reportError(const string & errorMessage, int errorNo=1);
    const Document &  asJson();
    void              toJson();
    bool              isSameStatementAs(const MySqlExecution * otherExec) const;
    bool              isSameAs(const Value & execDom, stringstream & errorMessage) const;
    void              moveFrom(MySqlExecution * previousExecution);
    int               close(bool isReusable);
    void              cleanup();

// state functions: execution steps
private:
    int               validateStatement();       // INITIAL_STATE
    int               createSettings();          // STATEMENT_VALID_STATE
    int               generateStatementText();   // SETTINGS_CREATED_STATE
    int               createPreparedStatement(); // SQL_GENERATED_STATE
    int               prepareToBind();           // MYSQL_STMT_CREATED
    int               bindParameters();          // BINDINGS_PREPARED_STATE
    int               executeStatement();        // STATEMENT_PREPARED_STATE
    int               retrieveResults();         // EXECUTION_COMPLETE_STATE

    int               bindParameter(const Value & binding, MYSQL_BIND * parameterBind, char *& buffer);
    int               bindColumn(MYSQL_FIELD * fieldDescriptor, MYSQL_BIND * columnBind, char *& buffer);
    int               storeResultRow();
    char *            getBlobBuffer(int size);
    int               stringToMySqlTime(const char * timeString, enum enum_field_types typeCode, MYSQL_TIME * mysqlTime);

public:
    int               changeState(ExecutionState newState);
    ExecutionState    getState() const {  return state_; }

    static StateFunctionMap stateFunctionMap_;
    static StateFunctionMap createStateFunctionMap();

private:
    int                   executionHandle_;
    RequestSequence       requestSequence_;  // assigned by execution thread if connection is async
    string                statementName_;
    va_list &             args_;
    const Document *      argDoc_;
    MYSQL_STMT *          statementHandle_;
    string                statementText_;
    rapidjson::Document   dom_;
    bool                  isAutoCommit_;  // false if part of a transaction
    ExecutionState        state_;
    int                   rc_;
    int                   errorNo_;
    string                errorMessage_;
    
    rapidjson::Document   settings_;
    MYSQL_BIND *          parameterBindArray_;
    unsigned long         paramCount_;
    char *                paramBuffer_;
    int                   paramBufferLen_;
    
    MYSQL_RES *           resultsMetadata_;
    MYSQL_BIND *          columnBindArray_;
    int                   columnCount_;
    char *                rowBuffer_;
    int                   rowBufferLen_;
    Document              results_;
    int                   rowCount_;
    int                   rowsAffected_;
    char *                blobBuffer_;
    int                   blobBufferLen_;
    
    MySqlConnection *     conn_;
    MySqlConnectionImpl * connImpl_;

    posix_time::ptime     startTime_;
    posix_time::ptime     executeTime_;
    posix_time::ptime     retrieveTime_;
    posix_time::ptime     completeTime_;

    static int            nextExecutionHandle_;
    static my_bool        mysqlTrue_;
    static my_bool        mysqlFalse_;
}; 

ostream & operator<<(ostream & o, const MySqlExecution & execution);

#endif // __execution_h__
