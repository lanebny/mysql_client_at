#include <cstdio>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <limits>

#include <boost/move/unique_ptr.hpp>

#include <rapidjson/filereadstream.h>
#include <rapidjson/filewritestream.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/reader.h>
#include <rapidjson/writer.h>

#include "connection_impl.h"
#include "execution.h"
#include "observer.h"

using namespace std;
using namespace boost;

// 


//                                   M Y S Q L  O B S E R V E R

unique_ptr<MySqlObserver>
MySqlObserver::createObserver(const char *                name,
			      ObserverType                observerType,
			      const rapidjson::Document * params,
			      MySqlConnection *           conn)
{
    stringstream errorMessage;

    switch (observerType)
    {
        case AUDIT_OBS:
        {
	    unique_ptr<MySqlObserver> auditObserver(new AuditObserver(name, params, conn));
            return boost::move(auditObserver);
        }
        case CAPTURE_OBS:
        {
	    unique_ptr<MySqlObserver> captureObserver(new CaptureObserver(name, params, conn));
            return boost::move(captureObserver);
        }
        case REPLAY_OBS:
        {
	    unique_ptr<MySqlObserver> replayObserver(new ReplayObserver(name, params, conn));
            return boost::move(replayObserver);
        }
        case DEBUG_OBS:
        {
	    unique_ptr<MySqlObserver> debugObserver(new DebugObserver(name, params, conn));
            return boost::move(debugObserver);
        }
        default:
            errorMessage << "Invalid observer type " << observerType;
            throw std::invalid_argument(errorMessage.str());
    }
}

MySqlObserver::MySqlObserver(const char * name, const rapidjson::Document * params, MySqlConnection * conn)
:   name_(name),
    conn_(conn)
{
    CONN_LOG(conn_, trace) << "Creating observer " << name_
                           << " on connection " << conn_->getConnectionName();  
    getWorkingDirectory(params);
}

MySqlObserver::~MySqlObserver()
{
    CONN_LOG(conn_, trace) << "Destroying observer " << name_
                           << " on connection " << conn_->getConnectionName();  
}

void              
MySqlObserver::startProgram(const char * programName)
{
    CONN_LOG(conn_, trace) << "Starting program " << programName << " for observer " << name_;
    currentProgram_ = programName;
}

void              
MySqlObserver::endProgram(const char * programName)
{
    CONN_LOG(conn_, trace) << "Ending program " << programName << " for observer " << name_;
    assert(currentProgram_ == programName);
    currentProgram_.clear();
}

void                   
MySqlObserver::getWorkingDirectory(const rapidjson::Document * params)
{
    if (params != NULL && params->IsObject())
    {
        Value::ConstMemberIterator itr = params->FindMember("working_directory");
        if (itr != params->MemberEnd())
        {
            workingDirectory_ = itr->value.GetString();
        }
    }
    if (workingDirectory_.empty())
    {
        char cwd[1024];
        getcwd(cwd, 1024);
        workingDirectory_ = string(cwd, strlen(cwd));
    }
}

string
MySqlObserver::getProgramPath()
{
    stringstream programPath;
    programPath << workingDirectory_ << "/" << name_ << "." << currentProgram_ << ".json";
    return programPath.str();
}



//                                   A U D I T  O B S E R V E R

AuditObserver::AuditObserver(const char *                name,
		             const rapidjson::Document * params,
		             MySqlConnection *           conn)
:  MySqlObserver(name, params, conn),
   insertStatement_("insert_audit_record"),
   isAuditing_(false)
{
    if (conn->isReplay()) return;  // don't audit unit tests

    stringstream errorMessage;

    if (   params == NULL 
        || !params->IsObject()
        || !params->HasMember("database")
        || !params->HasMember("table_name")
        || !params->HasMember("sql"))
    {
        CONN_LOG(conn, error) << "audit observer requires database name, table name, and SQL dictionary path for audit db";
        return;
    }

    Value::ConstMemberIterator itr = params->FindMember("database");
    auditDatabaseName_ = itr->value.GetString();
    itr = params->FindMember("table_name");
    auditTableName_ = itr->value.GetString();
    itr = params->FindMember("sql");
    auditSqlPath_ = itr->value.GetString();
    if (params->HasMember("insert_statement"))
    {
        itr = params->FindMember("insert_statement");
        insertStatement_ = itr->value.GetString();
    }

    // Connect to the database containing the audit table,
    // using the credentials from the main connection
    stringstream connectionName;
    connectionName << "audit_" << conn_->getConnectionName();
    auditConn_ = MySqlConnection::createConnection(connectionName.str().c_str(),
                                                   auditDatabaseName_.c_str(),
 	                                           auditSqlPath_.c_str(),
					           conn_->getUser(),
                                                   conn_->getPassword(),
					           conn_->getHost(),
                                                   conn_->getPort(),
                                                   conn_->getSocket(),
						   0,
					           true);  // async 
    isAuditing_ = prepareToAudit();
    if (!isAuditing_ && auditConn_->isOpen())
        auditConn_->close();
}

bool
AuditObserver::prepareToAudit()
{
    int rc = auditConn_->open();
    if (rc != 0)
    {
        CONN_LOG(auditConn_, error) << "Error connecting to audit database " << auditDatabaseName_ 
                                    << ": " << auditConn_->getErrorMessage();
        return false; 
    }

    const rapidjson::Value & statements =  auditConn_->getStatements()["statements"];
    if (!statements.HasMember(insertStatement_.c_str()))
    {
        CONN_LOG(auditConn_, error) << "SQL dictionary " << auditSqlPath_ 
                                    << " does not include " << insertStatement_ << " statement";
        return false;
    }

    // Create the audit table if it doesn't exist
    auditConn_->execute("create_audit_table", "", "table_name", auditTableName_.c_str() ); 
    rc = auditConn_->getReturnCode();
    if (rc != 0)
    {
        CONN_LOG(auditConn_, error) << "Error creating audit table: " << auditConn_->getErrorMessage();
        return false; 
    }

    return true;
}

AuditObserver::~AuditObserver()
{
    CONN_LOG(auditConn_, info) << "Destroying audit observer";
}

void
AuditObserver::startProgram(const char * programName)
{
}

// If the execution is transitioning from a non-terminal state to a 
// terminal state (either EXECUTION_COMPLETE or ERROR), then 
// add an audit record.
MySqlExecution::ExecutionState   
AuditObserver::onEvent(MySqlExecution * execution, ExecutionState newState)
{
    if (!isAuditing_) return newState;

    if (   execution == NULL
        || execution->isTerminalState(execution->getState())
        || !execution->isTerminalState(newState))
        return newState;
    
    const rapidjson::Document & dom = execution->asJson();
    insertRecord("EXECUTE", &dom);

    return newState;
}


// If the event is COMMIT or ROLLBACK, add an audit record
void
AuditObserver::onEvent(AuditEventType event, const char * comment, MySqlExecution * execution)
{
    if (!isAuditing_) return;
    if (event != AUDIT_COMMIT && event != AUDIT_ROLLBACK) return;
    insertRecord(event == AUDIT_COMMIT ? "COMMIT" : "ROLLBACK", NULL, comment);
}

// Construct a json object containing the arguments for the 
// insert-record statement from statement parameters and the json doc
// representing the execution. Each parameter name should correspond 
// to a member of the doc.
void
AuditObserver::insertRecord(const char * event, const rapidjson::Document * executionDom, const char * comment)
{
    rapidjson::Document insertArgs;
    insertArgs.SetObject();
    Value eventValue(event, strlen(event), insertArgs.GetAllocator());
    insertArgs.AddMember("event", eventValue, insertArgs.GetAllocator());
    if (comment != NULL)
    {
        Value commentValue(comment, strlen(comment), insertArgs.GetAllocator());
        insertArgs.AddMember("comment", commentValue, insertArgs.GetAllocator());
    }

    const rapidjson::Value & statements =  auditConn_->getStatements()["statements"];
    const rapidjson::Value & insertStatement = statements[insertStatement_.c_str()];
    const rapidjson::Value & insertParams = insertStatement["parameters"];

    for (rapidjson::Value::ConstValueIterator itrparm = insertParams.Begin();
         itrparm != insertParams.End();
         ++itrparm)
    {
        const rapidjson::Value & paramAttrs = *itrparm;
        const string & paramName = paramAttrs["name"].GetString();

        if (paramName == "table_name") 
        {
            Value tableName(auditTableName_.c_str(), auditTableName_.size(), insertArgs.GetAllocator());
            insertArgs.AddMember("table_name", tableName, insertArgs.GetAllocator());
            continue;
        }
        else if (paramName == "program")
        {
            const string program = conn_->getCurrentProgram();
            if (!program.empty())
            {
                Value programValue(program.c_str(), program.size(), insertArgs.GetAllocator());
                insertArgs.AddMember("program", programValue, insertArgs.GetAllocator());
            }
            continue;
        }
        else if (paramName == "transaction")
        {
            const string transaction = conn_->getCurrentTransaction();
            if (!transaction.empty())
            {
                Value transactionValue(transaction.c_str(), transaction.size(), insertArgs.GetAllocator());
                insertArgs.AddMember("transaction", transactionValue, insertArgs.GetAllocator());
            }
            continue;
        }
        
        if (executionDom == NULL) continue;
        rapidjson::Value::ConstMemberIterator itrvalue = executionDom->FindMember(paramName.c_str());
        if (itrvalue == executionDom->MemberEnd()) continue;  

        Value parameterName;
        parameterName.CopyFrom(itrvalue->name, insertArgs.GetAllocator());
        Value parameterValue;
        parameterValue.CopyFrom(itrvalue->value, insertArgs.GetAllocator());

        // If it's a scalar value, write it out as is
        if (!parameterValue.IsObject() && !parameterValue.IsArray())
            insertArgs.AddMember(parameterName, parameterValue, insertArgs.GetAllocator());
        // If it's an object or array, serialize it to json and write it as a string field
	else
        {
            stringstream valueStream;
            MySqlConnectionImpl::printValue(parameterValue, valueStream);
            Value complexValue(valueStream.str().c_str(), valueStream.str().size(), insertArgs.GetAllocator());
            insertArgs.AddMember(parameterName, complexValue, insertArgs.GetAllocator());
        }
    }

    // insert the record
    auditConn_->executeJson(insertStatement_.c_str(), "", &insertArgs);
}

void
AuditObserver::endProgram(const char * programName)
{
}

//                                  C A P T U R E  O B S E R V E R

CaptureObserver::CaptureObserver(const char *                name,
				 const rapidjson::Document * params,
				 MySqlConnection *           conn)
:  MySqlObserver(name, params, conn)
{
}

CaptureObserver::~CaptureObserver()
{
}

void
CaptureObserver::startProgram(const char * programName)
{
    MySqlObserver::startProgram(programName);

    if (capturedExecutions_.IsObject())
        capturedExecutions_.RemoveAllMembers();
    else
        capturedExecutions_.SetObject();
    Value executions(kArrayType);
    capturedExecutions_.AddMember("executions", executions, capturedExecutions_.GetAllocator());
}  

// If we're in a program (client has called startProgram), and 
// the execution is transitioning from a non-terminal state to a 
// terminal state (either EXECUTION_COMPLETE or ERROR), then 
// serialize to JSON and save it to the executions DOM
MySqlExecution::ExecutionState   
CaptureObserver::onEvent(MySqlExecution * execution, ExecutionState newState)
{
    if (   !currentProgram_.empty()
        && execution != NULL
        && !execution->isTerminalState(execution->getState())
        && execution->isTerminalState(newState))
    {
        // save with the target state
        ExecutionState save = execution->getState();
        execution->setState(newState); 
        const rapidjson::Value & dom =  execution->asJson();
        rapidjson::Value domCopy;
        domCopy.CopyFrom(dom, capturedExecutions_.GetAllocator());
        capturedExecutions_["executions"].PushBack(domCopy, capturedExecutions_.GetAllocator());
        execution->setState(save);
    }
    return newState;
}

void
CaptureObserver::endProgram(const char * programName)
{
    stringstream errorMessage;
    string capturePath = getProgramPath();
    MySqlObserver::endProgram(programName);

    if (!capturedExecutions_.IsObject() || capturedExecutions_["executions"].Empty()) return;
    FILE * fp = fopen(capturePath.c_str(), "w");
    if (!fp)
    {
        errorMessage << "Unable to open " << capturePath;
        perror(errorMessage.str().c_str());
        return;
    }
    char writeBuffer[65536];
    FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
    Writer<FileWriteStream> writer(os);
    capturedExecutions_.Accept(writer);
    fclose(fp); 
    capturedExecutions_.RemoveAllMembers();   
}  


//                                  R E P L A Y  O B S E R V E R

// 
ReplayObserver::ReplayObserver(const char *                name,
			       const rapidjson::Document * params,
			       MySqlConnection *           conn)
:  MySqlObserver(name, params, conn),
   executionNumber_(0)
{
    conn_->setTransactions(false);
}

ReplayObserver::~ReplayObserver() 
{
}

void              
ReplayObserver::startProgram(const char * programName)
{
    stringstream errorMessage;
    MySqlObserver::startProgram(programName);

    if (replayExecutions_.IsObject())
        replayExecutions_.RemoveAllMembers();
    else
        replayExecutions_.SetObject();
    executionNumber_ = 0;

    string replayPath = getProgramPath();

    FILE * fp = fopen(replayPath.c_str(), "r");
    if (!fp)
    {
        errorMessage << "Unable to open " << replayPath << " for reading";
        perror(errorMessage.str().c_str());
        return;
    }
    char readBuffer[65536];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));
    ParseResult ok = replayExecutions_.ParseStream(is);
    fclose(fp); 
}

// If the SQL text has been generated, find the matching json doc 
// in the list of prior executions, confirm that SQL matches, then
// copy the state from the json doc into the current execution. 
MySqlExecution::ExecutionState    
ReplayObserver::onEvent(MySqlExecution * execution, ExecutionState newState)
{
    stringstream errorMessage;

    if (execution->getState() == MySqlExecution::INITIAL_STATE) 
        executionNumber_++;
    if (newState != MySqlExecution::SQL_GENERATED_STATE) return newState;
    if (!replayExecutions_.IsObject() || replayExecutions_.ObjectEmpty()) return newState;

    // The SQL text and parameter bindings have been generated for the current execution.
    // Match against the corresponding execution in the replay doc
    const rapidjson::Value & replayExecutions = replayExecutions_["executions"];
    if (replayExecutions.Size() < executionNumber_)
    {
        errorMessage << "Test executes more statements than expected. Expected " << replayExecutions.Size();
        conn_->reportError(errorMessage);
        return MySqlExecution::ERROR_STATE;
    }
    const rapidjson::Value & replayExecution = replayExecutions[executionNumber_-1];
    if (!execution->isSameAs(replayExecution, errorMessage))
    {
        conn_->reportError(errorMessage);
        return MySqlExecution::ERROR_STATE;
    }
    
    // Executions match: we will switch to the state in the replay execution
    // which could either be ERROR or EXECUTION_COMPLETE. If the original
    // execution succeeded, copy row-count, rows-affected and results to
    // the current execution. If the original execution failed, copy the
    // error message and error number to the current execution
    execution->rc_ = replayExecution["rc"].GetInt();
    if (replayExecution.HasMember("rows_returned"))
        execution->rowCount_ = replayExecution["rows_returned"].GetInt();
    if (replayExecution.HasMember("rows_affected"))
        execution->rowsAffected_ = replayExecution["rows_affected"].GetInt();
    if (replayExecution.HasMember("results"))
    {
        const Value & replayResults = replayExecution["results"];
        execution->getResults().CopyFrom(replayResults, execution->getResults().GetAllocator());
    }
    if (replayExecution.HasMember("error_no"))
    {
        int replayErrorNo = replayExecution["error_no"].GetInt();
	if (replayErrorNo)
	{
            const string & replayErrorMessage = replayExecution["error_message"].GetString();
            execution->errorMessage_ = replayErrorMessage;
            execution->errorNo_ = replayErrorNo;
            conn_->reportError(replayErrorMessage, replayErrorNo, execution->getHandle());
        }
    }
    int exitState = replayExecution["state"].GetInt();
    return static_cast<MySqlExecution::ExecutionState>(exitState);
}

void              
ReplayObserver::endProgram(const char * programName)
{
}


//                                  D E B U G  O B S E R V E R

// The debug observer sets the severity threshold to trace for both
// the console log and the file log (if there is one). It also logs
// information about state changes.
DebugObserver::DebugObserver(const char *                name,
		             const rapidjson::Document * params,
		             MySqlConnection *           conn)
:  MySqlObserver(name, params, conn)
{
    priorConsoleLoglevel_ = conn_->getConsoleLoglevel();
    conn_->setConsoleLoglevel(loglevel::trace);
    priorFileLoglevel_ = conn_->getFileLoglevel();
    conn_->setFileLoglevel(loglevel::trace);
}

DebugObserver::~DebugObserver() 
{
}

void              
DebugObserver::startProgram(const char * programName)
{
}

MySqlExecution::ExecutionState   
DebugObserver::onEvent(MySqlExecution * execution, ExecutionState newState)
{
    EX_LOG(conn_, execution, trace) << execution->getState() << " -> " << newState;

    if (newState == MySqlExecution::SQL_GENERATED_STATE)
    {
        EX_LOG(conn_, execution, trace) << "  " << execution->statementText_;
    }

    if (newState == MySqlExecution::BINDINGS_PREPARED_STATE)
    {
        stringstream settingsMsg;
        settingsMsg << "  Ready to execute MySql bind: paramCount " << execution->paramCount_ << "\n   ";
        for (Value::ConstMemberIterator itrsetting = execution->settings_.MemberBegin();
             itrsetting != execution->settings_.MemberEnd();
	     ++itrsetting)
        {
	    settingsMsg << itrsetting->name.GetString() << ":";
            const Value & setting = itrsetting->value;
            if (setting.HasMember("param_value"))
	        MySqlConnectionImpl::printValue(setting["param_value"], settingsMsg);
	    settingsMsg << "  ";			  
        }
        EX_LOG(conn_, execution, trace) << settingsMsg.str();
    }

    if (execution->getState() == MySqlExecution::BINDINGS_PREPARED_STATE)
    {
        EX_LOG(conn_, execution, trace) << "  Execution complete. " << execution->rowsAffected_  << " rows affected";
        if (execution->columnCount_)
        {
            stringstream colsMsg;
            colsMsg << "  Returns columns ";
            for (int icol = 0; icol < execution->columnCount_; icol++)
            {
                MYSQL_FIELD * fieldDescriptor = mysql_fetch_field_direct(execution->resultsMetadata_, icol);
                colsMsg << fieldDescriptor->name << " ";
            }
            EX_LOG(conn_, execution, trace) << colsMsg.str();
        }
    }

    if (newState == MySqlExecution::STATEMENT_COMPLETE_STATE)
    {
        if (execution->results_.IsObject())
        {
            stringstream resultsMsg;
            resultsMsg << "  ";
            MySqlConnectionImpl::printValue(execution->results_, resultsMsg);
            EX_LOG(conn_, execution, trace) << resultsMsg.str();
        }
    }
	
    return newState;
}

void              
DebugObserver::endProgram(const char * programName)
{
}
