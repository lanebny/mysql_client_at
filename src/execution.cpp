#include <boost/bind.hpp>
#include <boost/regex.hpp>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "connection_impl.h"
#include "execution.h"


 
//                          M Y S Q L  E X E C U T I O N

// A MySqlExecution object encapsulates the execution of a single
// SQL statement. It is implememented as a simple state machine. An
// execution starts in INITIAL and transitions from state to
// state by calling functions in a map (stateFunctionMap) indexed by
// the state code, until it reaches a terminal state, either
// STATEMENT_COMPLETE or ERROR.
//
// INITIAL           (validateStatement)       ->  STATEMENT_VALID
// STATEMENT_VALID   (createSettings)          ->  SETTINGS_CREATED
// SETTINGS_CREATED  (generateStatementText)   ->  SQL_GENERATED
// SQL_GENERATED     (createPreparedStatement) ->  MYSQL_STMT_CREATED  (connects to MySQL service)
// MSQL_STMT_CREATED (prepareToBind)           ->  BINDINGS_PREPARED





MySqlExecution::StateFunctionMap MySqlExecution::stateFunctionMap_ = MySqlExecution::createStateFunctionMap();
int MySqlExecution::nextExecutionHandle_= 1;
my_bool MySqlExecution::mysqlTrue_ = true;
my_bool MySqlExecution::mysqlFalse_ = false;

MySqlExecution::MySqlExecution(const char *          statementName,
			       const char *          comment,
			       va_list &             args,
                               MySqlConnection *     conn,
                               MySqlConnectionImpl * connImpl)
:   executionHandle_(nextExecutionHandle_++),
    requestSequence_(0),
    statementName_(statementName),
    comment_(comment),
    args_(args),
    argDoc_(NULL),
    statementHandle_(NULL),
    isAutoCommit_(conn->impl_->isAutoCommit()),
    rc_(-1),
    errorNo_(0),
    parameterBindArray_(NULL),
    paramCount_(0),
    paramBuffer_(NULL),
    paramBufferLen_(0),
    resultsMetadata_(NULL),
    columnBindArray_(NULL),
    columnCount_(0),
    rowBuffer_(NULL),
    rowBufferLen_(0),
    rowCount_(0),
    blobBuffer_(NULL),
    blobBufferLen_(0),
    conn_(conn),
    connImpl_(connImpl)
{
    EX_LOG(conn_, this, trace) << "Creating execution " << executionHandle_;
}

MySqlExecution::~MySqlExecution()
{
    close(false);  
    cleanup();
}

MySqlExecution::StateFunctionMap
MySqlExecution::createStateFunctionMap()
{
    StateFunctionMap stateFunctionMap;
    stateFunctionMap[INITIAL_STATE]            = MySqlExecution::validateStatement;
    stateFunctionMap[STATEMENT_VALID_STATE]    = MySqlExecution::createSettings;
    stateFunctionMap[SETTINGS_CREATED_STATE]   = MySqlExecution::generateStatementText;
    stateFunctionMap[SQL_GENERATED_STATE]      = MySqlExecution::createPreparedStatement;
    stateFunctionMap[MYSQL_STMT_CREATED_STATE] = MySqlExecution::prepareToBind;
    stateFunctionMap[BINDINGS_PREPARED_STATE]  = MySqlExecution::bindParameters;
    stateFunctionMap[STATEMENT_PREPARED_STATE] = MySqlExecution::executeStatement;
    stateFunctionMap[EXECUTION_COMPLETE_STATE] = MySqlExecution::retrieveResults;
    return stateFunctionMap;
}

// Run the state machine up to the point where we need to talk to the server
int
MySqlExecution::prepareToExecute()
{
    int rc = 0;
    setState(INITIAL_STATE);
    rowCount_ = 0;
    rowsAffected_ = 0;
    conn_->errorMessage_.clear();
    conn_->errorNo_ = 0;
    startTime_ = posix_time::microsec_clock::local_time();
    rc = crankStateMachine(SQL_GENERATED_STATE);
    return rc;
}

// Send the prepared statement to the server and process the response
int
MySqlExecution::execute()
{
    int rc = crankStateMachine();
    close(true); // allow re-use
    return rc;
}

// Loop: look up the transition function corresponding to 
// the current state and call it. Exit if there is no transition
// function or the state matches the exit state passed by 
// the caller.
int
MySqlExecution::crankStateMachine(MySqlExecution::ExecutionState exitState)
{
    int rc = 0;
    while (true)
    {
        if (state_ == exitState) break;
        StateFunctionMap::iterator itr = stateFunctionMap_.find(state_);
        if (itr == stateFunctionMap_.end()) break;
        const StateFunction & stateFunction = (*itr).second;
        rc = stateFunction(this);
        if (rc != 0) break;
    }
    rc_ = rc;
    return rc;
}

bool
MySqlExecution::isTerminalState(ExecutionState state)
{
    StateFunctionMap::iterator itr = stateFunctionMap_.find(state);
    if (itr != stateFunctionMap_.end()) return false;
    return true;
}

// Read the SQL statement text from the JSON SQL dictionary specfied
// when the connection was created
int 
MySqlExecution::validateStatement()
{
    EX_LOG(conn_, this, trace) << "Executing " << statementName_;

    stringstream errorMessage;

    const Document & statementDict = conn_->getStatements();
    if (!statementDict.IsObject())
    {
       errorMessage << "Internal error: statement dictionary corrupt";
       return reportError(errorMessage);
    }
    if (!statementDict["statements"].HasMember(statementName_.c_str()))
    {
       errorMessage << "Unknown statement \'" << statementName_ << "\'"; 
       return reportError(errorMessage);
    }
    return changeState(STATEMENT_VALID_STATE);
}

// Merge the parameter declarations in the statement JSON object with the
// argument values passed to the execution constructor. The output is an
// array containing an object for each parameter. An object in this array
// contains the parameter name, the type (MARKER or SUBSTITUTE), the MySQL
// data type, and the value. Parameter values can be passed either as a
// va_list or a JSON name/value object.
int 
MySqlExecution::createSettings()
{
    stringstream errorMessage;
    const rapidjson::Value & statement =  conn_->getStatements()["statements"][statementName_.c_str()];
  
    if (!statement.HasMember("parameters")) 
    {
        if (args_ != NULL)
        {
            errorMessage << "Arguments passed for statement \'" << statementName_ 
                         << "\' which takes no arguments";
            return reportError(errorMessage);
        }
        return 0;
    }

    // initialize the parameter settings document
    if (args_ == NULL)
    {
        errorMessage << "No arguments passed for statement \'" << statementName_ << "\'";
        return reportError(errorMessage);
    }
    settings_.SetObject();
    
    const rapidjson::Value & parameters = statement["parameters"];
    if (!parameters.IsArray())
    {
        errorMessage << "Parameter definitions for statement \'" << statementName_ << "\' are corrupt";
        return reportError(errorMessage);
    }
    for (Value::ConstValueIterator itr = parameters.Begin();
         itr != parameters.End(); 
         ++itr)
    {
        if (!itr->IsObject())
        {
            errorMessage << "Parameter list for statement \'"
			 << statementName_ << "\' is corrupt";
            return reportError(errorMessage);
        }
        const Value & parameterAttrs = *itr;
        const string & parameterName = parameterAttrs["name"].GetString();

        Value parameterSetting(kObjectType);

        // set the parameter type code (MARKER or SUBSTITUTE)
        if (!parameterAttrs.HasMember("param_type"))
        {
            errorMessage << "param_type missing in definition of parameter " << parameterName
                        << " for statement \'" << statementName_ << "\'";
            return reportError(errorMessage);
        }
        const string & parameterType = parameterAttrs["param_type"].GetString();
        ParamType parameterTypeCode;
        if (parameterType == "marker") 
            parameterTypeCode = MARKER;
        else if (parameterType == "substitute")
            parameterTypeCode = SUBSTITUTE;
        else
        {
            errorMessage << "Unknown parameter type \'" << parameterType << "\'"
                         << " in parameter " << parameterName
                         << " for statement " << statementName_;
            return reportError(errorMessage);
        }
        parameterSetting.AddMember("param_type", parameterTypeCode, settings_.GetAllocator());

        // set the parameter datatype 
        if (!parameterAttrs.HasMember("data_type"))
        {
            errorMessage << "data_type missing in definition of parameter " << parameterName
                        << " for statement " << statementName_;
            return reportError(errorMessage);
        }
        const string & dataType = parameterAttrs["data_type"].GetString();
        enum_field_types dataTypeCode;
        if (dataType == "int") 
            dataTypeCode = MYSQL_TYPE_LONG;
        else if (dataType == "double")
            dataTypeCode = MYSQL_TYPE_DOUBLE;
        else if (dataType == "string")
            dataTypeCode = MYSQL_TYPE_STRING;
        else if (dataType == "date")
            dataTypeCode = MYSQL_TYPE_DATE;
        else if (dataType == "time")
            dataTypeCode = MYSQL_TYPE_TIME;
        else if (dataType == "datetime")
            dataTypeCode = MYSQL_TYPE_DATETIME;
        else if (dataType == "timestamp")
            dataTypeCode = MYSQL_TYPE_TIMESTAMP;
        else
        {
            errorMessage << "Unsupported parameter datatype \'" << dataType << "\'"
                         << " in parameter " << parameterName
                         << " for statement " << statementName_;
            return reportError(errorMessage);
        }
        parameterSetting.AddMember("param_data_type", dataTypeCode, settings_.GetAllocator());

        // add the setting to the settings doc
        Value parameterNameValue(kStringType);
        parameterNameValue.SetString(parameterName.c_str(), parameterName.size(), results_.GetAllocator());
        settings_.AddMember(parameterNameValue, parameterSetting, settings_.GetAllocator());

    }  // end loop through parameters

    // Add the values passed by caller to the settings doc. 
    // The arguments can either be in a C va_list or in a json doc.
    // Arguments may be omitted. If the va_list is incomplete,
    // the last argument should be "end"
    Value::ConstMemberIterator itrarg;
    if (argDoc_ != NULL) itrarg = argDoc_->MemberBegin();
    int settingCount = settings_.MemberCount();
    for (int i = 0; i < settingCount; i++)
    {
        const char * tag;
        if (argDoc_ == NULL)
        {
            tag = va_arg(args_, char *);
            if (strcmp(tag, "end") == 0) break;
        }
        else
        {
            if (itrarg == argDoc_->MemberEnd()) break;
            tag = itrarg->name.GetString();
        }
        Value::MemberIterator itertag = settings_.FindMember(tag);
        if (itertag == settings_.MemberEnd())
        {
            errorMessage << "Unknown parameter \'" << tag << "\'"
                         << " for statement " << statementName_;
            return reportError(errorMessage);
        }
        Value & setting = itertag->value;
        enum enum_field_types dataTypeCode = static_cast<enum enum_field_types>(setting["param_data_type"].GetInt());
        switch (dataTypeCode)
        {
            case MYSQL_TYPE_LONG:
            {
                int intValue = argDoc_ ? itrarg++->value.GetInt() : va_arg(args_, int);
                setting.AddMember("param_value", intValue, settings_.GetAllocator());
                break;
            }
            case MYSQL_TYPE_DOUBLE:
            {
                double doubleValue = argDoc_ ? itrarg++->value.GetDouble() : va_arg(args_, double);
                setting.AddMember("param_value", doubleValue, settings_.GetAllocator());
                break;
            }
            case MYSQL_TYPE_STRING:
            {
                char * stringArg = argDoc_ ? const_cast<char *>(itrarg++->value.GetString()) : va_arg(args_, char *);
	        Value stringValue(kStringType);
	        stringValue.SetString(stringArg, strlen(stringArg), settings_.GetAllocator());
                setting.AddMember("param_value", stringValue, settings_.GetAllocator());
                break;
            }
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_TIMESTAMP:
            {
                char * timeArg = argDoc_ ? const_cast<char *>(itrarg++->value.GetString()) : va_arg(args_, char *);
                int rc = stringToMySqlTime(timeArg, dataTypeCode, NULL);
                if (rc > 0) return rc;
	        Value timeValue(kStringType);
	        timeValue.SetString(timeArg, strlen(timeArg), settings_.GetAllocator());
                setting.AddMember("param_value", timeValue, settings_.GetAllocator());
                break;
            }
        }
    }
	
    va_end(args_);
    args_ = NULL;

    return changeState(SETTINGS_CREATED_STATE);
}

// Replace any substitution parameters in the SQL text
// with the parameter values in the settings list
int
MySqlExecution::generateStatementText()
{
    stringstream errorMessage;
    const rapidjson::Value & statement =  conn_->getStatements()["statements"][statementName_.c_str()];

    if (!statement.HasMember("statement_text"))
    {
        errorMessage << "No statement text supplied for statement " << statementName_;
        return reportError(errorMessage);
    }
    const Value & statementTextLines = statement["statement_text"];
    stringstream statementStream;
    for (Value::ConstValueIterator itrtext = statementTextLines.Begin();
    	 itrtext != statementTextLines.End();
	 ++itrtext)
    {
    	statementStream << itrtext->GetString();
    }
    string statementText(statementStream.str());

    // perform substitutions
    for (Value::ConstMemberIterator itrsetting = settings_.MemberBegin();
    	 itrsetting != settings_.MemberEnd();
	 ++itrsetting)
    {
        const Value & setting = itrsetting->value;
        if (setting["param_type"] == SUBSTITUTE)
        {
            stringstream patternStream;
            patternStream << "@" << itrsetting->name.GetString();
            regex pattern(patternStream.str());
            statementText = regex_replace(statementText, pattern, setting["param_value"].GetString()); 
        }
    }

    statementText_ = statementText;
    EX_LOG(conn_, this, info) << "Preparing to execute " << *this;
    return changeState(SQL_GENERATED_STATE);
}

// Send the statement text to MySql and get back a statement handle.
// If we already have a handle from a prior execution, use that if 
// the statement text hasn't been changed by parameter substitution.

// Make sure that MySql sees the same parameters as are listed in
// the SQL dictionary entry. (That is, the same MARKER-type parameters,
// designated by '?'. SUBSTITUTE parameters have already been replaced
// by their values by the framework.)

// This is the first state where we actually connect to MySql. Lazy
// connect makes it easier to unit-test applications that use this
// framework -- the replay observer sees the transition to this state
// and skips right to completion.
int
MySqlExecution::createPreparedStatement()
{
    stringstream errorMessage;
    int rc;

    MYSQL * db = NULL;

    // can throw if this the first attempt to connect to the database
    try
    {
        db = connImpl_->getdb();
    }
    catch (std::exception e)
    {
        errorMessage << "Error connecting to MySql: " << e.what();
        return reportError(errorMessage);
    }

    // If there is a live prior execution of the same statement (texts identical)
    // then reuse the statement handle and buffers allocated for that execution
    MySqlExecution * priorExecution = conn_->impl_->findLivePriorExecution(this);
    if (priorExecution)
    {
        moveFrom(priorExecution);
        EX_LOG(conn_, this, trace) << "reusing " << priorExecution->getHandle();
        return changeState(MYSQL_STMT_CREATED_STATE);
    }

    // send the statement to MySql 
    statementHandle_ = mysql_stmt_init(db);
    rc = mysql_stmt_prepare(statementHandle_, statementText_.c_str(), statementText_.size());
    if (rc != 0) 
    {
        errorMessage << "preparing statement " << statementName_;
        return conn_->impl_->reportMySqlError(errorMessage);
    }

    paramCount_ = mysql_stmt_param_count(statementHandle_);

    // if mysql found no params, confirm that the user didn't pass any
    if (paramCount_ == 0 && settings_.MemberCount() > 0)
    {
        for (Value::ConstMemberIterator itrbind = settings_.MemberBegin();
    	     itrbind != settings_.MemberEnd();
	     ++itrbind)
        {
            const Value & binding = itrbind->value;
            if (binding["param_type"] == MARKER)
            {
                errorMessage << "MySql found no parameters in statement " << statementName_
                             << " but " << itrbind->name.GetString() << " is declared as marker\n" << statementText_;
                return reportError(errorMessage);
            }
        }
    }

    // confirm that mysql and the caller agree on the number of paramters 
    if (paramCount_ > 0)
    {
        int parametersPassed = 0;
        for (Value::ConstMemberIterator itrsetting = settings_.MemberBegin();
    	     itrsetting != settings_.MemberEnd();
	     ++itrsetting)
        {
            const Value & setting = itrsetting->value;
            if (setting["param_type"] == MARKER) 
                parametersPassed++;
        }
        if (parametersPassed != paramCount_)
        {
            errorMessage << "MySql expects " << paramCount_ << " parameters in statement " << statementName_
                         << " but " << parametersPassed << " were passed";
            return reportError(errorMessage);
        }
    }
    return changeState(MYSQL_STMT_CREATED_STATE);
}

// Allocate arrays of MYSQL_BIND structs for parameters and results 
// on the heap. If there are parameters, 
int 
MySqlExecution::prepareToBind()
{
    stringstream errorMessage;
    int rc;

    // There are parameters. If we are not reusing a prior execution
    // allocate an array of MYSQL_BIND structs and a buffer to hold 
    // the parameter values
    if (paramCount_)
    {
        if (parameterBindArray_ == NULL)
            parameterBindArray_ = new MYSQL_BIND[paramCount_];
        memset(parameterBindArray_, 0, paramCount_*sizeof(MYSQL_BIND));
        paramBuffer_ = NULL;
        paramBufferLen_ = 0;

        // first loop through params to determine buffer size
        MYSQL_BIND * parameterBind = parameterBindArray_;
        for (Value::ConstMemberIterator itrsetting = settings_.MemberBegin();
             itrsetting != settings_.MemberEnd();
	     ++itrsetting)
        {
            const Value & setting = itrsetting->value;
            if (setting["param_type"] == MARKER)
            {
                paramBufferLen_ += bindParameter(setting, parameterBind, paramBuffer_);
                parameterBind++;
            }
        }
 
        // second loop through params to set buffer pointers
        if (paramBufferLen_ > 0)
        {
            paramBuffer_ = new char[paramBufferLen_];
            memset(paramBuffer_, 0, paramBufferLen_);
            char * valuePtr = paramBuffer_;
            MYSQL_BIND * parameterBind = parameterBindArray_;
            for (Value::ConstMemberIterator itrsetting = settings_.MemberBegin();
                itrsetting != settings_.MemberEnd();
	        ++itrsetting)
            {
                const Value & setting = itrsetting->value;
                if (setting["param_type"] == MARKER)
                {
                    bindParameter(setting, parameterBind, valuePtr);
                    parameterBind++;
                }
            } 
        }
    }

    // if the statement returns a result set, allocate column binds and a row buffer
    resultsMetadata_ = mysql_stmt_result_metadata(statementHandle_);
    if (resultsMetadata_ != NULL)
    {
        columnCount_ = mysql_num_fields(resultsMetadata_);
        assert(columnCount_ > 0);
        assert(columnBindArray_ == NULL);
        columnBindArray_ = new MYSQL_BIND[columnCount_];
        memset(columnBindArray_, 0, columnCount_*sizeof(MYSQL_BIND));
        rowBuffer_ = NULL;
        rowBufferLen_ = 0;
        MYSQL_BIND * columnBind = columnBindArray_;

        // first loop through columns to determine row buffer length
        for (unsigned int icol = 0; icol < columnCount_; icol++)
        {
            MYSQL_FIELD * fieldDescriptor = mysql_fetch_field_direct(resultsMetadata_, icol);
            rowBufferLen_ += bindColumn(fieldDescriptor, columnBind++, rowBuffer_);
        }
        rowBuffer_ = new char[rowBufferLen_];

        // second loop to store buffer pointers
        char * valuePtr = rowBuffer_;
        columnBind = columnBindArray_;
        for (unsigned int icol = 0; icol < columnCount_; icol++)
        {
            MYSQL_FIELD * fieldDescriptor = mysql_fetch_field_direct(resultsMetadata_, icol);
            bindColumn(fieldDescriptor, columnBind++, valuePtr);
        }
    }

    return changeState(BINDINGS_PREPARED_STATE);
}

// Pass the parameter bindings created by prepareToBind to the MySql service.
int 
MySqlExecution::bindParameters()
{
    stringstream errorMessage;

    if (paramCount_ != 0) 
    {
        my_bool result = mysql_stmt_bind_param(statementHandle_, parameterBindArray_);
        if (!result && mysql_stmt_errno(statementHandle_) != 0)
        {
            errorMessage << "binding parameters for statement " << statementName_;
            return reportMySqlError(statementHandle_, errorMessage);
        }
    }

    return changeState(STATEMENT_PREPARED_STATE);
}

// Execute the statement. If the statement is not the kind that
// returns rows (i.e there is no results meta-data), save the
// affected row count and transition to STATEMENT_COMPLETE_STATE 
// (a final state). Otherwise transition to EXECUTION_COMPLETE_STATE
// and retrieve the results.
int
MySqlExecution::executeStatement()
{
    stringstream errorMessage;
    executeTime_ = posix_time::microsec_clock::local_time();
    int rc = mysql_stmt_execute(statementHandle_);
    if (rc != 0)
    {
        errorMessage << "executing statement (" << rc << ") " << statementName_;
        return reportMySqlError(statementHandle_, errorMessage);
    }
    rowsAffected_ = 0;
    if (resultsMetadata_ == NULL)
    {
        rowsAffected_ = mysql_stmt_affected_rows(statementHandle_);
        return changeState(STATEMENT_COMPLETE_STATE);
    }
    else
        return changeState(EXECUTION_COMPLETE_STATE);
}

// Retrieve the results returned by the statement execution.
// The column MYSQL_BIND structs have already been set up by
// prepareToBind. For columns whose values are unpredictable
// a second buffer, the blob buffer, has been allocated, and
// the pointer in the MYSQL_BIND strut has been set to null.
// Values for these columns are retrieved one at a time by
// calling mysql_stmt_fetch_column. (See storeResultRow.)
int
MySqlExecution::retrieveResults()
{
    stringstream errorMessage;
    retrieveTime_ = posix_time::microsec_clock::local_time();
    
    MYSQL * db = connImpl_->getdb();

    int rc = mysql_stmt_bind_result(statementHandle_, columnBindArray_);
    if (rc != 0)
    {
        errorMessage << "binding results of statement " << statementName_;
        return reportMySqlError(statementHandle_, errorMessage);
    }
    if (blobBufferLen_ > 0)
        blobBuffer_ = new char[blobBufferLen_];

    // initialize the results document
    try
    {
        if (results_.IsObject())
            results_.RemoveAllMembers();
        else
            results_.SetObject();
        Value columns(kObjectType);
        for (unsigned int icol = 0; icol < columnCount_; icol++)
        {
            MYSQL_FIELD * fieldDescriptor = mysql_fetch_field_direct(resultsMetadata_, icol);
            Value fieldName(kStringType);
            fieldName.SetString(fieldDescriptor->name, fieldDescriptor->name_length, results_.GetAllocator());
            Value dataType(kNumberType);
            dataType.SetInt(fieldDescriptor->type);
            columns.AddMember(fieldName, dataType, results_.GetAllocator());
        }
        results_.AddMember("columns", columns, results_.GetAllocator());
        results_.AddMember("rows", Value(kArrayType).Move(), results_.GetAllocator());
    }
    catch (std::exception e)
    {
        errorMessage << "Error initializing results for " << statementName_;
        return reportError(errorMessage);
    }
    
    bool more = true;
    rowCount_ = 0;
    while (more)
    {
        rc = mysql_stmt_fetch(statementHandle_);
        switch (rc)
        {
            case 0:
            case MYSQL_DATA_TRUNCATED: 
            {
                rc = storeResultRow(); 
                if (rc != 0) return rc;
                rowCount_++;              
                break;
            }
            case 1:
                errorMessage << "fetching row for statement " << statementName_;
                return reportMySqlError(statementHandle_, errorMessage);
            case MYSQL_NO_DATA:
                more = false;
                break;
        }
    }
    return changeState(STATEMENT_COMPLETE_STATE);
}

// Transition to a new state. Alert each observer registered for
// the connection with the new state. An observer can change the target
// state. For example, when the replay observer sees the that new state
// is SQL_GENERATED_STATE, it locates the json doc for the prior instance
// of the execution, copies it into the current execution. and changes
// the state to whatever the state the prior instance finished in
// (either complete or error).
int
MySqlExecution::changeState(ExecutionState newState)
{
    if (newState == STATEMENT_COMPLETE_STATE || newState == ERROR_STATE)
        completeTime_ = posix_time::microsec_clock::local_time();
    ExecutionState realNewState = newState;
    for (MySqlConnection::ObserverList::iterator itrobs = conn_->observers_.begin();
         itrobs != conn_->observers_.end();
         ++itrobs)
    {
        ExecutionState observerState = (*itrobs)->onEvent(this, newState);
        if (observerState != newState) realNewState = observerState;
    }
    state_ = realNewState;
    return (errorNo_ ? errorNo_ : 0);
}

// Execution is complete. If we might re-use this execution,
// retain the statement handle and clear the execution-related data, 
// otherwise release everything
int 
MySqlExecution::close(bool isReusable)
{
    if (statementHandle_ != NULL)
    {
        if (resultsMetadata_ != NULL)
        {
            mysql_stmt_free_result(statementHandle_);
            resultsMetadata_ = NULL;
        }
        if (!isReusable)
        {
            mysql_stmt_close(statementHandle_);
            statementHandle_ = NULL;
            cleanup();
        }
    }
    return 0;
}

void
MySqlExecution::cleanup()
{
    if (parameterBindArray_ != NULL)
    {
        delete[] parameterBindArray_;
        parameterBindArray_ = NULL;
    }

    if (paramBuffer_ != NULL)
    {
        delete[] paramBuffer_;
        paramBuffer_ = NULL;
    }

    if (columnBindArray_ != NULL)
    {
        delete[] columnBindArray_;
        columnBindArray_ = NULL;
    }

    if (rowBuffer_ != NULL)
    {
        delete[] rowBuffer_;
        rowBuffer_ = NULL;
    }

    if (blobBuffer_ != NULL)
    {
        delete[] blobBuffer_;
        blobBuffer_ = NULL;
    }
}

// Bind a single SQL statement parameter, that is, determine the buffer space
// required for the parameter value and fill in its MYSQL_BIND struct.
// This method is called twice for each parameter, first without a buffer
// to determine  the space required for the value, and second, with a buffer.
// to set up for the mysql_stmt_bind_param call.
int
MySqlExecution::bindParameter(const Value & setting, MYSQL_BIND * parameterBind, char *& buffer)
{
    enum enum_field_types dataTypeCode = static_cast<enum enum_field_types>(setting["param_data_type"].GetInt());
    parameterBind->buffer_type = dataTypeCode;
    bool hasValue = setting.HasMember("param_value");
    int bufferSpaceRequired = 0;
    switch (dataTypeCode)
    {
        case MYSQL_TYPE_LONG:
        {
            bufferSpaceRequired = sizeof(long);
            if (buffer != NULL)
            {
                long * intValue = reinterpret_cast<long *>(buffer);
                if (hasValue)
                {
                    const Value & paramValue = setting["param_value"];
                    *intValue = paramValue.GetInt();
                    parameterBind->is_null = &mysqlFalse_;
                }
                else
                {
                    *intValue = 0;
                    parameterBind->is_null = &mysqlTrue_;
                }
                parameterBind->buffer = static_cast<void *>(buffer);
                parameterBind->buffer_length = sizeof(long);
                parameterBind->length = NULL;
            }
            break;
        }
        case MYSQL_TYPE_DOUBLE:
            bufferSpaceRequired = sizeof(double);
            if (buffer != NULL)
            {
                double * doubleValue = reinterpret_cast<double *>(buffer);
                if (hasValue)
                {
                    const Value & paramValue = setting["param_value"];
                    *doubleValue = paramValue.GetDouble();
                    parameterBind->is_null = &mysqlFalse_;
                }
                else
                {
                    *doubleValue = 0;
                    parameterBind->is_null = &mysqlTrue_;
                }
                parameterBind->buffer = static_cast<void *>(buffer);
                parameterBind->buffer_length = sizeof(double);
                parameterBind->length = NULL;
            }
            break;

        case MYSQL_TYPE_STRING:
        {
            bufferSpaceRequired = 0;
            if (hasValue)
            {
                const Value & paramValue = setting["param_value"];
                char * stringValue = const_cast<char *>(paramValue.GetString());
                parameterBind->buffer = static_cast<void *>(stringValue);
                parameterBind->buffer_length = strlen(stringValue);
                parameterBind->length = NULL;
                parameterBind->is_null = &mysqlFalse_;
            }
            else
            {
                parameterBind->buffer = NULL;
                parameterBind->buffer_length = 0;
                parameterBind->length = NULL;
                parameterBind->is_null = &mysqlTrue_;
            }
            break;
        }

        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
        {
            bufferSpaceRequired = sizeof(MYSQL_TIME);
            if (buffer != NULL)
            {
                parameterBind->is_null = &mysqlTrue_;
                if (hasValue)
                {
                    const Value & paramValue = setting["param_value"];
                    MYSQL_TIME * mysqlTimeValue = reinterpret_cast<MYSQL_TIME *>(buffer);
                    const char * timeArg = paramValue.GetString();
                    int rc = stringToMySqlTime(timeArg, dataTypeCode, mysqlTimeValue);
                    if (rc == 0)
                    {
                        parameterBind->buffer = static_cast<void *>(buffer);
                        parameterBind->buffer_length = sizeof(MYSQL_TIME);
                        parameterBind->length = NULL;
                        parameterBind->is_null = &mysqlFalse_;
                    }
                }
            }
        }
    }
    if (buffer != NULL && bufferSpaceRequired != 0) 
        buffer += bufferSpaceRequired;
    return bufferSpaceRequired;
}

// Bind a single result column, that is, deterrmine the space required to receive
// the column value and fill in the MYSQL_BIND structure for the column. This method
// is called twice, once without a buffer to determine the space required to hold
// a row, and then with a buffer, to set up for the mysql_stmt_bind_result call.

// For columns whose size is unpredictable (string, text and blob), update the estimated
// size of the of the buffer that will received these columns ('blobBufferSize')
int 
MySqlExecution::bindColumn(MYSQL_FIELD * fieldDescriptor, MYSQL_BIND * columnBind, char *& buffer)
{
    int bufferSpaceRequired = 0;
    int blobSpaceRequired = 0;
    enum enum_field_types columnType = fieldDescriptor->type;
    columnBind->buffer_type = columnType;

    // initialize for scalar value
    columnBind->buffer = reinterpret_cast<void *>(buffer);
    columnBind->buffer_length = fieldDescriptor->length;
    columnBind->length = NULL;
    bufferSpaceRequired = fieldDescriptor->length;

    // handle non-scalar columns
    switch (columnType)
    {
        case FIELD_TYPE_STRING:
        case FIELD_TYPE_VAR_STRING:
            bufferSpaceRequired = sizeof(long);
            if (buffer != NULL)
            {
                columnBind->buffer = NULL;
                columnBind->buffer_length = 0;
                unsigned long * lengthPtr = reinterpret_cast<unsigned long *>(buffer);
                columnBind->length = lengthPtr;
                blobSpaceRequired = max(fieldDescriptor->length, fieldDescriptor->max_length);
            }
            break;

        case FIELD_TYPE_DATE:
        case FIELD_TYPE_TIME:
        case FIELD_TYPE_DATETIME:
        case FIELD_TYPE_TIMESTAMP:
            bufferSpaceRequired = sizeof(MYSQL_TIME);
            columnBind->buffer_length = bufferSpaceRequired;
            break;
    }

    if (buffer != NULL)
    {
        my_bool * isNull = reinterpret_cast<my_bool *>(buffer + bufferSpaceRequired);
        columnBind->is_null = isNull;
    }
    bufferSpaceRequired += sizeof(my_bool);
    if (buffer != NULL) 
        buffer += bufferSpaceRequired;
    if (blobSpaceRequired > 0) 
        blobBufferLen_ = max(blobBufferLen_, blobSpaceRequired);
    return bufferSpaceRequired;           
}

// Retrieve one results row.
int
MySqlExecution::storeResultRow()
{
    stringstream errorMessage;

    Value & rows = results_["rows"];
    rows.PushBack(Value(kObjectType).Move(), results_.GetAllocator());
    Value & row = rows[rows.Size()-1];
    MYSQL_BIND * columnBind = columnBindArray_;
    for (int icol = 0; icol < columnCount_; ++icol, ++columnBind)
    {
        MYSQL_FIELD * fieldDescriptor = mysql_fetch_field_direct(resultsMetadata_, icol);
        Value fieldName(fieldDescriptor->name, fieldDescriptor->name_length, results_.GetAllocator());
        if (*columnBind->is_null)
        {
            row.AddMember(fieldName, Value(kNullType).Move(), results_.GetAllocator());
            continue;
        }
        switch (fieldDescriptor->type)
        {
            case FIELD_TYPE_LONG:
                row.AddMember(fieldName, Value(*static_cast<int *>(columnBind->buffer)).Move(), results_.GetAllocator());
                break;

            case FIELD_TYPE_LONGLONG:
                row.AddMember(fieldName, Value(*static_cast<int64_t *>(columnBind->buffer)).Move(), results_.GetAllocator());
                break;

            case FIELD_TYPE_DOUBLE:
                row.AddMember(fieldName, Value(*static_cast<double*>(columnBind->buffer)).Move(), results_.GetAllocator());
                break;

            case FIELD_TYPE_STRING:
            case FIELD_TYPE_VAR_STRING:
            case FIELD_TYPE_ENUM:
            {
                int actualLength = *columnBind->length;
                columnBind->buffer = getBlobBuffer(actualLength);
                columnBind->buffer_length = actualLength;
                int rc = mysql_stmt_fetch_column(statementHandle_, columnBind, icol, 0);
                if (rc != 0)
                {
                   errorMessage << "fetching string column " << fieldName.GetString()
                                << " in statement " << statementName_;
                   return reportMySqlError(statementHandle_, errorMessage);
                }
                row.AddMember(fieldName, 
                              Value(static_cast<char *>(columnBind->buffer), actualLength, results_.GetAllocator()).Move(), 
                              results_.GetAllocator());
                columnBind->buffer = NULL;
                columnBind->buffer_length = 0;
                break;
            }

            case FIELD_TYPE_DATE:
            case FIELD_TYPE_TIME:
            case FIELD_TYPE_DATETIME:
            case FIELD_TYPE_TIMESTAMP:
            {
                MYSQL_TIME * mysqlTime = reinterpret_cast<MYSQL_TIME *>(columnBind->buffer);
                row.AddMember(fieldName, Value(kObjectType).Move(), results_.GetAllocator());
                Value & timeValue = row[fieldDescriptor->name];
                if (fieldDescriptor->type != FIELD_TYPE_TIME)
                {
                     timeValue.AddMember("year", Value(mysqlTime->year).Move(), results_.GetAllocator());
                     timeValue.AddMember("month", Value(mysqlTime->month).Move(), results_.GetAllocator());
                     timeValue.AddMember("day", Value(mysqlTime->day).Move(), results_.GetAllocator());
                }
                if (fieldDescriptor->type != FIELD_TYPE_DATE)
                {
                     timeValue.AddMember("hour", Value(mysqlTime->hour).Move(), results_.GetAllocator());
                     timeValue.AddMember("minute", Value(mysqlTime->minute).Move(), results_.GetAllocator());
                     timeValue.AddMember("second", Value(mysqlTime->second).Move(), results_.GetAllocator());
                     if (mysqlTime->second_part != 0)
                     {
                         timeValue.AddMember("second_part", Value(mysqlTime->second_part).Move(), results_.GetAllocator());
                     }
                }
                break;
            }

            default:
            {
                errorMessage << "Column " << fieldDescriptor->name 
                             << " has unsupported type " << fieldDescriptor->type;
                reportError(errorMessage);
                return 1;
            }
        }
    }
    return 0;
}

char *
MySqlExecution::getBlobBuffer(int bufferSize)
{
    if (bufferSize > blobBufferLen_) 
    {
        if (blobBuffer_ != NULL) 
            delete[] blobBuffer_;
        blobBufferLen_ = bufferSize;
        blobBuffer_ = new char[blobBufferLen_];
    }
    return blobBuffer_;    
}

// Convert the ISO string representation of a time/date to
// a MYSQL_TIME struct for transmission the MySQL server.
// Accepts dates, times and datetimes.
int               
MySqlExecution::stringToMySqlTime(const char * timeString, enum enum_field_types typeCode, MYSQL_TIME * mysqlTime)
{
    stringstream errorMessage;

    unsigned int year = 0;
    unsigned int month = 0;
    unsigned int day = 0;
    unsigned int hour = 0;
    unsigned int minute = 0;
    unsigned int second = 0;
    unsigned long second_part = 0;

    if (strcmp(timeString, "not-a-date-time") == 0) return -1;

    if (typeCode != MYSQL_TYPE_TIME) // starts with date
    {
        const char * datePattern = "^(\\d+)[-_/](\\d+)[-_/](\\d+)";
        const regex dateRegex(datePattern);
        cmatch dateMatch;
        if (!regex_search(timeString, dateMatch, dateRegex))
        {
            errorMessage << "Parameter \'" << timeString << "\' not in correct format: "
                         << "expect yyyy-mm-dd";
            return reportError(errorMessage);
	}
	year = atoi(dateMatch[1].first);
        month = atoi(dateMatch[2].first);
        day = atoi(dateMatch[3].first);
        if (year < 100) year += 2000;
        if ((year < 1970 || year > 3000) && year != 9999)
        {
            errorMessage << "Illegal year " << year << " in parameter \'" << timeString << "\'";
            return reportError(errorMessage);
        }
        if (month < 1 || month > 12)
        {
            errorMessage << "Illegal month " << month << " in parameter \'" << timeString << "\'";
            return reportError(errorMessage);
        }
        if (day < 1 || day > 31)
        {
            errorMessage << "Illegal day " << day << " in parameter \'" << timeString << "\'";
            return reportError(errorMessage);
        }
    } 

    if (typeCode != MYSQL_TYPE_DATE)
    {
        const char * timePattern = "[^T](\\d+):(\\d+):(\\d+)\\.(\\d+)?";
        const regex timeRegex(timePattern);
        cmatch timeMatch;
        if (!regex_search(timeString, timeMatch, timeRegex))
        {
            errorMessage << "Parameter \'" << timeString << "\' not in correct format: "
                         << "expect hh:mm:ss.ffffff";
            return reportError(errorMessage);
	}
        hour = atoi(timeMatch[0].first);
        minute = atoi(timeMatch[2].first);
        second = atoi(timeMatch[3].first);
        if (timeMatch.size() > 4)
            second_part = atoi(timeMatch[4].first);
    }

    if (mysqlTime == NULL) return 0; // just validating

    memset(mysqlTime, 0, sizeof(MYSQL_TIME));
    mysqlTime->year = year;
    mysqlTime->month = month;
    mysqlTime->day = day;
    mysqlTime->hour = hour;
    mysqlTime->minute = minute;
    mysqlTime->second = second;
    mysqlTime->second_part = second_part;
    return 0;
}

const Document &
MySqlExecution::asJson()
{
    if (!dom_.IsObject())
        toJson();
    return dom_;
}

// Render the contents of an execution as a json dom. The dom may either 
// provide field values for an audit record, or it may be serialized to
// a json file for later replay.
void
MySqlExecution::toJson() 
{
    dom_.SetObject();

    // statement_name
    Value nameValue(statementName_.c_str(), statementName_.size(), dom_.GetAllocator());
    dom_.AddMember("statement_name", nameValue, dom_.GetAllocator());

    // comment
    if (!comment_.empty())
    {
        Value commentValue(comment_.c_str(), comment_.size(), dom_.GetAllocator());
        dom_.AddMember("comment", commentValue, dom_.GetAllocator());
    }

    // statement_text
    Value textValue(statementText_.c_str(), statementText_.size(), dom_.GetAllocator());
    dom_.AddMember("statement_text", textValue, dom_.GetAllocator());

    // program (used for audit filtering)
    string program = conn_->getCurrentProgram();
    if (!program.empty())
    {
        Value programValue(program.c_str(), program.size(), dom_.GetAllocator());
        dom_.AddMember("program", programValue, dom_.GetAllocator());
    }

    // transaction(used for audit filtering)
    const string & transaction = conn_->getCurrentTransaction();
    if (!transaction.empty())
    {
        Value transactionValue(transaction.c_str(), transaction.size(), dom_.GetAllocator());
        dom_.AddMember("transaction", transactionValue, dom_.GetAllocator());
    }

    // state (used by replay)
    dom_.AddMember("state", getState(), dom_.GetAllocator());

    // rc
    dom_.AddMember("rc", rc_, dom_.GetAllocator());

    // rows_returned
    dom_.AddMember("rows_returned", rowCount_, dom_.GetAllocator());

    // rows_affected
    dom_.AddMember("rows_affected", rowsAffected_, dom_.GetAllocator());

    // error_message
    Value messageValue(errorMessage_.c_str(), errorMessage_.size(), dom_.GetAllocator());
    dom_.AddMember("error_message", messageValue, dom_.GetAllocator());

    // error_no
    dom_.AddMember("error_no", errorNo_, dom_.GetAllocator());

    // start_time: when the execution was created (yyyy-mm-ddThh:mm:ss.fffff)
    string startTimeString = posix_time::to_iso_extended_string(startTime_);
    Value startTimeValue(startTimeString.c_str(), startTimeString.size(), dom_.GetAllocator());
    dom_.AddMember("start_time", startTimeValue, dom_.GetAllocator());

    // execute_time: when the statement was submitted to MySql
    string executeTimeString = posix_time::to_iso_extended_string(executeTime_);
    Value executeTimeValue(executeTimeString.c_str(), executeTimeString.size(), dom_.GetAllocator());
    dom_.AddMember("execute_time", executeTimeValue, dom_.GetAllocator());

    // retrieve_time: when results retrieval started
    string retrieveTimeString = posix_time::to_iso_extended_string(retrieveTime_);
    Value retrieveTimeValue(retrieveTimeString.c_str(), retrieveTimeString.size(), dom_.GetAllocator());
    dom_.AddMember("retrieve_time", retrieveTimeValue, dom_.GetAllocator());

    // complete time: when the execution was complete
    string completeTimeString = posix_time::to_iso_extended_string(completeTime_);
    Value completeTimeValue(completeTimeString.c_str(), completeTimeString.size(), dom_.GetAllocator());
    dom_.AddMember("complete_time", completeTimeValue, dom_.GetAllocator());

    // copy in the dom containing the parameter names and values 
    if (settings_.IsObject() && !settings_.ObjectEmpty())
    {
        Value settings(kObjectType);
        settings.CopyFrom(settings_, dom_.GetAllocator());
        dom_.AddMember("parameters", settings, dom_.GetAllocator());
    }

    // copy in the dom containing the results
    if (results_.IsObject() && !results_.ObjectEmpty())
    {
        Value results(kObjectType);
        results.CopyFrom(results_, dom_.GetAllocator());
        dom_.AddMember("results", results, dom_.GetAllocator());
    }

    // user (for audit)
    const char * user = conn_->getUser();
    Value userValue(user, strlen(user), dom_.GetAllocator());
    dom_.AddMember("user", userValue, dom_.GetAllocator());

    // host (for audit)
    const char * host = conn_->getUser();
    Value hostValue(host, strlen(host), dom_.GetAllocator());
    dom_.AddMember("host", hostValue, dom_.GetAllocator());
}

// Determine whether we can re-use the prepared statement. Note that we
// don't re-use if the current instance is part of a transaction and the
// previous instance isn't. This is because MySql appears to remember the
// autocommit setting of the previous execution and will do things like
// FK constraint validation that should be deferred until commit.
bool
MySqlExecution::isSameStatementAs(const MySqlExecution * otherExecution) const
{
    if (otherExecution->statementName_ != statementName_) return false;
    if (otherExecution->statementText_ != statementText_) return false;
    if (otherExecution->isAutoCommit_ != isAutoCommit_) return false;  
    return true;
}

// Re-use statement handle and MYSQL_BINDS allocated for a prior execution.
// (We can't reuse the parameter buffer bucause its size may change because
// of string-valued parameters.)
void
MySqlExecution::moveFrom(MySqlExecution * previousExecution)
{
    statementHandle_ = previousExecution->statementHandle_;
    previousExecution->statementHandle_ = NULL;
    paramCount_ = previousExecution->paramCount_;
    parameterBindArray_ = previousExecution->parameterBindArray_;
    previousExecution->parameterBindArray_ = NULL;
}

// Compare this live execution with the json serialization of an earlier execution. 
// Called by the replay observer before the statement is actually executed, 
// so compare only name and text
bool
MySqlExecution::isSameAs(const Value & dom, stringstream & errorMessage) const
{
    if (dom["statement_name"] != statementName_.c_str())
    {
        errorMessage << "Statement names don\'t match: " 
                     << dom["statement_name"].GetString() << " NE " << statementName_;
        return false;
    }
    if (dom["statement_text"] != statementText_.c_str())
    {
        errorMessage << "Statement texts don\'t match"; 
        return false;
    }
    return true;
}

int
MySqlExecution::reportMySqlError(MYSQL_STMT * statementHandle, const stringstream & context)
{
    stringstream errorMessage;
    errorMessage << "MySql error " << context.str()
                 << ": " << mysql_stmt_error(statementHandle)
                 << " (" << mysql_stmt_errno(statementHandle) << ")";
    return reportError(errorMessage, mysql_stmt_errno(statementHandle));
}

int
MySqlExecution::reportError(const stringstream & errorMessage, int errorNo)
{
    return reportError(errorMessage.str(), errorNo);
}

// Record the error details in the execution object, and change the state
// to ERROR_STATE. Calling reportError terminates the execution. Pass the
// error up the containing connection.
int
MySqlExecution::reportError(const string & errorMessage, int errorNo)
{
    rc_ = errorNo;
    errorNo_ = errorNo;
    errorMessage_ = errorMessage;
    changeState(ERROR_STATE);
    return conn_->reportError(errorMessage, errorNo, executionHandle_);
}

// Render an execution as "f(arg1 [,arg2....])" where f is the statement name
// and argN is the value assigned to the Nth parameter
ostream & operator<<(ostream & o, const MySqlExecution & execution)
{
    string arguments;
    const Document & settings = execution.getSettings();
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<StringBuffer> writer(buffer);
    for (Value::ConstMemberIterator itrsetting = settings.MemberBegin();
         itrsetting != settings.MemberEnd();
	 ++itrsetting)
    {
        buffer.Clear();
        writer.Reset(buffer);
        const Value & setting = itrsetting->value;
        string argstring;
	if (setting.HasMember("param_value"))
	{
            const Value & arg = setting["param_value"];
            arg.Accept(writer);
            argstring = buffer.GetString();
	    if (argstring.size() > 64)
	    {
                size_t seploc = argstring.find_first_of(" :.;\r\n\t");
                argstring = argstring.substr(0, seploc) + "...";
            }
        }
        if (!arguments.empty()) arguments += ", ";
        arguments += argstring;
    }

    return o << execution.getStatementName() << "(" << arguments << ")";
}

