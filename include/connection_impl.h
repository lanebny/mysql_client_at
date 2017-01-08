#ifndef __connection_impl_h__
#define __connection_impl_h__

#include <mysql.h>
#include <rapidjson/document.h>

#include "observer.h"

using namespace rapidjson;
using namespace std;



//                    M Y S Q L  C O N N E C T I O N  I M P L

class MySqlConnectionImpl
{
    friend class MySqlConnection;
  
public:
    typedef MySqlExecution::ExecutionState ExecutionState;

    struct MySqlLibrary
    {
        MySqlLibrary();
        ~MySqlLibrary();
    };

public:
    MySqlConnectionImpl(MySqlConnection * conn,
			const char *      databaseName, 
                        const char *      statementPath,
                        const char *      user,
                        const char *      password,
                        const char *      host,
                        int               port,
                        const char *      socket,
                        unsigned long     flags);
    ~MySqlConnectionImpl();

    MYSQL *           getdb();
    const Document &  getStatements();
    void              startMySqlThread();
    void              endMySqlThread();
    ExecutionState    changeState(ExecutionState prevState);
    MySqlExecution *  findLivePriorExecution(MySqlExecution * execution) const;
    bool              isAutoCommit() const { return isAutoCommit_; }
    int               setAutoCommit(bool isAutoCommit);
    int               commit();
    int               rollback();
    int               addObserver(const char * observerName, ObserverType observerType);
    int               removeObserver(const char * observerName);
    void              close();
    int               reportMySqlError(const stringstream & context);
    static void       printValue(const Value & value, ostream & outputStream);

private:
    int               loadStatements();
    int               open();

private:
    MySqlConnection *    conn_;
    MYSQL *              db_;
    const char *         databaseName_;
    const char *         statementPath_;
    Document             statementDict_;
    bool                 statementsLoaded_;
    const char *         user_;
    const char *         password_; 
    const char *         host_;
    int                  port_;
    const char *         socket_;
    unsigned long        flags_;
    bool                 isOpen_;
    bool                 isAutoCommit_;

    static MySqlLibrary  library_;

};  // MySqlConnectionImpl

#endif // __connection_impl_h__
