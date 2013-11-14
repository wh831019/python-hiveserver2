import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
import sasl
from cloudera.thrift_sasl import TSaslClientTransport

from TCLIService import TCLIService
from TCLIService.ttypes import TOpenSessionReq, TGetTablesReq, TFetchResultsReq,\
  TStatusCode, TGetResultSetMetadataReq, TGetColumnsReq, TType, TTypeId, \
  TExecuteStatementReq, TGetOperationStatusReq, TFetchOrientation, TCloseOperationReq, \
  TCloseSessionReq, TGetSchemasReq, TCancelOperationReq

## optional auth values:
## PLAIN:  when 'hive.server2.authentication' is set as 'LDAP' or 'NONE'
## NOSASL: when 'hive.server2.authentication' is set as 'NOSASL'
auth = 'PLAIN' # PLAIN or NOSASL
username = ''
password = ''
host = 'localhost'
port = 10000
test_hql = 'select * from foo limit 10'

def get_type(typeDesc):
    for ttype in typeDesc.types:
        if ttype.primitiveEntry is not None:
            return TTypeId._VALUES_TO_NAMES[ttype.primitiveEntry.type]
        elif ttype.mapEntry is not None:
            return ttype.mapEntry
        elif ttype.unionEntry is not None:
            return ttype.unionEntry
        elif ttype.arrayEntry is not None:
            return ttype.arrayEntry
        elif ttype.structEntry is not None:
            return ttype.structEntry
        elif ttype.userDefinedTypeEntry is not None:
            return ttype.userDefinedTypeEntry

def get_value(colValue):
    if colValue.boolVal is not None:
      return colValue.boolVal.value
    elif colValue.byteVal is not None:
      return colValue.byteVal.value
    elif colValue.i16Val is not None:
      return colValue.i16Val.value
    elif colValue.i32Val is not None:
      return colValue.i32Val.value
    elif colValue.i64Val is not None:
      return colValue.i64Val.value
    elif colValue.doubleVal is not None:
      return colValue.doubleVal.value
    elif colValue.stringVal is not None:
      return colValue.stringVal.value

# for SASL connection
def sasl_factory():
    saslc = sasl.Client()
    saslc.setAttr("username", username)
    saslc.setAttr("password", password)
    saslc.init()
    return saslc

try:

    print "1) Preparing the connection..."
    sock = TSocket(host, port)
    if auth == 'NOSASL':
        transport = TBufferedTransport(sock)
    else:
        transport = TSaslClientTransport(sasl_factory, "PLAIN", sock)
    client = TCLIService.Client(TBinaryProtocol(transport))
    transport.open()

    print "\n2) Opening Session..."
    res = client.OpenSession(TOpenSessionReq(username=username, password=password))
    session = res.sessionHandle
    print('Session opened. ( %s )' % session.sessionId)

    ## 3) Show tables
    print "\n3) Try fetching table list..."
    query = TExecuteStatementReq(session, statement="show tables", confOverlay={})
    response = client.ExecuteStatement(query)
    opHandle = response.operationHandle

    fetchReq = TFetchResultsReq(operationHandle=opHandle, orientation=TFetchOrientation.FETCH_NEXT, maxRows=100);
    resultsRes = client.FetchResults(fetchReq);

    ## close operation && release lock
    req = TCloseOperationReq(operationHandle=opHandle)
    client.CloseOperation(req)

    print('-'*32)
    for row in resultsRes.results.rows:
        print row.colVals[0].stringVal.value
    print('-'*32)

    # 4) try execute HQL
    print "\n4) Executing Test HQL: %s..." % test_hql
    query = TExecuteStatementReq(session, statement=test_hql, confOverlay={})
    response = client.ExecuteStatement(query)
    opHandle = response.operationHandle

    print('-'*32)
    meta = []
    if opHandle.hasResultSet:
        metaReq = TGetResultSetMetadataReq(operationHandle=opHandle)
        schema = client.GetResultSetMetadata(metaReq).schema
        for i, col in enumerate(schema.columns):
            type = get_type(col.typeDesc)
            name = col.columnName
            meta.append(type)
            if i == 0:
                print name,
            else:
                print ', ' + name,
        print

    print('-'*32)
    fetchReq = TFetchResultsReq(operationHandle=opHandle, orientation=TFetchOrientation.FETCH_NEXT, maxRows=100);
    resultsRes = client.FetchResults(fetchReq);
    for row in resultsRes.results.rows:
        for i, col in enumerate(row.colVals):
            if i == 0:
                print get_value(col),
            else:
                print ', ' + str(get_value(col)),
        print

    print('-'*32)

    req = TCloseOperationReq(operationHandle=opHandle)
    client.CloseOperation(req)

    print "\n# 5) Closing Session..."
    req = TCloseSessionReq(sessionHandle=session)
    client.CloseSession(req)
    print("Bye")

except Exception, e:
    print e
