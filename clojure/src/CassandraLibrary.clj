(def accountIdTemp "38488a4c-babd-48e8-a127-c89b0b9ac944")   ;;global Temp
(def tokenTemp "LerldAfIAk")                                 ;;global Temp

;;init value
(set! (. io.cassandra.sdk.constants.APIConstants API_VERSION) "1")

; Status Message API
(defn isOK [StatusMessageObject]
  (.isOk StatusMessageObject)                                ;return bool
 )
(defn getMessageObj [StatusMessageObject]
  (.getMessage StatusMessageObject)                          ;return string with message
  )
(defn getStatusObj [StatusMessageObject]
  (.getStatus StatusMessageObject)                           ;return string with status
  )
(defn getDetailObj [StatusMessageObject]
  (.getDetail StatusMessageObject)                           ;return string with detail
  )
(defn getTtlObj [StatusMessageObject]
  (.getTtl StatusMessageObject)                              ;return integer
  )
(defn getErrorObj [StatusMessageObject]
  (.getError StatusMessageObject)                            ;return string with Error
  )

(defn getAllInfoStatus [StatusMessageObject]
  (.concat
    (.concat
      (.concat
        (.concat (getStatusObj StatusMessageObject) "\n")
        (getMessageObj StatusMessageObject)) "\n")
    (getDetailObj StatusMessageObject)
  )                                                          ;return string with full status
 )

; KeySpace API
(import 'io.cassandra.sdk.keyspace.KeyspaceAPI)
(defn deleteKeyspace [keySpaceName accountId token]
  (def keySpace
    (KeyspaceAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.deleteKeyspace keySpace keySpaceName)                    ;return StatusMessageObject
  )

(defn createKeyspace [keySpaceName accountId token]
  (def keySpace
    (KeyspaceAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.createKeyspace keySpace keySpaceName)                    ;return StatusMessageObject
  )

;ColumnFamily API
(import 'io.cassandra.sdk.columnfamily.ColumnFamilyAPI)
(defn createColumnFamily [keyspaceName columnFamilyName comparatorType accountId token]
  (def columnFamily
    (ColumnFamilyAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.createColumnFamily columnFamily keyspaceName columnFamilyName comparatorType)
                                                             ;return StatusMessageObject
  )

(defn deleteColumnFamily [keyspaceName columnFamilyName accountId token]
  (def columnFamily
    (ColumnFamilyAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.deleteColumnFamily columnFamily keyspaceName columnFamilyName)
                                                             ;return StatusMessageObject
  )

(defn getColumnFamilyObjects [keyspaceName accountId token]
  (def columnFamily
    (ColumnFamilyAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
    (to-array
      (.getColumnfamilies
        (.getColumnFamilies columnFamily keyspaceName )))    ;return array with columnfamily objects
)
(defn getColumnFamily [accountId token keyspaceName columnFamilyName]
  (def columnFamily
    (ColumnFamilyAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.getColumnFamily columnFamily keyspaceName columnFamilyName)        ;return columnfamily object
  )


(defn getNameColumnFamily [columnFamilyObject]
  (.getName (.getColumnfamily columnFamilyObject))           ;return string with name columnfamily
 )

(defn getSortedby [columnFamilyObject]
  (.getSortedby (.getColumnfamily columnFamilyObject))       ;return string with name columnfamily
  )

(defn getColumnFamiliesName [keyspaceName accountId token]
  (def colunmFamilyArray
    (getColumnFamilyObjects keyspaceName accountId token))
  (def returnVector (vector))
  (areduce colunmFamilyArray i ret ()  (def returnVector
                                         (conj returnVector
                                           (getNameColumnFamily
                                             (aget colunmFamilyArray i)))))
  (vec returnVector)                                         ;return vector with ColumnFamily name
 )

(defn getColumnFamiliesNameAndSortedBy [keyspaceName accountId token]
  (def colunmFamilyArray
    (getColumnFamilyObjects keyspaceName accountId token))
  (def returnVector (vector))
  (areduce colunmFamilyArray i ret ()  (def returnVector
                                         (conj returnVector [(getNameColumnFamily (aget colunmFamilyArray i)) (getSortedby (aget colunmFamilyArray i))])))
  (vec returnVector)                                         ;return 2d vector with ColumnFamily name and Sorted by
  )

;Columns API
(import 'io.cassandra.sdk.column.ColumnAPI)
(defn upsertColumn [accountId token keySpaceName columnFamilyName columnName comparatorType isIndex]
  (def column
    (ColumnAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
    (.upsertColumn column keySpaceName columnFamilyName columnName comparatorType isIndex)
                                                             ;return StatusMessageObject
  )

(defn getNameColumn [columnObject]
  (.getName columnObject)                                    ;return string with name column
  )

(defn isIndexColumn [columnObject]
  (.isIndex columnObject)                                    ;return boolean index
  )

(defn getValidatorColumn [columnObject]
  (.getValidator columnObject)                               ;return string with validator column
  )

(defn getColumnesObjects [accountId token keySpaceName columnFamilyName]
  (def colunmFamily
    (getColumnFamily accountId token keySpaceName columnFamilyName))
  (to-array (.getColumns colunmFamily))                      ;return array with column objects
  )

(defn getNameColumes [accountId token keySpaceName columnFamilyName]
  (def columsArray (getColumnesObjects accountId token keySpaceName columnFamilyName))
  (def returnVector (vector))
  (areduce columsArray i ret () (def returnVector
                                  (conj returnVector
                                    (getNameColumn
                                      (aget columsArray i)))))
  (vec returnVector)                                         ;return vector with Columns name
  )

(defn getAllInfoColumes [accountId token keySpaceName columnFamilyName]
  (def columsArray (getColumnesObjects accountId token keySpaceName columnFamilyName))
  (def returnVector (vector))
  (areduce columsArray i ret () (def returnVector
                                  (conj returnVector
                                    [(getNameColumn (aget columsArray i)) (getValidatorColumn (aget columsArray i)) (isIndexColumn (aget columsArray i))]
                                    )))
  (vec returnVector)                                         ;return 2d vector with information about columns
  )

;Counter API
(import 'io.cassandra.sdk.counter.CounterAPI)
(defn getCounter [accountId token keySpaceName columnFamilyName columnName rowKey]
  (def myCounter
    (CounterAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.getCounter myCounter keySpaceName columnFamilyName rowKey columnName)
                                                              ;return LinkedHashMap with information about Counter
  )

(defn incrementCounter [accountId token keySpaceName columnFamilyName columnName rowKey incrementValue]
  (def myCounter
    (CounterAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
    (.incrementCounter myCounter keySpaceName columnFamilyName rowKey columnName incrementValue)
                                                              ;return StatusMessageObject
  )

(defn decrementCounter [accountId token keySpaceName columnFamilyName columnName rowKey decrementValue]
  (def myCounter
    (CounterAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.decrementCounter myCounter keySpaceName columnFamilyName rowKey columnName decrementValue)
                                                              ;return StatusMessageObject
  )

;CQL API does not work
(import 'io.cassandra.sdk.cql.CqlAPI)
(defn executeCqlQuery [accountId token keySpaceName columnFamilyName cqlQuery csvFormat]
  (def myCql
    (CqlAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
   (.executeCqlQuery myCql keySpaceName columnFamilyName cqlQuery csvFormat)

  )

;Data API
(import 'io.cassandra.sdk.data.DataAPI)
(defn getData [accountId token keySpaceName columnFamilyName rowKey limit fromKey]
  (def data
    (DataAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.getData data keySpaceName columnFamilyName rowKey limit fromKey)
                                                              ;return LinkedHashMap
  )


;;(println (getAllInfoStatus (createKeyspace "base2" accountIdTemp tokenTemp)))
;;(println (getAllInfoStatus (deleteKeyspace "base2" accountIdTemp tokenTemp)))
;;(println (getAllInfoStatus(createColumnFamily "base2" "tab7" "UTF8Type" accountIdTemp tokenTemp)))
;;(println (getAllInfoStatus(deleteColumnFamily "base2" "tab6" accountIdTemp tokenTemp)))
;;(println (getColumnFamiliesName "base2" accountIdTemp tokenTemp))
;;(println (getColumnFamiliesNameAndSortedBy "base2" accountIdTemp tokenTemp))
;;(println (getAllInfoStatus(upsertColumn accountIdTemp tokenTemp "base2" "tab2" "column2" "AsciiType" true)))
;;(println (getNameColumes accountIdTemp tokenTemp "base2" "tab1") )
;;(println (getAllInfoColumes accountIdTemp tokenTemp "base2" "tab1"))
;;(println (getAllInfoStatus (incrementCounter accountIdTemp tokenTemp "base2" "tab1" "column1" "0" 5)))
;;(println (getAllInfoStatus (decrementCounter accountIdTemp tokenTemp "base2" "tab1" "column1" "0" 5)))
;;(println (getCounter accountIdTemp tokenTemp "base2" "tab1" "column1" "0"))
;;(println (executeCqlQuery accountIdTemp tokenTemp "base2" "tab1" "select *" false))
(println (getData accountIdTemp tokenTemp "base2" "tab1" "0" 0 ""))

