(def accountIdTemp "38488a4c-babd-48e8-a127-c89b0b9ac944")  ;;global Temp
(def tokenTemp "LerldAfIAk")                                ;;global Temp

;;init value
(set! (. io.cassandra.sdk.constants.APIConstants API_VERSION) "1")

; Status Message API
(defn isOK [StatusMessageObject]
  (.isOk StatusMessageObject)                               ;return bool
 )
(defn getMessageObj [StatusMessageObject]
  (.getMessage StatusMessageObject)                         ;return string with message
  )
(defn getStatusObj [StatusMessageObject]
  (.getStatus StatusMessageObject)                          ;return string with status
  )
(defn getDetailObj [StatusMessageObject]
  (.getDetail StatusMessageObject)                          ;return string with detail
  )
(defn getTtlObj [StatusMessageObject]
  (.getTtl StatusMessageObject)                             ;return integer
  )
(defn getErrorObj [StatusMessageObject]
  (.getError StatusMessageObject)                           ;return string with Error
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
  (.deleteKeyspace keySpace keySpaceName)                   ;return StatusMessageObject
  )

(defn createKeyspace [keySpaceName accountId token]
  (def keySpace
    (KeyspaceAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (.createKeyspace keySpace keySpaceName)                   ;return StatusMessageObject
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
        (.getColumnFamilies columnFamily keyspaceName )))   ;return array with columnfamily objects
)

(defn getNameColumnFamily [columnFamilyObject]
  (.getName (.getColumnfamily columnFamilyObject))          ;return string with name columnfamily
 )

(defn getSortedby [columnFamilyObject]
  (.getSortedby (.getColumnfamily columnFamilyObject))      ;return string with name columnfamily
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

;;(println (getAllInfoStatus (createKeyspace "base2" accountIdTemp tokenTemp)))
;;(println (getAllInfoStatus (deleteKeyspace "base2" accountIdTemp tokenTemp)))
;;(println (getAllInfoStatus(createColumnFamily "base2" "tab7" "UTF8Type" accountIdTemp tokenTemp)))
;;(println (getAllInfoStatus(deleteColumnFamily "base2" "tab6" accountIdTemp tokenTemp)))
(println (getColumnFamiliesNameAndSortedBy "base2" accountIdTemp tokenTemp))






