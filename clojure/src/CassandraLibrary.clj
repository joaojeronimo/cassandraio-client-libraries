(def accountIdTemp "38488a4c-babd-48e8-a127-c89b0b9ac944")  ;;global Temp
(def tokenTemp "LerldAfIAk")                                ;;global Temp

;;init value
(set! (. io.cassandra.sdk.constants.APIConstants API_VERSION) "1")

; KeySpace API
(import 'io.cassandra.sdk.keyspace.KeyspaceAPI)
(defn deleteKeyspace [keySpaceName accountId token]
  (def keySpace
    (KeyspaceAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))

  (println
    (.getDetail
      (.deleteKeyspace keySpace keySpaceName)))
  )

(defn createKeyspace [keySpaceName accountId token]
  (def keySpace
    (KeyspaceAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))

  (println
    (.getDetail
      (.createKeyspace keySpace keySpaceName)))
  )

;ColumnFamily API
(import 'io.cassandra.sdk.columnfamily.ColumnFamilyAPI)
(defn createColumnFamily [keyspaceName columnFamilyName comparatorType accountId token]
  (def columnFamily
    (ColumnFamilyAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))

  (println (.getDetail
             (.createColumnFamily columnFamily keyspaceName columnFamilyName comparatorType)))
  )

(defn deleteColumnFamily [keyspaceName columnFamilyName accountId token]
  (def columnFamily
    (ColumnFamilyAPI. io.cassandra.sdk.constants.APIConstants/API_URL token accountId))
  (println
    (.getDetail
      (.deleteColumnFamily columnFamily keyspaceName columnFamilyName)))
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

(defn getColumnFamiliesName [keyspaceName accountId token]
  (def colunmFamilyArray
    (getColumnFamilyObjects keyspaceName accountId token))
  (def returnVector (vec (object-array 0)))
  (areduce colunmFamilyArray i ret ()  (def returnVector
                                         (conj returnVector
                                           (getNameColumnFamily
                                             (aget colunmFamilyArray i)))))
  (vec returnVector)                                         ;return vector ColumnFamili name
 )

;;(createKeyspace "base2" accountIdTemp tokenTemp)
;;(deleteKeyspace "base2" accountIdTemp tokenTemp)
;;(createColumnFamily "base2" "tab5" "UTF8Type" accountIdTemp tokenTemp)
;;(deleteColumnFamily "base2" "tab1" accountIdTemp tokenTemp)
(println (getColumnFamiliesName "base2" accountIdTemp tokenTemp))







