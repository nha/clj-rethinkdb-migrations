(ns joplin.rethinkdb.database
  (:require [clj-time.core :as t]
            [joplin.core :refer :all]
            [rethinkdb.query :as r]
            [ragtime.protocols :refer [DataStore]]))

(defn ensure-migration-schema [conn]
  (when-not (some #{"migrations"} (r/run (r/table-list) conn))
    (r/run (r/table-create "migrations") conn)))

;;; ========================================================================
;;; Ragtime interface
;;; ========================================================================

(defprotocol Connection
  (connect [this]))

(defrecord RethinkDBDatabase [host auth-key db]
  Connection
  (connect [db]
    (let [args (flatten (vec (merge {:db "ragtime"} db)))]
      (apply r/connect args)))

  DataStore
  (add-migration-id [db id]
    (with-open [conn (connect db)]
      (ensure-migration-schema conn)
      (-> (r/table "migrations")
          (r/insert {:id id :created_at (t/now)})
          (r/run conn))))
  (remove-migration-id [db id]
    (with-open [conn (connect db)]
      (ensure-migration-schema conn)
      (-> (r/table "migrations")
          (r/get id)
          r/delete
          (r/run conn))))
  (applied-migration-ids [db]
    (with-open [conn (connect db)]
      (ensure-migration-schema conn)
      (map :id (-> (r/table "migrations")
                   (r/order-by :created_at)
                   (r/run conn))))))

(defn ^:private ->RethinkDBDatabase [target]
  (map->RethinkDBDatabase (:db target)))

;;; ========================================================================
;;; Joplin interface
;;; ========================================================================

(defmethod migrate-db :rethinkdb [target & args]
  (apply do-migrate (get-migrations (:migrator target))
         (->RethinkDBDatabase target) args))

(defmethod rollback-db :rethinkdb [target amount-or-id & args]
  (apply do-rollback (get-migrations (:migrator target))
         (->RethinkDBDatabase target) amount-or-id args))

(defmethod seed-db :rethinkdb [target & args]
  (apply do-seed-fn (get-migrations (:migrator target))
         (->RethinkDBDatabase target) target args))

(defmethod pending-migrations :rethinkdb [target & args]
  (do-pending-migrations (->RethinkDBDatabase target)
                         (get-migrations (:migrator target))))

(defmethod create-migration :rethinkdb [target id & args]
  (do-create-migration target id "joplin.rethinkdb.database"))
