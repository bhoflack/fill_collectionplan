(ns fill-collectionplan.core  
  (:require [clojure.java.jdbc :as sql])
  (:import [groovy.util Eval]))

(defn field-information-for-collectionplan
  "Extract the field information for a collectionplan"
  [ds collectionplan]
  (sql/with-connection ds
    (sql/with-query-results fields
      ["select qpc.prompt, qpc2.prompt as update_prompt, qpca.message, qpca.assign_type, qpcat.LOW_VALUE_LOOKUP, qpcat.HIGH_VALUE_LOOKUP, qpcat.LOW_VALUE_OTHER, qpcat.HIGH_VALUE_OTHER  
  from QA_PLANS qp
    inner join QA_PLAN_CHARS qpc
    on qp.plan_id = qpc.plan_id
    inner join QA_PLAN_CHAR_ACTION_TRIGGERS qpcat
    on (qpc.char_id = qpcat.char_id
        and qp.plan_id = qpcat.plan_id)
    inner join QA_PLAN_CHAR_ACTIONS qpca
    on qpca.PLAN_CHAR_ACTION_TRIGGER_ID = qpcat.PLAN_CHAR_ACTION_TRIGGER_ID
    inner join QA_PLAN_CHARS qpc2
    on (qpc2.char_id = qpca.assigned_char_id
        and qpc2.plan_id = qp.plan_id)
  where name = ?
    and qpc.enabled_flag = 1
  order by qpc.PROMPT_SEQUENCE, trigger_sequence" collectionplan]
      (doall fields))))

(defn evaluate-script
  "Evaluate a script by replacing variables with the value of the context"
  [context script wrap-string]
  (let [variables (re-seq #"&(\w+)" script)
        script1 (atom script)]
    (doall
     (for [[token var] variables]
       (let [v (get context var)]
         (swap! script1 clojure.string/replace token
                (if wrap-string (format "'%s'" v) v)))))
    @script1))

(defn execute-sql-script
  [context ds script]
  (let [query (evaluate-script context script true)]
    (sql/with-connection ds
      (sql/with-query-results rs
        [query]
        (.toString (first (vals (first rs))))))))

(defn execute-script
  [context script]
  (let [script1 (evaluate-script context script false)
        script2 (clojure.string/replace script1 #"0(\d+)" "$1")]
    (Eval/me script2)))

(defn execute-actions
  "Execute all actions for the collectionplan"
  [ds name values]
  (let [fields (field-information-for-collectionplan ds name)
        values1 (atom values)]
    (doall
     (for [field fields]
       (do
         (let [low-value-other (:low_value_other field)
               prompt (:prompt field)
               assign-type (:assign_type field)
               update-prompt (:update_prompt field)
               message (:message field)]
           
         (if (or (nil? low-value-other)
                 (= low-value-other (get @values1 prompt)))
           (swap! values1 assoc update-prompt
                  (if (= assign-type "S")
                    (execute-sql-script @values1 ds (clojure.string/replace message ";" ""))
                    (execute-script @values1 message))))))))
    @values1))
        
        


      
