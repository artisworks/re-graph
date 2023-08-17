(ns re-graph.interop
  (:require
   [aleph.http :as http]
   [byte-streams]
   [clojure.tools.logging :as log]
   [jsonista.core :as json]
   [manifold.deferred :as d]
   [manifold.stream :as stream]))

(defn reconnect-after [timeout f]
  (future
    (Thread/sleep timeout)
    (f)))

(defn read-message [msg]
  (-> msg
      byte-streams/to-string
      (json/read-value json/keyword-keys-object-mapper)))

(defn read-ws-message [msg]
  (let [data (byte-streams/to-string msg)]
    data))

;; TODO: Read sub-protocols from callbacks
(defn create-ws [url {:keys [on-open on-message on-error on-close] :as callbacks}]
  (log/info "Connecting to " url)
  (try
    (let [conn @(http/websocket-client
                 url
                 {:max-frame-size 9999999
                  :max-frame-payload 9999999
                  :compression? true
                  :sub-protocols "graphql-ws"
                  :heartbeats {:send-after-idle 20000
                               :timeout 10000}})]
      (log/info "Connected to " url)
      (stream/consume (comp on-message read-ws-message) conn)
      (stream/on-closed conn (fn [] (on-close 5001 "closed")))
      (on-open conn))
    (catch Exception e
      ;; TODO: check if this is best logic. Currently if we try to reconnect
      ;; and some thing happens (transient DNS error) then we raise on-error
      ;; and without calling on-close it would not retry again.
      (on-error e)
      (on-close 5001 "Closed"))))

(defn send-ws [conn payload]
  (log/info "Sending ws payload out")
  @(d/timeout! (stream/put! conn payload) 2000))

(defn close-ws [conn]
  (stream/close! conn))

(defn handle-response [success failure data]
  (log/info "Received response " (:status data))
  (success (update data :body read-message)))

(defn handle-error [callback data]
  (log/info "Received response error " (:status data))
  (callback data))

(defn send-http [url request payload on-success on-error]
  (log/info "send-http" {:url url :request request :payload payload})
  (let [req (-> request
                (update :headers assoc  "Content-Type" "application/json" "Accept" "application/json")
                (assoc :body payload))]
    (d/on-realized
     (http/post url req)
     (partial handle-response on-success on-error)
     (partial handle-error on-error))))
