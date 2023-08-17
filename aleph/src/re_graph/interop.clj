(ns re-graph.interop
  (:require
   [aleph.http :as http]
   [byte-streams]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [jsonista.core :as json]
   [manifold.deferred :as d]
   [manifold.stream :as stream]))

(defn- read-message [msg]
  (-> msg
      byte-streams/to-string
      (json/read-value json/keyword-keys-object-mapper)))

(defn- read-ws-message [msg]
  (let [data (byte-streams/to-string msg)]
    data))

(defn- handle-response [success failure data]
  (let [msg (try (read-message (:body data))
                 (catch Exception e
                   (log/error "Error while reading message " e)
                   (failure e)
                   ::read-message-error))]
    (when (not= ::read-message-error msg)
      (success (assoc data :body msg)))))

(defn- handle-error [callback data]
  (log/error "Received response error " (:status data))
  (callback data))

;; ──────────────────────────────────── API ───────────────────────────────────

(defn create-ws [url {:keys [on-open
                             on-message
                             on-close
                             on-error
                             subprotocols]}]
  (log/info "Connecting to " url)
  (try
    (let [conn @(http/websocket-client
                 url
                 {:max-frame-size 9999999
                  :max-frame-payload 9999999
                  :compression? true
                  :sub-protocols (str/join "," subprotocols)
                  :heartbeats {:send-after-idle 20000
                               :timeout 10000}})]
      (log/info "Connected to " url)
      (stream/consume (comp on-message read-ws-message) conn)
      (stream/on-closed conn (fn [] (on-close)))
      (on-open conn))
    (catch Exception e
      (on-error e)
      (on-close))))

(defn send-ws [conn payload]
  @(d/timeout! (stream/put! conn payload) 2000))

(defn close-ws [conn]
  (stream/close! conn))

(defn send-http [url request payload on-success on-error]
  (let [req (-> request
                (update :headers assoc  "Content-Type" "application/json" "Accept" "application/json")
                (assoc :body payload))]
    (d/on-realized
     (http/post url req)
     (partial handle-response on-success on-error)
     (partial handle-error on-error))))
