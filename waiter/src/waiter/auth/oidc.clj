;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns waiter.auth.oidc
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [digest]
            [waiter.auth.authentication :as auth]
            [waiter.auth.jwt :as jwt]
            [waiter.cookie-support :as cookie-support]
            [waiter.status-codes :refer :all]
            [waiter.util.http-utils :as hu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (java.net URI)
           (java.security SecureRandom)))

(def ^:const challenge-cookie-duration-secs 60)

(def ^:const code-verifier-length 128)

(def ^:const content-security-policy-value "default-src 'none'; frame-ancestors 'none'")

(def ^:const oidc-challenge-cookie-prefix "x-waiter-oidc-challenge-")

(def ^:const oidc-callback-uri "/oidc/v1/callback")

(defn create-code-identifier
  "Returns a string generated using the code verifier to use as an identifier for the OIDC workflow."
  [code-verifier]
  (digest/md5 code-verifier))

;; code_verifier = high-entropy cryptographic random STRING using the unreserved characters
;;   [A-Z] / [a-z] / [0-9] / "-" / "." / "_" / "~"
;; from Section 2.3 of [RFC3986], with a minimum length of 43 characters and a maximum length of 128 characters.
;; https://tools.ietf.org/html/rfc7636#section-4.1
(let [allowed-chars "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
      allowed-chars-count (count allowed-chars)
      secure-rng (SecureRandom.)]
  (defn create-code-verifier
    "Creates a randomly generated string of length code-verifier-length from the list of allowed characters."
    []
    (apply str (repeatedly code-verifier-length #(nth allowed-chars (.nextInt secure-rng allowed-chars-count))))))

(defn create-state-code
  "Creates an encoded string of the input state map."
  [state-map password]
  (utils/map->base-64-string state-map password))

(defn parse-state-code
  "Parses the encoded string into the state map."
  [state-str password]
  (utils/base-64-string->map state-str password))

(defn validate-oidc-callback-request
  [password {:keys [headers] :as request}]
  (let [{:strs [code state]} (-> request ru/query-params-request :query-params)
        bad-request-map {:log-level :info
                         :query-param-state state
                         :status http-400-bad-request}]
    (when (str/blank? code)
      (throw (ex-info "Query parameter code is missing" bad-request-map)))
    (when (str/blank? state)
      (throw (ex-info "Query parameter state is missing" bad-request-map)))
    (let [{:keys [identifier] :as state-map}
          (try
            (parse-state-code state password)
            (catch Throwable throwable
              (throw (ex-info "Unable to parse state"
                              bad-request-map throwable))))]
      (when-not (and (map? state-map)
                     (string? (get state-map :redirect-uri))
                     (string? identifier)
                     (not (str/blank? identifier)))
        (throw (ex-info "The state query parameter is invalid" bad-request-map)))
      (let [oidc-challenge-cookie (str oidc-challenge-cookie-prefix identifier)
            challenge-cookie (some-> headers
                               (get "cookie")
                               (cookie-support/cookie-value oidc-challenge-cookie))]
        (when (str/blank? challenge-cookie)
          (throw (ex-info "No challenge cookie set"
                          (assoc bad-request-map :cookie-name oidc-challenge-cookie))))
        (let [decoded-value (cookie-support/decode-cookie challenge-cookie password)]
          (when-not (map? decoded-value)
            (throw (ex-info "Decoded challenge cookie is invalid" bad-request-map)))
          (let [{:keys [code-verifier expiry-time]} decoded-value]
            (when-not (integer? expiry-time)
              (throw (ex-info "The challenge cookie has invalid format" bad-request-map)))
            (when-not (->> expiry-time (tc/from-long) (t/before? (t/now)))
              (throw (ex-info "The challenge cookie has expired" bad-request-map)))
            (when (or (not (string? code-verifier))
                      (str/blank? code-verifier))
              (throw (ex-info "No challenge code available from cookie" bad-request-map)))
            {:code code
             :code-verifier code-verifier
             :state-map state-map}))))))

(defn attach-threat-remediation-headers
  "Threat remediation by
   - avoiding storing responses in the browser cache,
   - limiting locations from which resource types may be loaded."
  [headers]
  (assoc headers
    ;; prevent the request and response from being stored by the cache
    "cache-control" "no-store"
    "content-security-policy" content-security-policy-value))

(defn oidc-callback-request-handler
  "Handler for the OIDC callback that will retrieve and validate the access token.
   Upon successful validation, the handler responds with a 302 redirect to the original url
   (preserved in the state) before the auth flow was triggered.
   Unsuccessful authentication returns either a 400 Bad request, the downstream auth server
   response, or a 401 unauthorized with appropriate details."
  [{:keys [jwt-auth-server jwt-validator password] :as oidc-authenticator} request]
  (if (nil? oidc-authenticator)
    (utils/exception->response
      (throw (ex-info "OIDC authentication disabled" {:status http-501-not-implemented}))
      request)
    (let [{:keys [code-verifier code state-map]} (validate-oidc-callback-request password request)]
      (async/go
        (let [access-token-ch (jwt/request-access-token jwt-auth-server request oidc-callback-uri code code-verifier)
              body-or-throwable (async/<! access-token-ch)]
          (try
            (if (instance? Throwable body-or-throwable)
              (throw body-or-throwable)
              (let [access-token body-or-throwable
                    _ (log/info "successfully retrieved access token" (utils/truncate access-token 30))
                    key-id->jwk (jwt/get-key-id->jwk jwt-auth-server)
                    result-map-or-throwable (jwt/extract-claims jwt-validator key-id->jwk request access-token)
                    _ (when (instance? Throwable result-map-or-throwable)
                        (throw result-map-or-throwable))
                    {:keys [expiry-time subject]} result-map-or-throwable
                    _ (log/info "authenticated subject is" subject)
                    auth-params-map (auth/build-auth-params-map :oidc subject {:jwt-access-token access-token})
                    auth-cookie-age-in-seconds (- expiry-time (jwt/current-time-secs))]
                (auth/handle-request-auth
                  (constantly
                    (let [{:keys [identifier redirect-uri]} state-map
                          oidc-challenge-cookie (str oidc-challenge-cookie-prefix identifier)]
                      (-> {:headers (attach-threat-remediation-headers {"location" redirect-uri})
                           :status http-302-moved-temporarily}
                        (cookie-support/add-encoded-cookie password oidc-challenge-cookie "" 0)
                        (utils/attach-waiter-source))))
                  request auth-params-map password auth-cookie-age-in-seconds)))
            (catch Throwable throwable
              (utils/exception->response
                (ex-info "Error in retrieving access token"
                         (-> (ex-data throwable)
                           (utils/assoc-if-absent :log-level :info)
                           (utils/assoc-if-absent :status http-401-unauthorized))
                         throwable)
                request))))))))

(defn trigger-authorize-redirect
  "Triggers a 302 temporary redirect response to the authorize endpoint."
  [jwt-auth-server password {:keys [query-string request-method uri] :as request} response]
  (let [request-host (utils/request->host request)
        request-scheme (utils/request->scheme request)
        make-redirect-uri (fn make-oidc-redirect-uri [transform-host]
                            (str "https://" (transform-host request-host) uri
                                 (when query-string (str "?" query-string))))]
    (if (= :https request-scheme)
      (let [code-verifier (create-code-verifier)
            cookie-identifier (create-code-identifier code-verifier)
            state-data {:identifier cookie-identifier
                        :redirect-uri (make-redirect-uri identity)}
            state-code (create-state-code state-data password)
            authorize-uri (jwt/retrieve-authorize-url
                            jwt-auth-server request oidc-callback-uri code-verifier state-code)
            expiry-time (-> (t/now)
                          (t/plus (t/seconds challenge-cookie-duration-secs))
                          (tc/to-long))
            oidc-challenge-cookie (str oidc-challenge-cookie-prefix cookie-identifier)
            challenge-cookie-value {:code-verifier code-verifier
                                    :expiry-time expiry-time}]
        (-> response
          (assoc :status http-302-moved-temporarily)
          (update :headers assoc "location" authorize-uri)
          (update :headers attach-threat-remediation-headers)
          (cookie-support/add-encoded-cookie
            password oidc-challenge-cookie challenge-cookie-value challenge-cookie-duration-secs)))
      ;; trigger SSL redirect to the same page since OIDC auth works only for https requests
      (let [redirect-uri (make-redirect-uri utils/authority->host)]
        (-> response
          (assoc :status (if (= request-method :get) http-302-moved-temporarily http-307-temporary-redirect))
          (update :headers assoc "location" redirect-uri))))))

(defn make-oidc-auth-response-updater
  "Returns a response updater that rewrites 401 waiter responses to 302 redirects."
  [jwt-auth-server password request]
  (fn update-oidc-auth-response [{:keys [status] :as response}]
    (if (and (= status http-401-unauthorized)
             (utils/waiter-generated-response? response))
      ;; issue 302 redirect
      (trigger-authorize-redirect jwt-auth-server password request response)
      ;; non-401 response, avoid authentication challenge
      response)))

(defn oidc-enabled-on-service?
  "Returns true if OIDC auth is enabled for the service."
  [allow-oidc-auth-api? allow-oidc-auth-services? {:keys [waiter-api-call?] :as request}]
  (let [use-oidc-auth-env (get-in request [:waiter-discovery :service-description-template "env" "USE_OIDC_AUTH"])]
    (or
      ;; service requests will enable OIDC auth based on env variable or when absent, allow-oidc-auth-services?
      (and (not waiter-api-call?)
           (if (some? use-oidc-auth-env) (= "true" use-oidc-auth-env) allow-oidc-auth-services?))
      ;; waiter api requests will enable OIDC auth based on allow-oidc-auth-api?
      (and waiter-api-call? allow-oidc-auth-api?))))

;; Accept-Redirect request header "yes" means the user-agent will follow redirects.
;; Accept-Redirect-Auth request header indicates which authorities the user-agent is willing to redirect to and authenticate at.
;; https://tools.ietf.org/id/draft-williams-http-accept-auth-and-redirect-02.html#rfc.section.2
(def ^:const accept-redirect-header-name "accept-redirect")
(def ^:const accept-redirect-auth-header-name "accept-redirect-auth")

(defn supports-redirect?
  "Returns true when:
   - either the request is deemed to have come from a browser
   - or the accept-redirect=yes request header is present in the request."
  [oidc-authority request]
  (or (hu/browser-request? request)
      (and (= "yes" (get-in request [:headers accept-redirect-header-name]))
           (let [accept-redirect-auth (get-in request [:headers accept-redirect-auth-header-name])]
             (or (str/blank? accept-redirect-auth)
                 (= "." accept-redirect-auth)
                 (some #(= oidc-authority %) (str/split accept-redirect-auth #" ")))))))

(defn too-many-oidc-challenge-cookies?
  "Returns true if the request already contains too many OIDC challenge cookies."
  [request num-allowed]
  (let [cookie-header (get-in request [:headers "cookie"])
        request-cookies (cond->> cookie-header
                          (not (string? cookie-header)) (str/join ";"))
        num-challenge-cookies (count
                                (filter #(str/starts-with? % oidc-challenge-cookie-prefix)
                                        (str/split (str request-cookies) #";")))]
    (log/info "request has" num-challenge-cookies "oidc challenge cookies")
    (> num-challenge-cookies num-allowed)))

(defn wrap-auth-handler
  "Wraps the request handler with a handler to trigger OIDC+PKCE authentication."
  [{:keys [allow-oidc-auth-api? allow-oidc-auth-services? jwt-auth-server oidc-authorize-uri
           oidc-num-challenge-cookies-allowed-in-request password]}
   request-handler]
  (let [oidc-authority (-> oidc-authorize-uri (URI.) (.getAuthority))]
    (fn oidc-auth-handler [request]
      (cond
        (or (auth/request-authenticated? request)
            (not (oidc-enabled-on-service? allow-oidc-auth-api? allow-oidc-auth-services? request))
            ;; OIDC auth is no-op when request cannot be redirected
            (not (supports-redirect? oidc-authority request))
            ;; OIDC auth is avoided if client already has too many challenge cookies
            (too-many-oidc-challenge-cookies? request oidc-num-challenge-cookies-allowed-in-request))
        (request-handler request)

        :else
        (ru/update-response
          (request-handler request)
          (make-oidc-auth-response-updater jwt-auth-server password request))))))

(defrecord OidcAuthenticator [allow-oidc-auth-api? allow-oidc-auth-services? oidc-authorize-uri
                              jwt-auth-server jwt-validator oidc-num-challenge-cookies-allowed-in-request password])

(defn create-oidc-authenticator
  "Factory function for creating OIDC authenticator middleware"
  [jwt-auth-server jwt-validator
   {:keys [allow-oidc-auth-api? allow-oidc-auth-services? oidc-authorize-uri
           oidc-num-challenge-cookies-allowed-in-request password]
    :or {allow-oidc-auth-api? false
         allow-oidc-auth-services? false
         oidc-num-challenge-cookies-allowed-in-request 20}}]
  {:pre [(satisfies? jwt/AuthServer jwt-auth-server)
         (some? jwt-validator)
         (boolean? allow-oidc-auth-api?)
         (boolean? allow-oidc-auth-services?)
         (integer? oidc-num-challenge-cookies-allowed-in-request)
         (not (str/blank? oidc-authorize-uri))
         (not-empty password)]}
  (->OidcAuthenticator allow-oidc-auth-api? allow-oidc-auth-services? oidc-authorize-uri
                       jwt-auth-server jwt-validator oidc-num-challenge-cookies-allowed-in-request password))