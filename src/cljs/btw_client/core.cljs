(ns btw-client.core
    (:require
        [reagent.core :as reagent]
        [cljs.core.async :refer [put! take! chan close! <! >! sliding-buffer]]
        [cljs-http.client :as http]
        [goog.net.XhrIo :as xhr]
        [cljs.pprint :refer [cl-format]]
        )
    (:require-macros [cljs.core.async.macros :refer [go go-loop alt!]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Vars

(defonce service-url
    "http://localhost:3000")

(defonce debug?
  ^boolean js/goog.DEBUG)

(defonce app-state
  (reagent/atom
   {:text "This is the initial text "
    :hello 10
    :filters {:ch-in (chan) ; channel for adding new filters
              ; the list of filters
              :borough nil
              :intersection1 nil
              :intersection2 nil
              :off-street nil
              :vehicle-type nil
              :factor nil
              :cluster-id nil
              }
    :filtered-result  0 ; This is the actual value that is being filtered with (total accident count, persons injured etc.)
    }))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Page

(defn header-component []
  [:div
   [:h1 "NYC Accident Explorer"]])

;;generic function
(defn prompt-message
    "A prompt that will animate to help the user with a given input"
    [message]
    [:div {:class "my-messages"}
     [:div {:class "prompt message-animation"} [:p message]]])

(defn input-element
    "An input element which updates its value and on focus parameters on change, blur, and focus"
    [id name type value in-focus]
    [:input {:id id
             :name name
             :class "form-control"
             :type type
             :required ""
             :value @value
             :on-change #(reset! value (-> % .-target .-value))
             ;; Below we change the state of in-focus
             :on-focus #(swap! in-focus not)
             :on-blur #(swap! in-focus not)}])

(defn input-and-prompt
    "Creates an input box and a prompt box that appears above the input when the input comes into focus."
    [label-value input-name input-type input-element-arg prompt-element]
    (let [input-focus (atom false)]
        (fn []
            [:div
             [:label label-value]
             (if @input-focus prompt-element [:div])
             [input-element input-name input-name input-type input-element-arg input-focus]])))

(defn email-form [email-address-atom]
    (input-and-prompt "email"
                      "email"
                      "email"
                      email-address-atom
                      [prompt-message "What's your email?"]))

;;
;; Active Filters Component
;;
(defn removable-filter [filter-state filter-type value];  {{{
    [:a {:href "#"
         :on-click (fn [e]
                       ;(prn "Removing filter: " value)
                       (swap! filter-state assoc filter-type nil)
                       false)
         }
     value]);  }}}
(defn active-filters-component [filter-state];  {{{
    (let [borough       (:borough @filter-state)
          intersection1 (:intersection1 @filter-state)
          intersection2 (:intersection2 @filter-state)
          off-street    (:off-street @filter-state)
          vehicle-type  (:vehicle-type @filter-state)
          factor        (:factor @filter-state)
          cluster-key   (:cluster-id @filter-state)]
        [:div#active-filters
         ;[:h5 "Active Filters"
          [:ul
           (if (not (empty? borough))
               [:li [:span "Borough: "] (removable-filter filter-state :borough borough)])
           (if (and (not (empty? intersection1)) (empty? intersection2))
               [:li [:span "Street of "] (removable-filter filter-state :intersection1 intersection1)])
           (if (and (not (empty? intersection1)) (not (empty? intersection2)))
               [:li "Intersection at" (removable-filter filter-state :intersection1 intersection1)
                    " and " (removable-filter filter-state :intersection2 intersection2)])
           (if (not (empty? off-street))
               [:li [:span "Off Street Address: "] (removable-filter filter-state :off-street off-street)])
           (if (not (empty? vehicle-type))
               [:li [:span "Vehicle Type: "] (removable-filter filter-state :vehicle-type vehicle-type)])
           (if (not (empty? factor))
               [:li [:span "Contributing Factor: "] (removable-filter filter-state :factor factor)])
           (if (not (nil? cluster-key))
               [:li [:span "Cluster ID: "] (removable-filter filter-state :cluster-id cluster-key)])
           ]]));  }}}

;;
;; Auto Complete component
;;
(defn add-filter [filter-state item];  {{{
    (let [t (:type item)]
        (cond (= t "intersection")
              ;; we only let setting the other intersection by the related intersection component
              (do (swap! filter-state assoc :intersection1 (:result item))
                  (swap! filter-state assoc :intersection2 nil))
              (= t "borough")
              (swap! filter-state assoc :borough (:result item))
              (= t "off street")
              (swap! filter-state assoc :off-street (:result item))
              (= t "vehicle_type")
              (swap! filter-state assoc :vehicle-type (:result item))
              (= t "factor")
              (swap! filter-state assoc :factor (:result item))
              :else (prn "Unknown type: " t))));  }}}
(defn autocomplete-input [filter-state post-ch default-value state];  {{{
    (if (and (clojure.string/blank? (:value @state))
             (not (:has-focus @state)))
        (swap! state assoc :value default-value))
    [:div#autocomplete-input
     [:input {:type "text"
              :defaultValue default-value
              :value (:value @state) 
              :on-change (fn [e]
                             (swap! state assoc :show-results true)
                             (swap! state assoc :value (-> e .-target .-value))
                             (put! post-ch (:value @state))
                             )
              :on-click (fn [e]
                            (if (= default-value (:value @state))
                                (swap! state assoc :value "")))
              :on-focus (fn [e]
                            (swap! state assoc :has-focus true)
                            ; at this point we gain focus again. If we have a previous query, rerun it
                            (swap! state assoc :show-results true))
              :on-blur (fn [e]
                           (swap! state assoc :has-focus false)
                           (swap! state assoc :show-results false))
              :on-key-press (fn [e]
                                (if (= 13 (.-charCode e))
                                    (do (add-filter filter-state (first (:result @state)))
                                        (swap! state assoc :show-results false)
                                        (swap! state assoc :value "")
                                        (swap! state assoc :result (list)))))
              }]]);  }}}
(defn autocomplete-lister [filter-state state];  {{{
    "List the autocomplete result within this component, but only if we are told to do so."
    (fn []
        [:div {:on-focus #(swap! state assoc :show-results true)}
         (if (:show-results @state)
             [:ul
              (for [item (:result @state)]
                  ^{:key item} [:li
                                [:a {:on-click (fn [e]
                                                   (swap! state assoc :show-results false)
                                                   (swap! state assoc :value "")
                                                   (swap! state assoc :result (list))
                                                   (add-filter filter-state item)
                                                   false)
                                     :href "#"}
                                 (:result item)]
                                 " "
                                 [:span {:class (str "type " (:type item))} (:type item)]
                                 ])])
         ]))
;  }}}
(defn autocomplete-component [filter-state];  {{{
    (let [url           (str service-url "/rpc/autocomplete_all")
          default-value "Filter accidents by typing here..."
          input-state   (reagent/atom {:value ""
                                       :has-focus false
                                       :show-results true
                                       :result (list)})
          post-ch       (chan (sliding-buffer 1))]
        ;; setup logic (once)
        (go-loop []
                 (let [value (<! post-ch)
                       response (<! (http/post url {:json-params {:value value :total 10}}))]
                     (swap! input-state assoc :result (:body response))
                     (recur)))
        (fn []
            [:div#autocomplete
             [autocomplete-input filter-state post-ch default-value input-state]
             [active-filters-component filter-state]
             [autocomplete-lister filter-state input-state]
             ])));  }}}

(defn base-filtered-component [{component-name          :name;  {{{
                                filter-state            :filter-state
                                state                   :state
                                update-state-on-load    :update-state-on-load
                                component-did-update-fn :component-did-update
                                component-did-mount-fn  :component-did-mount
                                component-render-fn     :component-render
                                update?                 :update-condition}]
    "A base component for filtered accidents. When these filters change, these components will
     reload automatically."
    (let [post-ch          (chan (sliding-buffer 1))
          ;; component-did-update function
          component-did-update (or component-did-update-fn (fn [this state]))
          component-did-mount  (or component-did-mount-fn (fn [this]))
          component-render     (or component-render-fn (fn [state filter-state]
                                                           (prn "default render logic for " component-name)))
          ;; if no update condition function has been supplied, use default one
          update-condition (or update? (fn [state filter-state]
                                           (not= (:last-filter-state @state) @filter-state)))]
        ;(prn "setting up " component-name)
        (go-loop []
                 ;(prn "updateing " component-name)
                 (let [[url fs] (<! post-ch)
                       response (<! (http/post url {:json-params {:_borough       (:borough fs)
                                                                  :_intersection1 (:intersection1 fs)
                                                                  :_intersection2 (:intersection2 fs)
                                                                  :_off_street    (:off-street fs)
                                                                  :_vehicle_type  (:vehicle-type fs)
                                                                  :_factor        (:factor fs)
                                                                  :_cluster_key   (:cluster-id fs)}}))]
                     (update-state-on-load state filter-state (:body response))
                     (recur)))
        (reagent/create-class
            {:component-did-update (fn [this]
                                       (component-did-update this state))
             :component-did-mount  component-did-mount
             :reagent-render       (fn []
                                       (if (update-condition state filter-state)
                                           (put! post-ch [(:url @state) @filter-state]))
                                       (component-render state filter-state))})))
;  }}}

;;
;; Season Component
;;
(defn season-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_season_cached_by_filter_accidents?select=year,month,count&order=year,month")]
        (base-filtered-component {:name "season-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :url url
                                                        :result (list)
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                              [:div
                                               [:h2 "Timeline"]
                                               (if (:has-result @state)
                                                   [:ul
                                                    (for [item (:result @state)]
                                                        ^{:key item} [:li (:year item) "-" (:month item) ": " (:count item)])])])})));  }}}

;;
;; Year Component
;;
(defn year-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_year_cached_by_filter_accidents?select=year,count")]
        (base-filtered-component {:name "year-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :url url
                                                        :result (list)
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Year"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              (for [item (:result @state)]
                                                                  ^{:key item} [:li (:year item) ": " (:count item)])])])})));  }}}

;;
;; Month Component
;;
(defn month-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_month_cached_by_filter_accidents?select=month,count")]
        (base-filtered-component {:name "month-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :url url
                                                        :result (list)
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Month"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              (for [item (:result @state)]
                                                                  ^{:key item} [:li (:month item) ": " (:count item)])])]
                                                        )})));  }}}

;;
;; Weekday Component
;;
(defn weekday-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_weekday_cached_by_filter_accidents?select=name,count")]
        (base-filtered-component {:name "weekday-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :url url
                                                        :result (list)
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Weekday"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              (for [item (:result @state)]
                                                                  ^{:key item} [:li (:name item) ": " (:count item)])])]
                                                        )})));  }}}

;;
;; Hour Component
(defn hour-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_hour_cached_by_filter_accidents?select=hour,count")]
        (base-filtered-component {:name "hour-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :url url
                                                        :result (list)
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Hours During the Day"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              (for [item (:result @state)]
                                                                  ^{:key item} [:li (:hour item) ": " (:count item)])])]
                                                        )})));  }}}

;;
;; Intersection Component
;;
(defn intersection-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_intersection_cached_by_filter_accidents?select=name,count&limit=25")]
        (base-filtered-component {:name "intersection-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :url url
                                                        :result (list)
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Related Intersections (Top 25)"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              (for [item (:result @state)]
                                                                  ^{:key item} [:li (:name item) ": " (:count item)])])]
                                                        )})));  }}}

;;
;; Off Street Component
;;
(defn off-street-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_off_street_cached_by_filter_accidents?select=name,count&limit=25")]
        (base-filtered-component {:name "off-street-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :url url
                                                        :result (list)
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        (if (not (empty? (:result @state)))
                                                            [:div
                                                             [:h2 "Off Street Adresses (Top 25)"]
                                                             (if (:has-result @state)
                                                                 [:ul
                                                                  (for [item (:result @state)]
                                                                      ^{:key item} [:li (:name item) ": " (:count item)])])]))})));  }}}

;;
;; Casualty Component
;;
(defn int-comma [n] (cl-format nil "~:d" n))
(defn casualty-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_casualties_cached_by_filter_accidents")]
        (base-filtered-component {:name "casualty-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:count nil
                                                        :total-number-persons-injured nil
                                                        :total-number-persons-killed nil
                                                        :total-number-pedestrians-injured nil
                                                        :total-number-pedestrians-killed nil
                                                        :total-number-cyclist-injured nil
                                                        :total-number-cyclist-killed nil
                                                        :total-number-motorist-injured nil
                                                        :total-number-motorist-killed nil
                                                        :has-result false
                                                        :url url
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (let [x (first response)]
                                                                (reset! state {:count (:count x)
                                                                               :total-number-persons-injured (:total_number_persons_injured x)
                                                                               :total-number-persons-killed (:total_number_persons_killed x)
                                                                               :total-number-pedestrians-injured (:total_number_pedestrians_injured x)
                                                                               :total-number-pedestrians-killed (:total_number_pedestrians_killed x)
                                                                               :total-number-cyclist-injured (:total_number_cyclist_injured x)
                                                                               :total-number-cyclist-killed (:total_number_cyclist_killed x)
                                                                               :total-number-motorist-injured (:total_number_motorist_injured x)
                                                                               :total-number-motorist-killed (:total_number_motorist_killed x)
                                                                               :url url
                                                                               :has-result true
                                                                               :last-filter-state @filter-state})
                                                                ; TODO this should be based on the user's selection later
                                                                (swap! app-state assoc :filtered-result (:count x))
                                                                ))
                                  :component-render (fn [state filter-state]
                                                        [:div#casualty-component
                                                         ;[:h2 "Casualties"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              [:li.total
                                                               [:div.value (int-comma (:count @state))]
                                                               [:div.label "Accidents In Total"]]
                                                              [:li.persons
                                                               [:div.value (int-comma (:total-number-persons-injured @state))]
                                                               [:div.label "Persons Injured"]]
                                                              [:li.persons
                                                               [:div.value (int-comma (:total-number-persons-killed @state))]
                                                               [:div.label "Persons Killed"]]
                                                              [:li.motorist
                                                               [:div.value (int-comma (:total-number-motorist-injured @state))]
                                                               [:div.label "Motorist Injured"]
                                                               ]
                                                              [:li.motorist
                                                               [:div.value (int-comma (:total-number-motorist-killed @state))]
                                                               [:div.label "Motorist Killed"]
                                                               ]
                                                              [:li.cyclist
                                                               [:div.value (int-comma (:total-number-cyclist-injured @state))]
                                                               [:div.label "Cyclist Injured"]
                                                               ]
                                                              [:li.cyclist
                                                               [:div.value (int-comma (:total-number-cyclist-killed @state))]
                                                               [:div.label "Cyclist Killed"]
                                                               ]
                                                              [:li.pedestrians
                                                               [:div.value (int-comma (:total-number-pedestrians-injured @state))]
                                                               [:div.label "Pedestrians Injured"]
                                                               ]
                                                              [:li.pedestrians
                                                               [:div.value (int-comma (:total-number-pedestrians-killed @state))]
                                                               [:div.label "Pedestrians Killed"]
                                                               ]
                                                              ])]
                                                        )})));  }}}

;;
;; Borough Component
;;
(defn- length [[from to]]
    (- to from))
(defn- domain-to-range
    "Converts a value from domain to range. In other words, maps the current state of the world
    to what the animation's value needs to be."
    [[domain-from domain-to :as domain] [range-from range-to :as range] domain-value]
    (let [domain-len (length domain)
          range-len (length range)
          domain-offset (- domain-value domain-from)
          ratio (/ domain-offset domain-len)
          range-offset (* ratio range-len)]
        (+ range-offset range-from)))
(defn borough-component [filter-state];  {{{
    (let [dom-node (reagent/atom nil)
          url (str service-url "/rpc/stats_borough_cached_by_filter_accidents?select=name,count")]
        (base-filtered-component {:name "borough-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :result (list)
                                                        :url url
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-did-update
                                  (fn [this state]
                                      (let [canvas (.-nextSibling (.-firstChild @dom-node))
                                            ctx    (.getContext canvas "2d")
                                            w      (.-clientWidth canvas)
                                            h      (.-clientHeight canvas)
                                            max-value (:filtered-result @app-state)
                                            max-n     (count (:result @state))
                                            single-h  (/ h max-n)]
                                          (.clearRect ctx 0 0 w h)
                                          (prn "domain-to-range" (domain-to-range [0 max-value] [0 500] 3000))
                                          (prn "max-value: " max-value ", max-n: " max-n ", single-height " single-h)
                                          ;(doseq [index (range 0 w 10)]
                                              ;(.moveTo ctx (+ index 0.5) 0)
                                              ;(.lineTo ctx (+ index 0.5) h))
                                          (doseq [index (range 0 h single-h)]
                                              (.moveTo ctx 0 (+ index 0.5))
                                              (.lineTo ctx w (+ index 0.5)))
                                          (.moveTo ctx 0 (- h 0.5))
                                          (.lineTo ctx w (- h 0.5))
                                          (set! (.-strokeStyle ctx) "#444")
                                          (.stroke ctx)
                                          (set! (.-fillStyle ctx) "darkblue")
                                          (set! (.-strokeStyle ctx) "#fff")
                                          (doseq [[value index] (map vector (:result @state) (range))]
                                              (.fillRect ctx 0 (* index single-h) (domain-to-range [0 max-value] [0 w] (:count value)) single-h)
                                              (.strokeRect ctx 0 (+ (* index single-h) 0.5) (domain-to-range [0 max-value] [0 w] (:count value)) (+ single-h 0.5))
                                              )
                                          ;(.fillRect ctx 0 (* 0 single-h) (domain-to-range [0 max-value] [0 w] (:count (nth (:result @state) 0))) single-h)
                                          ;(.fillRect ctx 0 (* 1 single-h) (domain-to-range [0 max-value] [0 w] (:count (nth (:result @state) 1))) single-h)
                                          ;(.fillRect ctx 0 (* 2 single-h) (domain-to-range [0 max-value] [0 w] (:count (nth (:result @state) 2))) single-h)
                                      ))

                                  :component-did-mount
                                  (fn [this]
                                      (reset! dom-node (reagent/dom-node this)))

                                  :component-render
                                  (fn [state filter-state]
                                      [:div#boroughs.with-canvas
                                       [:h2 "Boroughs"]
                                       [:canvas (if (not (:has-result @state)) {:style {:display "none"}})
                                        (if-let [node @dom-node]
                                            (do (prn "node is there!"
                                                     {:width (.-clientWidth node)
                                                      :height (.-clientHeight node)})))]
                                      (if (:has-result @state)
                                          [:ul
                                           (for [item (:result @state)]
                                               ^{:key item} [:li (:name item) ": " (:count item)])])
                                      ])})));  }}}

;;
;; Factor Component
;;
(defn factor-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_factors_cached_by_filter_accidents?select=name,count")]
        (base-filtered-component {:name "factor-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :result (list)
                                                        :url url
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Factors"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              (for [item (:result @state)]
                                                                  ^{:key item} [:li (:name item) ": " (:count item)])])]
                                                        )})));  }}}

;;
;; Vehicle Type Component
;;
(defn vehicle-type-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_vehicle_types_cached_by_filter_accidents?select=name,count")]
        (base-filtered-component {:name "vehicle-type-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :result (list)
                                                        :url url
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Vehicle Types"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              (for [item (:result @state)]
                                                                  ^{:key item} [:li (:name item) ": " (:count item)])])]
                                                        )})));  }}}

(defn cluster-component [filter-state];  {{{
    (let [url (str service-url "/rpc/stats_cluster_cached_by_filter_accidents?cluster_size=eq.25m&limit=10")]
        (base-filtered-component {:name "cluster-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :result nil
                                                        :url (str url "&order=accident_count.desc")
                                                        :order-by "&order=accident_count.desc"
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (prn "setting url: " (str url (:order-by @state)))
                                                            (reset! state {:result response
                                                                           :url (str url (:order-by @state))
                                                                           :last-filter-state @filter-state
                                                                           :order-by (:order-by @state)
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Clusters (Top 10)"]
                                                         [:select {:value (:order-by @state) :on-change (fn [e]
                                                                                                            (prn "setting order-by to: " (-> e .-target .-value))
                                                                                                            (swap! state assoc :order-by (-> e .-target .-value)))}
                                                          [:option {:value "&order=accident_count.desc"}"by accident count"]
                                                          [:option {:value "&order=total_number_persons_injured.desc"}"by persons injured"]
                                                          [:option {:value "&order=total_number_persons_killed.desc"}"by persons killed"]
                                                          [:option {:value "&order=total_number_cyclist_injured.desc"}"by cyclist injured"]
                                                          [:option {:value "&order=total_number_cyclist_killed.desc"}"by cyclist killed"]
                                                          [:option {:value "&order=total_number_pedestrians_injured.desc"}"by pedestrians injured"]
                                                          [:option {:value "&order=total_number_pedestrians_killed.desc"}"by pedestrians killed"]
                                                          [:option {:value "&order=total_number_motorist_injured.desc"}"by motorist injured"]
                                                          [:option {:value "&order=total_number_motorist_killed.desc"}"by motorist killed"]
                                                          ]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              ;{:cluster_key 1500, :cluster_number_persons_injured 252, :accident_count 275, :cluster_number_cyclist_killed 0, :total_number_cyclist_killed 0, :total_number_motorist_injured 4, :cluster_size "40m", :cluster_number_cyclist_injured 34, :total_number_pedestrians_injured 5, :total_number_persons_injured 9, :total_number_motorist_killed 0, :cluster_number_persons_killed 1, :cluster_number_motorist_injured 88, :cluster_number_motorist_killed 0, :total_number_persons_killed 0, :cluster_number_pedestrians_injured 130, :cluster_number_pedestrians_killed 1, :total_number_cyclist_injured 0, :cluster_count 2257}
                                                              (for [item (:result @state)]
                                                                  ^{:key item} [:li [:a {:href "#" :on-click (fn [e]
                                                                                                                 (swap! filter-state assoc :cluster-id (:cluster_key item))
                                                                                                                 false)}
                                                                                     [:ul
                                                                                      [:li "total: " (:accident_count item) "/" (:cluster_count item)]
                                                                                      [:li "persons injured: " (:total_number_persons_injured item) "/" (:cluster_number_persons_injured item)]
                                                                                      [:li "persons killed " (:total_number_persons_killed item) "/" (:cluster_number_persons_killed item)]
                                                                                      [:li "pedestrians injured " (:total_number_pedestrians_injured item) "/" (:cluster_number_pedestrians_injured item)]
                                                                                      [:li "pedestrians killed " (:total_number_pedestrians_killed item) "/" (:cluster_number_pedestrians_killed item)]
                                                                                      [:li "cyclist injured " (:total_number_cyclist_injured item) "/" (:cluster_number_cyclist_injured item)]
                                                                                      [:li "cyclist killed " (:total_number_cyclist_killed item) "/" (:cluster_number_cyclist_killed item)]
                                                                                      [:li "motorist injured " (:total_number_motorist_injured item) "/" (:cluster_number_motorist_injured item)]
                                                                                      [:li "motorist killed " (:total_number_motorist_killed item) "/" (:cluster_number_motorist_killed item)]
                                                                                      ]]])])]
                                                        )})));  }}}

(defn setup-filter-controller [filter-state]
    "A controller responsible for actually adding new filters and triggering
    dependent components after filters have changed."
    (go-loop []
             (let [[ftext ftype] (<! (:ch-in @filter-state))]
                 (cond (= ftype "intersection")
                       (prn "handle intersection!")
                       (= ftype "off street")
                       (prn "handle off street")
                       (= ftype "factor")
                       (prn "handle factor")
                       (= ftype "borough")
                       (prn "handle borough")
                       (= ftype "vehicle_type")
                       (prn "handle vehicle type")
                       :else (prn "Unknown filter: " ftype))
                 (prn {:text ftext :type ftype} " in controller!")
                 (recur))))

(defn body-component [state]
    (let [filter-state (reagent/cursor state [:filters])]
        [:div
         [autocomplete-component filter-state]
         [casualty-component filter-state]
         [borough-component filter-state]
         [season-component filter-state]
         [year-component filter-state]
         [cluster-component filter-state]
         [month-component filter-state]
         [weekday-component filter-state]
         [hour-component filter-state]
         [factor-component filter-state]
         [vehicle-type-component filter-state]
         [intersection-component filter-state]
         [off-street-component filter-state]
         ]))

(defn footer-component []
  [:div#footer
   [:div#left-column
    [:p "this is the " [:strong "footer"] " text, render something here"]]
   [:div#right-column
    [:p "This is the footer"]]])

(defn page [state]
    (setup-filter-controller (reagent/cursor state [:filters]))
    (fn []
        [:div
         [header-component]
         [body-component state]
         [footer-component]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Initialize App

(defn dev-setup []
  (when debug?
    (enable-console-print!)
    (println "dev mode")
    ))

(defn reload []
  ; TODO how to integrate these channels inside the components?
  (reagent/render [page app-state]
                  (.getElementById js/document "app")))

(defn ^:export main []
  (dev-setup)
  (reload))
