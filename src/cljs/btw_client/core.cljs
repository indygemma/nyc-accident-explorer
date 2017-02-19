(ns btw-client.core
    (:require
        [reagent.core :as reagent]
        [cljs.core.async :refer [put! take! chan close! <! >! sliding-buffer]]
        [cljs-http.client :as http]
        [goog.net.XhrIo :as xhr])
    (:require-macros [cljs.core.async.macros :refer [go go-loop alt!]]))

(defn log [s]
  (.log js/console (str s)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Vars

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

(defn removable-filter [filter-state filter-type value]
    [:a {:href "#"
         :on-click (fn [e]
                       (prn "Removing filter: " value)
                       (swap! filter-state assoc filter-type nil)
                       false)
         }
     value])

(defn active-filters-component [filter-state]
    (let [borough       (:borough @filter-state)
          intersection1 (:intersection1 @filter-state)
          intersection2 (:intersection2 @filter-state)
          off-street    (:off-street @filter-state)
          vehicle-type  (:vehicle-type @filter-state)
          factor        (:factor @filter-state)]
        [:div
         [:h5 "Active Filters"
          [:ul
           [:li [:span "Borough: "] (if (empty? borough) "None"
                                        (removable-filter filter-state :borough borough))]
           [:li [:span "Street Intersection (1): "] (if (empty? intersection1) "None"
                                                        (removable-filter filter-state :intersection1 intersection1))]
           [:li [:span "Street Intersection (2): "] (if (empty? intersection2) "None"
                                                        (removable-filter filter-state :intersection2 intersection2))]
           [:li [:span "Off Street Address: "] (if (empty? off-street) "None"
                                                   (removable-filter filter-state :off-street off-street))]
           [:li [:span "Vehicle Type: "] (if (empty? vehicle-type) "None"
                                             (removable-filter filter-state :vehicle-type vehicle-type))]
           [:li [:span "Contributing Factor: "] (if (empty? factor) "None"
                                                    (removable-filter filter-state :factor factor))]
           ]]]))

;;
;; Auto Complete component
;;
(defn autocomplete-input [post-ch default-value state];  {{{
    (if (and (clojure.string/blank? (:value @state))
             (not (:has-focus @state)))
        (swap! state assoc :value default-value))
    [:div#autocomplete-input
     [:input {:type "text"
              :defaultValue default-value
              :value (:value @state) 
              :on-change (fn [e]
                             (swap! state assoc :value (-> e .-target .-value))
                             (put! post-ch (:value @state))
                             )
              :on-click (fn [e]
                            (if (= default-value (:value @state))
                                (swap! state assoc :value "")))
              :on-focus (fn [e]
                            (swap! state assoc  :has-focus true)
                            ; at this point we gain focus again. If we have a previous query, rerun it
                            (swap! state assoc :show-results true))
              :on-blur (fn [e]
                           (prn "ac input losing focus...")
                           (swap! state assoc :has-focus false)
                           (swap! state assoc :show-results false))
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
                                                   (prn "click!" item)
                                                   (swap! state assoc :show-results false)
                                                   (swap! state assoc :value "")
                                                   (swap! state assoc :result (list))
                                                   (prn "changing filter state")
                                                   (let [t (:type item)]
                                                       (cond (= t "intersection")
                                                             ;; we only let setting the other intersection by the related intersection component
                                                             (swap! filter-state assoc :intersection1 (:result item))
                                                             (= t "borough")
                                                             (swap! filter-state assoc :borough (:result item))
                                                             (= t "off street")
                                                             (swap! filter-state assoc :off-street (:result item))
                                                             (= t "vehicle_type")
                                                             (swap! filter-state assoc :vehicle-type (:result item))
                                                             (= t "factor")
                                                             (swap! filter-state assoc :factor (:result item))
                                                             :else (prn "Unknown type: " t)))
                                                   ;(put! @filter-ch [(:result item) (:type item)])
                                                   false)
                                     :href "#"}
                                 (:result item)]
                                 " "
                                 [:span {:class (str "type " (:type item))} (:type item)]
                                 ])])
         ]))
;  }}}
(defn autocomplete-component [filter-state];  {{{
    (let [url           "http://10.0.0.55:3000/rpc/autocomplete_all"
          default-value "Filter accidents by typing here..."
          input-state   (reagent/atom {:value ""
                                       :has-focus false
                                       :show-results true
                                       :result (list)})
          post-ch       (chan (sliding-buffer 1))]
        ;; setup logic (once)
        (prn "setup go-loop")
        (go-loop []
                 (let [value (<! post-ch)
                       response (<! (http/post url {:json-params {:value value :total 10}}))]
                     (swap! input-state assoc :result (:body response))
                     (recur)))
        (fn []
            [:div#autocomplete
             [autocomplete-input post-ch default-value input-state]
             [active-filters-component filter-state]
             [autocomplete-lister filter-state input-state]
             ])));  }}}

;;
;; Year Component
;;
(defn year-component [filter-state];  {{{
    (let [url      "http://10.0.0.55:3000/rpc/stats_year_by_filter_accidents"
          state (reagent/atom {:has-result false
                               :result (list)
                               :last-filter-state @filter-state})
          post-ch   (chan (sliding-buffer 1))]
        (go-loop []
                 (let [fs (<! post-ch)
                       response (<! (http/post url {:json-params {:_borough       (:borough fs)
                                                                  :_intersection1 (:intersection1 fs)
                                                                  :_intersection2 (:intersection2 fs)
                                                                  :_off_street    (:off-street fs)
                                                                  :_vehicle_type  (:vehicle-type fs)
                                                                  :_factor        (:factor fs)
                                                                  :_cluster_key   (:cluster-id fs)}}))]
                     (reset! state {:result (:body response)
                                    :last-filter-state @filter-state
                                    :has-result true})
                     (recur)))
        (fn []
            (if (not= (:last-filter-state @state) @filter-state)
                (put! post-ch @filter-state)) 
            [:div
             [:h2 "Year"]
             (if (:has-result @state)
                 [:ul
                  (for [item (:result @state)]
                      ^{:key item} [:li (:year item) ": " (:count item)])])])));  }}}

;;
;; Month Component
;;
(defn month-component [filter-state];  {{{
    (let [url      "http://10.0.0.55:3000/rpc/stats_month_by_filter_accidents"
          state (reagent/atom {:has-result false
                               :result (list)
                               :last-filter-state @filter-state})
          post-ch   (chan (sliding-buffer 1))]
        (go-loop []
                 (let [fs (<! post-ch)
                       response (<! (http/post url {:json-params {:_borough       (:borough fs)
                                                                  :_intersection1 (:intersection1 fs)
                                                                  :_intersection2 (:intersection2 fs)
                                                                  :_off_street    (:off-street fs)
                                                                  :_vehicle_type  (:vehicle-type fs)
                                                                  :_factor        (:factor fs)
                                                                  :_cluster_key   (:cluster-id fs)}}))]
                     (reset! state {:result (:body response)
                                    :last-filter-state @filter-state
                                    :has-result true})
                     (recur)))
        (fn []
            (if (not= (:last-filter-state @state) @filter-state)
                (put! post-ch @filter-state)) 
            [:div
             [:h2 "Month"]
             (if (:has-result @state)
                 [:ul
                  (for [item (:result @state)]
                      ^{:key item} [:li (:month item) ": " (:count item)])])])));  }}}

(defn weekday-component [filter-state]
    (let [url      "http://10.0.0.55:3000/rpc/stats_weekday_by_filter_accidents"
          state (reagent/atom {:has-result false
                               :result (list)
                               :last-filter-state @filter-state})
          post-ch   (chan (sliding-buffer 1))]
        (go-loop []
                 (let [fs (<! post-ch)
                       response (<! (http/post url {:json-params {:_borough       (:borough fs)
                                                                  :_intersection1 (:intersection1 fs)
                                                                  :_intersection2 (:intersection2 fs)
                                                                  :_off_street    (:off-street fs)
                                                                  :_vehicle_type  (:vehicle-type fs)
                                                                  :_factor        (:factor fs)
                                                                  :_cluster_key   (:cluster-id fs)}}))]
                     (reset! state {:result (:body response)
                                    :last-filter-state @filter-state
                                    :has-result true})
                     (recur)))
        (fn []
            (if (not= (:last-filter-state @state) @filter-state)
                (put! post-ch @filter-state)) 
            [:div
             [:h2 "Weekday"]
             (if (:has-result @state)
                 [:ul
                  (for [item (:result @state)]
                      ^{:key item} [:li (:name item) ": " (:count item)])])])))

;;
;; Casualty Component
;;
(defn casualty-component [filter-state];  {{{
    (let [url      "http://10.0.0.55:3000/rpc/stats_casualties_by_filter_accidents"
          state (reagent/atom {:count nil
                               :total-number-persons-injured nil
                               :total-number-persons-killed nil
                               :total-number-pedestrians-injured nil
                               :total-number-pedestrians-killed nil
                               :total-number-cyclist-injured nil
                               :total-number-cyclist-killed nil
                               :total-number-motorist-injured nil
                               :total-number-motorist-killed nil
                               :has-result false
                               :last-filter-state @filter-state})
          post-ch   (chan (sliding-buffer 1))]
        (go-loop []
                 (let [fs (<! post-ch)
                       response (<! (http/post url {:json-params {:_borough       (:borough fs)
                                                                  :_intersection1 (:intersection1 fs)
                                                                  :_intersection2 (:intersection2 fs)
                                                                  :_off_street    (:off-street fs)
                                                                  :_vehicle_type  (:vehicle-type fs)
                                                                  :_factor        (:factor fs)
                                                                  :_cluster_key   (:cluster-id fs)}}))
                       x (first (:body response))]
                     (reset! state {:count (:count x)
                                    :total-number-persons-injured (:total_number_persons_injured x)
                                    :total-number-persons-killed (:total_number_persons_killed x)
                                    :total-number-pedestrians-injured (:total_number_pedestrians_injured x)
                                    :total-number-pedestrians-killed (:total_number_pedestrians_killed x)
                                    :total-number-cyclist-injured (:total_number_cyclist_injured x)
                                    :total-number-cyclist-killed (:total_number_cyclist_killed x)
                                    :total-number-motorist-injured (:total_number_motorist_injured x)
                                    :total-number-motorist-killed (:total_number_motorist_killed x)
                                    :has-result true
                                    :last-filter-state @filter-state})
                     (prn "got response. current state: " @state)
                     (recur)))
        (fn []
            (let [this (reagent/current-component)]
                (if (not= (:last-filter-state @state) @filter-state)
                    (put! post-ch @filter-state)) 
                [:div
                 [:h2 "Casualties"]
                 (if (:has-result @state)
                     [:ul
                      [:li "Total Accident Count: " (:count @state)]
                      [:li "Persons Injured: " (:total-number-persons-injured @state)]
                      [:li "Persons Killed " (:total-number-persons-killed @state)]
                      [:li "Motorist Injured " (:total-number-motorist-injured @state)]
                      [:li "Motorist Killed " (:total-number-motorist-killed @state)]
                      [:li "Cyclist Injured " (:total-number-cyclist-injured @state)]
                      [:li "Cyclist Killed " (:total-number-cyclist-killed @state)]
                      [:li "Pedestrians Injured " (:total-number-pedestrians-injured @state)]
                      [:li "Pedestrians Killed " (:total-number-pedestrians-killed @state)]
                      ])]))));  }}}

(defn cluster-component []
    [:div#cluster
     [:h2 "Clustering"]])

(defn season-component []
    [:div
     [:h2 "Timeline"]])

(defn setup-filter-controller [filter-state]
    "A controller responsible for actually adding new filters and triggering
    dependent components after filters have changed."
    (prn "setting up filter controller")
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
         [cluster-component]
         [season-component]
         [year-component filter-state]
         [month-component filter-state]
         [weekday-component filter-state]
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
