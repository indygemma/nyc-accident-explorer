(ns btw-client.core
    (:require
        [reagent.core :as reagent]
        [cljs.core.async :refer [put! take! chan close! <! >! sliding-buffer]]
        [cljs-http.client :as http]
        [goog.net.XhrIo :as xhr]
        [cljs.pprint :refer [cl-format]]
        )
    (:require-macros [cljs.core.async.macros :refer [go go-loop alt!]]))

(enable-console-print!)

(defn int-comma [n] (cl-format nil "~:d" n))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Vars

(defn service-url []
    "http://btw.lab.indygemma.com/pgrest"
    )

(defonce debug?
  ^boolean js/goog.DEBUG)

(defn debug [& args]
    (when debug?
      apply prn args))

(defonce app-state
  (reagent/atom
   {:text "This is the initial text "
    :hello 10
    :filters {; the list of filters
              :year1 nil
              :month1 nil
              :year2 nil
              :month2 nil
              :hour nil
              :weekday nil
              :casualty-type nil
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
                       ;(debug "Removing filter: " value)
                       (swap! filter-state assoc filter-type nil)
                       false)
         }
     value]);  }}}
(defn active-filters-component [filter-state];  {{{
    (let [casualty-type (:casualty-type @filter-state)
          borough       (:borough @filter-state)
          intersection1 (:intersection1 @filter-state)
          intersection2 (:intersection2 @filter-state)
          off-street    (:off-street @filter-state)
          vehicle-type  (:vehicle-type @filter-state)
          factor        (:factor @filter-state)
          cluster-key   (:cluster-id @filter-state)]
        [:div#active-filters
         ;[:h5 "Active Filters"
          [:ul
           (if (not (empty? casualty-type))
               [:li [:span "Casualty Type "] (removable-filter filter-state :casualty-type casualty-type)])
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
              :else (debug "Unknown type: " t))));  }}}
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
    (let [url           (str (service-url) "/rpc/autocomplete_all")
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

;;
;; Chart Canvas logic
;;
(defn- length [[from to]];  {{{
    (- to from));  }}}
(defn- domain-to-range;  {{{
    "Converts a value from domain to range. In other words, maps the current state of the world
    to what the animation's value needs to be."
    [[domain-from domain-to :as domain] [range-from range-to :as range] domain-value]
    (let [domain-len (length domain)
          range-len (length range)
          domain-offset (- domain-value domain-from)
          ratio (/ domain-offset domain-len)
          range-offset (* ratio range-len)]
        (+ range-offset range-from)));  }}}

(defn calculate-horizontal-layer-positions [layer-name canvas layer-opts state result];  {{{
  (let [w (:canvas-width result)
        h (:canvas-height result)
        n (count (:result @state))
        ; (1) calculate element width (because of horizontal)
        f (:max-element-width layer-opts)
        max-el-w (apply f [canvas state])
        ; (2) calculate element height
        tl-y      (:t (:area-padding layer-opts))
        bl-y (- h (:b (:area-padding layer-opts)))
        el-h (/ (- bl-y tl-y) n)
        ; (3) element stats
        el-w (+ (apply (:l (:element-padding layer-opts)) [max-el-w])
                max-el-w
                (apply (:r (:element-padding layer-opts)) [max-el-w]))
        el-tl-x (apply (:l (:element-padding layer-opts)) [max-el-w])
        el-tl-y (apply (:t (:element-padding layer-opts)) [el-h])
        el-bl-x el-tl-x
        el-bl-y (- el-h (apply (:b (:element-padding layer-opts)) [el-h]))
        el-tr-x (- el-w (apply (:r (:element-padding layer-opts)) [max-el-w]))
        el-tr-y el-tl-y
        el-br-x el-tr-x
        el-br-y el-bl-y
        ; (4) calculate area padded positions
        tl-x (apply (:l (:area-padding layer-opts)) [result el-w])
        bl-x tl-x
        tr-x (- w (apply (:r (:area-padding layer-opts)) [result el-w]))
        tr-y tl-y
        br-x tr-x
        br-y bl-y
        x (- w tl-x)
        y tl-y]
    ;(prn "max element width: " max-el-w)
    ;(prn "element height: " el-h)
    ;(prn "total element width (incl. padding): " el-w)
    ;(prn "el stats: [tl.x,tl.y] " [el-tl-x el-tl-y])
    ;(prn "el stats: [bl.x,bl.y] " [el-bl-x el-bl-y])
    ;(prn "el stats: [tr.x,tr.y] " [el-tr-x el-tr-y])
    ;(prn "el stats: [br.x,br.y] " [el-br-x el-br-y])
    ;(prn "area stats: [tl.x,tl.y] " [tl-x tl-y])
    ;(prn "area stats: [tr.x,tr.y] " [tr-x tr-y])
    ;(prn "area stats: [bl.x,bl.y] " [bl-x bl-y])
    ;(prn "area stats: [br.x,br.y] " [br-x br-y])
    (assoc result layer-name
           {:x x
            :y y
            :tl-x tl-x
            :tl-y tl-y
            :tr-x tr-x
            :tr-y tr-y
            :bl-x bl-x
            :bl-y bl-y
            :br-x br-x
            :br-y br-y
            :el-tl-x el-tl-x
            :el-tl-y el-tl-y
            :el-tr-x el-tr-x
            :el-tr-y el-tr-y
            :el-bl-x el-bl-x
            :el-bl-y el-bl-y
            :el-br-x el-br-x
            :el-br-y el-br-y
            :el-w el-w
            :el-h el-h
            :max-el-w max-el-w
            })));  }}}
(defn calculate-vertical-layer-positions [layer-name canvas layer-opts state result];  {{{
  (let [w (:canvas-width result)
        h (:canvas-height result)
        n (count (:result @state))
        ; (1) calculate element height (by using the width-length of the max element)
        f (:max-element-width layer-opts)
        max-el-w (apply f [canvas state])
        ; (2) calculate static element width
        tl-x      (:l (:area-padding layer-opts))
        tr-x (- w (:r (:area-padding layer-opts)))
        el-w (/ (- tr-x tl-x) n)
        ; (3) element stats
        el-h (+ (apply (:t (:element-padding layer-opts)) [max-el-w])
                max-el-w
                (apply (:b (:element-padding layer-opts)) [max-el-w]))
        el-tl-x (apply (:l (:element-padding layer-opts)) [el-w])
        el-tl-y (apply (:t (:element-padding layer-opts)) [el-h])
        el-bl-x el-tl-x
        el-bl-y (- el-h (apply (:b (:element-padding layer-opts)) [el-h]))
        el-tr-x (- el-w (apply (:r (:element-padding layer-opts)) [el-w]))
        el-tr-y el-tl-y
        el-br-x el-tr-x
        el-br-y el-bl-y
        ; (4) calculate area padded positions
        bl-x tl-x
        br-x tr-x
        tl-y      (apply (:t (:area-padding layer-opts)) [result el-h])
        bl-y (- h (apply (:b (:area-padding layer-opts)) [result el-h]))
        tr-y tl-y
        br-y bl-y
        x (- w tl-x)
        y tl-y
        width (- tr-x tl-x)
        height (- bl-y tl-y)]
    ;(prn "max element width: " max-el-w)
    ;(prn "element height: " el-h)
    ;(prn "total element width (incl. padding): " el-w)
    ;(prn "el stats: [tl.x,tl.y] " [el-tl-x el-tl-y])
    ;(prn "el stats: [bl.x,bl.y] " [el-bl-x el-bl-y])
    ;(prn "el stats: [tr.x,tr.y] " [el-tr-x el-tr-y])
    ;(prn "el stats: [br.x,br.y] " [el-br-x el-br-y])
    ;(prn "area stats: [tl.x,tl.y] " [tl-x tl-y])
    ;(prn "area stats: [tr.x,tr.y] " [tr-x tr-y])
    ;(prn "area stats: [bl.x,bl.y] " [bl-x bl-y])
    ;(prn "area stats: [br.x,br.y] " [br-x br-y])
    (assoc result layer-name
           {:x x
            :y y
            :w width
            :h height
            :tl-x tl-x
            :tl-y tl-y
            :tr-x tr-x
            :tr-y tr-y
            :bl-x bl-x
            :bl-y bl-y
            :br-x br-x
            :br-y br-y
            :el-tl-x el-tl-x
            :el-tl-y el-tl-y
            :el-tr-x el-tr-x
            :el-tr-y el-tr-y
            :el-bl-x el-bl-x
            :el-bl-y el-bl-y
            :el-br-x el-br-x
            :el-br-y el-br-y
            :el-w el-w
            :el-h el-h
            :max-el-w max-el-w
            })));  }}}
(defn calculate-layer-positions [layer-name canvas layer-opts state result];  {{{
  (if (= :vertical (:direction layer-opts))
    (calculate-vertical-layer-positions layer-name canvas layer-opts state result)
    (calculate-horizontal-layer-positions layer-name canvas layer-opts state result)));  }}}
(defn sample-draw-chart [draw-opts canvas state canvas-state];  {{{
  ;(prn "state: " (:result @state))
  ;(prn "max :name " (apply max-key :name (:result @state)))
  ;(prn "max :count " (apply max-key :count (:result @state)))
  ;(prn "draw-opts: " draw-opts)
  (let [ctx (.getContext canvas "2d")
        w (.-clientWidth canvas)
        h (.-clientHeight canvas)
        total-accidents (:filtered-result @app-state)
        max-value (:count (apply max-key :count (:result @state)))
        result {:canvas-width w
                :canvas-height h}
        label-layer-opts (:labels (:layers draw-opts))
        result (calculate-layer-positions :labels canvas label-layer-opts state result)
        bar-label-layer-opts (:bar-labels (:layers draw-opts))
        result (calculate-layer-positions :bar-labels canvas bar-label-layer-opts state result)
        bars-layer-opts (:bars (:layers draw-opts))
        result (calculate-layer-positions :bars canvas bars-layer-opts state result)
          ]
    (.clearRect ctx (- 0 0.5) (- 0 0.5) (- w 0.5) (- h 0.5))
    ;; draw labels
    (let [x (:labels result)]
      (.beginPath ctx)
      (if (:debug label-layer-opts)
        (.fillRect ctx (:tl-x x) (:tl-y x) (- (:tr-x x) (:tl-x x)) (- (:br-y x) (:tr-y x)))
        (doseq [[value index] (map vector (:result @state) (range))]
          (let [mt (.measureText ctx (:name value))
                tw (.-width mt)
                pos-x (if (= (:element-align label-layer-opts) :right)
                        (- (:tr-x x) tw)
                        (+ (:tl-x x) (:el-tl-x x)))
                hover? (= (:hovered-index @state) index)]
            (if hover?
              (do (set! (.-fillStyle ctx) "red"))
              (do (set! (.-fillStyle ctx) "black")))
            (.fillText ctx (:name value) pos-x (+ (* index (:el-h x)) (:el-tl-y x))))))
      (.closePath ctx))
    ;; draw bars
    (let [x (:bars result)]
      (.beginPath ctx)

            ;; draw grid: 25%
            (let [domain-value (domain-to-range [0 max-value] [(:tl-x x) (:tr-x x)] (/ max-value 4))
                  range-value  (domain-to-range [(:tl-x x) (:tr-x x)] [0 max-value] domain-value)
                  percentage   (* (/ range-value total-accidents) 100)
                  label        (str (int percentage) "%")]
              (set! (.-strokeStyle ctx) "#ddd")
              ;; draw middle point verticle line
              (.setLineDash ctx (array 3 2))
              (.moveTo ctx domain-value 0)
              (.lineTo ctx domain-value (:bl-y x))
              (.stroke ctx)
              (set! (.-fillStyle ctx) "#ddd")
              (.fillText ctx label (- domain-value (/ (.-width (.measureText ctx label)) 2)) (+ (:br-y x) (:el-tl-y x))))

            ;; draw grid: 50%
            (let [domain-value (domain-to-range [0 max-value] [(:tl-x x) (:tr-x x)] (/ max-value 2))
                  range-value  (domain-to-range [(:tl-x x) (:tr-x x)] [0 max-value] domain-value)
                  percentage   (* (/ range-value total-accidents) 100)
                  label        (str (int percentage) "%")]
              (set! (.-strokeStyle ctx) "#ddd")
              ;; draw middle point verticle line
              (.setLineDash ctx (array 3 2))
              (.moveTo ctx domain-value 0)
              (.lineTo ctx domain-value (:bl-y x))
              (.stroke ctx)
              (set! (.-fillStyle ctx) "#ddd")
              (.fillText ctx label (- domain-value (/ (.-width (.measureText ctx label)) 2)) (+ (:br-y x) (:el-tl-y x))))

            ;; draw grid: 75%
            (let [domain-value (domain-to-range [0 max-value] [(:tl-x x) (:tr-x x)] (* max-value 0.75))
                  range-value  (domain-to-range [(:tl-x x) (:tr-x x)] [0 max-value] domain-value)
                  percentage   (* (/ range-value total-accidents) 100)
                  label        (str (int percentage) "%")]
              (set! (.-strokeStyle ctx) "#ddd")
              ;; draw middle point verticle line
              (.setLineDash ctx (array 3 2))
              (.moveTo ctx domain-value 0)
              (.lineTo ctx domain-value (:bl-y x))
              (.stroke ctx)
              (set! (.-fillStyle ctx) "#ddd")
              (.fillText ctx label (- domain-value (/ (.-width (.measureText ctx label)) 2)) (+ (:br-y x) (:el-tl-y x))))

            ;; draw grid: 100%
            (let [domain-value (domain-to-range [0 max-value] [(:tl-x x) (:tr-x x)] max-value)
                  range-value  (domain-to-range [(:tl-x x) (:tr-x x)] [0 max-value] domain-value)
                  percentage   (* (/ range-value total-accidents) 100)
                  label        (str (int percentage) "%")]
              (set! (.-strokeStyle ctx) "#ddd")
              ;; draw middle point verticle line
              (.setLineDash ctx (array 3 2))
              (.moveTo ctx domain-value 0)
              (.lineTo ctx domain-value (:bl-y x))
              (.stroke ctx)
              (set! (.-fillStyle ctx) "#ddd")
              (.fillText ctx label (- domain-value (/ (.-width (.measureText ctx label)) 2)) (+ (:br-y x) (:el-tl-y x))))

      (if (:debug bars-layer-opts)
        (.fillRect ctx (:tl-x x) (:tl-y x) (- (:tr-x x) (:tl-x x)) (- (:br-y x) (:tr-y x)))
        (doseq [[value index] (map vector (:result @state) (range))]
          (let [w  (domain-to-range [0 max-value] [(:tl-x x) (:tr-x x)] (:count value))
                h (:el-bl-y x)
                pos-x (if (= (:element-align bars-layer-opts) :right)
                        (- (:tr-x x) w)
                        (+ (:tl-x x) (:el-tl-x x)))
                hover? (= (:hovered-index @state) index)
                pos-y (+ (* index (:el-h x)) (:el-tl-y x))]
            (if hover?
              (do (set! (.-fillStyle ctx) "red"))
              (do (set! (.-fillStyle ctx) "darkblue")))
            (.fillRect ctx pos-x pos-y w h)
            (swap! canvas-state assoc :positions (conj (:positions @canvas-state) [pos-x pos-y (+ pos-x w) (+ pos-y h)]))
            ))
            )
      (.closePath ctx))

    ;; draw bar labels
    (let [x (:bar-labels result)]
      (.beginPath ctx)
      (if (:debug bar-label-layer-opts)
        (.fillRect ctx (:tl-x x) (:tl-y x) (- (:tr-x x) (:tl-x x)) (- (:br-y x) (:tr-y x)))
        (doseq [[value index] (map vector (:result @state) (range))]
          (let [mt (.measureText ctx (str (int-comma (:count value))))
                tw (.-width mt)
                w  (domain-to-range [0 max-value] [(:tl-x x) (:tr-x x)] (:count value))
                pos-x (if (= (:element-align bars-layer-opts) :right)
                        (- (:tr-x x) tw)
                        (+ (:tl-x x) (:el-tl-x x)))
                hover? (= (:hovered-index @state) index)]
            (if hover?
              (do (set! (.-fillStyle ctx) "red"))
              (do (set! (.-fillStyle ctx) "black")))
            (.fillText ctx (str (int-comma (:count value))) (+ (:el-tl-x x) w) (+ (* index (:el-h x)) (:el-tl-y x)))
            )))
      (.closePath ctx))
    )
  ); }}}
(defn horizontal-barchart [draw-opts canvas state canvas-state];  {{{
    (if (:has-result @state)
        (let [ctx    (.getContext canvas "2d")
              w      (- (.-clientWidth canvas) 0.5 50)
              pad-h  10
              h      (- (.-clientHeight canvas) 0.5 pad-h)
              max-value (:filtered-result @app-state)
              max-n     (count (:result @state))
              ; first entry has the highest count
              highest   (first (:result @state))
              lowest    (last  (:result @state))
              max-text-width (first (sort #(compare %2 %1) (map (fn [item] (.-width (.measureText ctx (:name item)))) (:result @state))))
              max-single-h  (/ h 5)
              single-item-h (/ h max-n)
              single-h      (if (> single-item-h max-single-h)
                              max-single-h
                              single-item-h)
              ;sw (+ max-text-width (/ max-text-width 10))
              sw 0
              ew (- w max-text-width)
              ]
            (.clearRect ctx 0 0 w h)

            ; set font size for the grid labels
            (set! (.-font ctx) (str "10px sans-serif"))

            (.beginPath ctx)
            (set! (.-strokeStyle ctx) "#ddd")
            ;; draw middle point verticle line
            (.setLineDash ctx (array 3 2))
            (.moveTo ctx (domain-to-range [0 max-value] [sw ew] (/ max-value 4)) 0)
            (.lineTo ctx (domain-to-range [0 max-value] [sw ew] (/ max-value 4)) (- h pad-h))
            (.stroke ctx)
            (set! (.-fillStyle ctx) "#ddd")
            (.fillText ctx "25%" (- (domain-to-range [0 max-value] [sw ew] (/ max-value 4))
                                    (/ (.-width (.measureText ctx "25%")) 2))
                       h)
            (.closePath ctx)

            (.beginPath ctx)
            (set! (.-strokeStyle ctx) "#ddd")
            ;; draw middle point verticle line
            (.setLineDash ctx (array 3 2))
            (.moveTo ctx (domain-to-range [0 max-value] [sw ew] (/ max-value 2)) 0)
            (.lineTo ctx (domain-to-range [0 max-value] [sw ew] (/ max-value 2)) (- h pad-h))
            (.stroke ctx)
            (set! (.-fillStyle ctx) "#ddd")
            (.fillText ctx "50%" (- (domain-to-range [0 max-value] [sw ew] (/ max-value 2))
                                    (/ (.-width (.measureText ctx "50%")) 2))
                       h)
            (.closePath ctx)

            (.beginPath ctx)
            (set! (.-strokeStyle ctx) "#ddd")
            ;; draw middle point verticle line
            (.setLineDash ctx (array 3 2))
            (.moveTo ctx (domain-to-range [0 max-value] [sw ew] (* max-value 0.75)) 0)
            (.lineTo ctx (domain-to-range [0 max-value] [sw ew] (* max-value 0.75)) (- h pad-h))
            (.stroke ctx)
            (set! (.-fillStyle ctx) "#ddd")
            (.fillText ctx "75%" (- (domain-to-range [0 max-value] [sw ew] (* max-value 0.75))
                                    (/ (.-width (.measureText ctx "75%")) 2))
                       h)
            (.closePath ctx)

            (.beginPath ctx)
            (.setLineDash ctx (array 0))
            ;(set! (.-strokeStyle ctx) "#ddd")
            ;; draw grid
            ;(doseq [index (range 0 h single-h)]
            ;(.moveTo ctx sw index)
            ;(.lineTo ctx w index)
            ;(.moveTo ctx sw h)
            ;(.lineTo ctx w  h)
            ;(.stroke ctx)
            ;(set! (.-fillStyle ctx) "darkblue")
            (set! (.-strokeStyle ctx) "#fff")
            (let [padding-h (/ single-h 7)]
                (doseq [[value index] (map vector (:result @state) (range))]
                    (let [x1 sw
                          y1 (+ (* index single-h) padding-h)
                          vw (domain-to-range [0 max-value] [0 (- ew sw)] (:count value))
                          vh (- single-h (* padding-h 2))
                          x2 (+ x1 vw)
                          y2 (+ y1 vh)
                          hover? (= (:hovered-index @state) index)]
                        (if hover?
                            (do (set! (.-fillStyle ctx) "red"))
                            (do (set! (.-fillStyle ctx) "darkblue")))
                        (.fillRect ctx x1 y1 vw vh)
                        (.strokeRect ctx x1 y1 vw vh)
                        (swap! canvas-state assoc :positions (conj (:positions @canvas-state) [x1 y1 x2 y2]))
                        ;(debug "canvas-state: " canvas-state)
                    )))
            (.closePath ctx)

            (.beginPath ctx)
            (let [font-size (* 0.35 single-h)]
            (set! (.-font ctx) (str font-size "px sans-serif")))
            ;; draw names (at each row)
            (set! (.-fillStyle ctx) "black")
            ;(set! (.-font-weight ctx) "bolder")
            (doseq [[value index] (map vector (:result @state) (range))]
                (let [mt (.measureText ctx (:name value))
                      tw (.-width mt)
                      hover? (= (:hovered-index @state) index)]
                  (if hover?
                    (do (set! (.-fillStyle ctx) "red"))
                    (do (set! (.-fillStyle ctx) "black")))
                  (.fillText ctx (:name value) (- w tw) (+ (* index single-h) (/ single-h 1.6)))))

            ;; draw values after each bar (if possible)
            ;(debug "DRAWING BOROUGH VALUES " (/ (int (first (clojure.string/split (.-font ctx) #"px"))) single-h) (.-font ctx))
            (doseq [[value index] (map vector (:result @state) (range))]
                (let [mt (.measureText ctx (int-comma (:count value)))
                      tw (.-width mt)
                      pw (domain-to-range [0 max-value] [0 (- ew sw)] (:count value))
                      nmt (.measureText ctx (int-comma (:name value)))
                      ntw (.-width nmt)
                      ]
                    (if (< (+ pw tw) (- ew ntw))
                        (do (set! (.-fillStyle ctx) "#000")
                            (.fillText ctx (int-comma (:count value)) (+ pw 5)     (+ (* index single-h) (/ single-h 1.6))))
                        (do (set! (.-fillStyle ctx) "#fff")
                            (.fillText ctx (int-comma (:count value)) (- pw tw 5)  (+ (* index single-h) (/ single-h 1.6)))))))

            (.closePath ctx))));  }}}
(defn vertical-barchart [draw-opts canvas state canvas-state];  {{{
  (let [ctx (.getContext canvas "2d")
        w (.-clientWidth canvas)
        h (.-clientHeight canvas)
        total-accidents (:filtered-result @app-state)
        max-value (:count (apply max-key :count (:result @state)))
        result {:canvas-width w
                :canvas-height h}
        label-layer-opts (:labels (:layers draw-opts))
        result (calculate-layer-positions :labels canvas label-layer-opts state result)
        bar-label-layer-opts (:bar-labels (:layers draw-opts))
        result (calculate-layer-positions :bar-labels canvas bar-label-layer-opts state result)
        bars-layer-opts (:bars (:layers draw-opts))
        result (calculate-layer-positions :bars canvas bars-layer-opts state result)
        ]
    (.clearRect ctx (- 0 0.5) (- 0 0.5) (- w 0.5) (- h 0.5))
    ;; draw labels
    (let [x (:labels result)]
      (.beginPath ctx)
      (if (:debug label-layer-opts)
        (doseq [[value index] (map vector (:result @state) (range))]
          (.fillRect ctx (+ (* index (:el-w x)) (:el-tl-x x))
                         (:tl-y x)
                         (:el-w x)
                         (:el-h x)))
        (doseq [[value index] (map vector (:result @state) (range))]
          (let [key ((:key-label label-layer-opts) value)
                mt (.measureText ctx key)
                tw (.-width mt)
                pos-x (+ (:tl-x x) (* index (:el-w x)) (:el-tl-x x))
                pos-y (+ (:tl-y x) (:el-tl-y x))
                hover? (= (:hovered-index @state) index)
                rotate? (not (nil? (:rotate label-layer-opts)))]
            (if hover?
              (do (set! (.-fillStyle ctx) "red"))
              (do (set! (.-fillStyle ctx) "black")))
            (if rotate?
              (do (.save ctx)
                  (.translate ctx pos-x pos-y)
                  (.rotate ctx  (* (:rotate label-layer-opts) (/ (.-PI js/Math) 180)))
                  (.fillText ctx key 0 0)
                  (.restore ctx))
              ; no rotation, but adjust to middle
              (do (.fillText ctx key (+ pos-x (/ (:el-w x) 4.5))
                                     pos-y))))))
      (.closePath ctx))

    ;; draw bars
    (let [x (:bars result)
          draw-grid (fn [value]
                      (let [domain-value (domain-to-range [0 max-value] [(:tl-y x) (:bl-y x)] value)
                            range-value  (domain-to-range [(:tl-y x) (:bl-y x)] [0 max-value] domain-value)
                            percentage   (* (/ range-value total-accidents) 100)
                            label        (str (int percentage) "%")]
                        (set! (.-strokeStyle ctx) "#ddd")
                        ;; draw middle point verticle line
                        (.setLineDash ctx (array 3 2))
                        (.moveTo ctx 30 (- (:h x) domain-value))
                        (.lineTo ctx w  (- (:h x) domain-value))
                        (.stroke ctx)
                        (set! (.-fillStyle ctx) "#ddd")
                        (.fillText ctx label 5 (+ (- (:h x) domain-value) 10))
                        ))]
      (.beginPath ctx)

      ;; draw grids
      (draw-grid (/ max-value 4))
      (draw-grid (/ max-value 2))
      (draw-grid (* max-value 0.75))
      (draw-grid max-value)

      (if (:debug bars-layer-opts)
        (.fillRect ctx (:tl-x x) (:tl-y x) (- (:tr-x x) (:tl-x x)) (- (:br-y x) (:tr-y x)))
        (doseq [[value index] (map vector (:result @state) (range))]
          (let [key (get value (:key bars-layer-opts))
                mt (.measureText ctx (str (int-comma key)))
                tw (.-width mt)
                h  (domain-to-range [0 max-value] [0 (- (:bl-y x) (:tl-y x))] key)
                pos-x (+ (:tl-x x) (* index (:el-w x)) (:el-tl-x x))
                pos-y (- (:bl-y x) h)
                w 20
                hover? (= (:hovered-index @state) index)]
            (if hover?
              (do (set! (.-fillStyle ctx) "red"))
              (do (set! (.-fillStyle ctx) "darkblue")))
            (.fillRect ctx pos-x pos-y w h)
            ;(.fillText ctx (str (int-comma (:count value))) pos-x (+ (* index (:el-h x)) (:el-tl-y x)))
            )))
      (.closePath ctx))

    ;; draw bar labels
    (let [x (:bar-labels result)]
      (.beginPath ctx)
      (if (:debug bar-label-layer-opts)
        (.fillRect ctx (:tl-x x) (:tl-y x) (- (:tr-x x) (:tl-x x)) (- (:br-y x) (:tr-y x)))
        (doseq [[value index] (map vector (:result @state) (range))]
          (let [mt (.measureText ctx (str (int-comma (:count value))))
                tw (.-width mt)
                w  (domain-to-range [0 max-value] [(:tl-x x) (:tr-x x)] (:count value))
                pos-x (+ (:tl-x x) (* index (:el-w x)) (:el-tl-x x) (/ (:el-w x) 5))
                pos-y (- (:bl-y x) (:el-bl-y x))
                hover? (= (:hovered-index @state) index)]
            (if hover?
              (do (set! (.-fillStyle ctx) "red"))
              (do (set! (.-fillStyle ctx) "#fff")))
            (do (.save ctx)
                (.translate ctx pos-x pos-y)
                (.rotate ctx  (* -90 (/ (.-PI js/Math) 180)))
                (.fillText ctx (str (int-comma (:count value))) 0 0)
                (.restore ctx))
            ;(.fillText ctx (str (int-comma (:count value))) pos-x pos-y)
            )))
      (.closePath ctx))
    ));  }}}
(defn default-horizontal-barchart-options []
  {:layers {:labels {:direction :horizontal
                     ;:debug true
                     :element-align :left
                     :area-padding {:l (fn [pos element-width] (- (:canvas-width pos)
                                                                  element-width))
                                    :t 10
                                    :b 10
                                    :r (fn [pos element-width] 0)}
                     :element-padding {:l (fn [element-width] (/ element-width 10))
                                       :t (fn [element-height] (/ element-height 1.6))
                                       :b (fn [element-height] (/ element-height 1.6))
                                       :r (fn [element-width] (/ element-width 10))}
                     ; calculate the maxium width
                     :max-element-width (fn [canvas state]
                                          (let [x (apply max-key :name (:result @state))
                                                w (.-width (.measureText (.getContext canvas "2d") (:name x)))]
                                            w))
                     }
            :bar-labels {:direction :horizontal
                         :element-align :left
                         :debug false
                         :area-padding {:l (fn [pos element-width] 0)
                                        :t 10
                                        :b 10
                                        :r (fn [pos element-width] (+ (:x (:labels pos))
                                                                      element-width))
                                        }
                         :element-padding {:l (fn [element-width]  (/ element-width  10))
                                           :t (fn [element-height] (/ element-height 1.6))
                                           :b (fn [element-height] (/ element-height 1.6))
                                           :r (fn [element-width]  (/ element-width  10))}
                         ; calculate the maxium width
                         :max-element-width (fn [canvas state]
                                              (let [x (apply max-key :count (:result @state))
                                                    w (.-width (.measureText (.getContext canvas "2d")
                                                                             (str (int-comma (:count x)))))]
                                                w))
                         }
            :bars {:direction :horizontal
                   :element-align :left
                   :debug false
                   :area-padding {:l (fn [pos element-width] 0)
                                  :t 10
                                  :b 10
                                  :r (fn [pos element-width] (+ (:x (:labels pos))
                                                                (:el-w (:bar-labels pos))))
                                  }
                   :element-padding {:l (fn [element-width]  (/ element-width  10))
                                     :t (fn [element-height] (/ element-height 7))
                                     :b (fn [element-height] (/ element-height 7))
                                     :r (fn [element-width]  (/ element-width  10))}
                   ; calculate the maxium width (we split the field into 4: 25%)
                   :max-element-width (fn [canvas state] 0)
                   }
            }
   })
(defn default-vertical-barchart-options ; {{{
  ([key-calc key-label value]
   (default-vertical-barchart-options key-calc key-label value nil))
  ([key-calc key-label value rotate-by]
   {:layers {:labels {:direction :vertical
                      :key-calc key-calc
                      :key-label key-label
                      :debug false
                      :rotate rotate-by
                      :area-padding {:l 40
                                     :t (fn [pos element-height] (- (:canvas-height pos)
                                                                    element-height))
                                     :b (fn [pos element-height] 0)
                                     :r 0}
                      :element-padding {:l (fn [element-width]  0)
                                        :t (fn [element-height] (/ element-height 1.6))
                                        :b (fn [element-height] 0)
                                        :r (fn [element-width]  0)}
                      ; calculate the maxium width
                      :max-element-width (fn [canvas state]
                                           (let [x (apply max-key key-calc (:result @state))
                                                 w (.-width (.measureText (.getContext canvas "2d")
                                                                          (key-label x)))]
                                             w))
                      }
             :bar-labels {:direction :vertical
                          :key value
                          :debug false
                          :area-padding {:l 40
                                         :t (fn [pos element-height] 0)
                                         :b (fn [pos element-height] (:el-h (:labels pos)))
                                         :r 0
                                         }
                          :element-padding {:l (fn [element-width]  (/ element-width  10))
                                            :t (fn [element-height] (/ element-height 1.6))
                                            :b (fn [element-height] (/ element-height 1.6))
                                            :r (fn [element-width]  (/ element-width  10))}
                          ; calculate the maxium width
                          :max-element-width (fn [canvas state]
                                               (let [x (apply max-key value (:result @state))
                                                     w (.-width (.measureText (.getContext canvas "2d")
                                                                              (str (int-comma (value x)))))]
                                                 w))
                          }
             :bars {:direction :vertical
                    :key value
                    :debug false
                    :area-padding {:l 40
                                   :t (fn [pos element-height] 0)
                                   :b (fn [pos element-height] (:el-h (:labels pos)))
                                   :r 0
                                   }
                    :element-padding {:l (fn [element-width]  (/ element-width  10))
                                      :t (fn [element-height] 0)
                                      :b (fn [element-height] 0)
                                      :r (fn [element-width]  (/ element-width  10))}
                    ; calculate the maxium width (we split the field into 4: 25%)
                    :max-element-width (fn [canvas state] 0)
                    }
             }
    }));  }}}
;;
;; filtered base component
;;
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
                                                           (debug "default render logic for " component-name)))
          ;; if no update condition function has been supplied, use default one
          update-condition (or update? (fn [state filter-state]
                                           (not= (:last-filter-state @state) @filter-state)))]
        ;(debug "setting up " component-name)
        (go-loop []
                 ;(debug "updateing " component-name)
                 (let [[url fs] (<! post-ch)
                       response (<! (http/post url {:json-params {:_year1         (:year1 fs)
                                                                  :_month1        (:month1 fs)
                                                                  :_year2         (:year2 fs)
                                                                  :_month2        (:month2 fs)
                                                                  :_hour          (:hour fs)
                                                                  :_weekday       (:weekday fs)
                                                                  :_casualty_type (:casualty-type fs)
                                                                  :_borough       (:borough fs)
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
             :component-did-mount  (fn [this]
                                     ; initially, we will load whatever is active filter right now
                                     (put! post-ch [(:url @state) @filter-state])
                                     (component-did-mount this))
             :reagent-render       (fn []
                                       (if (update-condition state filter-state)
                                           (put! post-ch [(:url @state) @filter-state]))
                                       (component-render state filter-state))})))
;  }}}
(defn- get-offsets [el offset-x offset-y];  {{{
    (if (= (.-offsetParent el) js/undefined)
        [offset-x offset-y]
        (get-offsets (.-offsetParent el) (+ offset-x (.-offsetLeft el))
                                         (+ offset-y (.-offsetTop el)))));  }}}
(defn get-canvas-position [event canvas-state];  {{{
    """ given a mouse event and canvas object, return the correct [x y] position """
    (let [[offset-x offset-y] (get-offsets (:canvas @canvas-state) 0 0)
          ox                  (+ offset-x
                                 (:style-padding-left @canvas-state)
                                 (:style-border-left @canvas-state)
                                 (:html-left @canvas-state))
          oy                  (+ offset-y
                                 (:style-padding-top @canvas-state)
                                 (:style-border-top @canvas-state)
                                 (:html-top @canvas-state))]
        [(- (.-pageX event) ox)
         (- (.-pageY event) oy)]
        ));  }}}
(defn canvas-base-filtered-component [{component-name          :name;  {{{
                                       filter-state            :filter-state
                                       the-canvas-state        :canvas-state
                                       state                   :state
                                       update-state-on-load    :update-state-on-load
                                       draw-fn                 :on-draw
                                       draw-options            :draw-options
                                       canvas-at               :canvas-at
                                       component-render-fn     :component-render
                                       update?                 :update-condition}]
    (let [dom-node     (reagent/atom nil)
          canvas-state the-canvas-state]
        (debug component-name "on case base filtered component creation: " canvas-state)
        (base-filtered-component {:name component-name
                                  :filter-state filter-state
                                  :state state
                                  :update-state-on-load (fn [state filter-state response]
                                                            (update-state-on-load canvas-state state filter-state response))
                                  :component-did-update
                                  (fn [this state]
                                      (draw-fn draw-options (canvas-at @dom-node) state canvas-state))

                                  :component-did-mount
                                  (fn [this]
                                      (let [canvas (canvas-at (reagent/dom-node this))]
                                          (reset! dom-node (reagent/dom-node this))
                                          ; set default translate 0.5 0.5 for non-blurry lines
                                          (.translate (.getContext canvas "2d") 0.5 0.5)
                                          ; calculate once: padding and other information for accurate mouse positions )
                                          (swap! canvas-state assoc :canvas canvas)
                                          (swap! canvas-state assoc :style-padding-top
                                                 (js/parseInt (.getPropertyValue (js/getComputedStyle canvas nil) "padding-top")))
                                          (swap! canvas-state assoc :style-padding-left
                                                 (js/parseInt (.getPropertyValue (js/getComputedStyle canvas nil) "padding-left")))
                                          (swap! canvas-state assoc :style-border-left
                                                 (js/parseInt (.getPropertyValue (js/getComputedStyle canvas nil) "border-left-width")))
                                          (swap! canvas-state assoc :style-border-top
                                                 (js/parseInt (.getPropertyValue (js/getComputedStyle canvas nil) "border-top-width")))
                                          (swap! canvas-state assoc :style-padding-right
                                                 (js/parseInt (.getPropertyValue (js/getComputedStyle canvas nil) "padding-right")))
                                          (swap! canvas-state assoc :style-padding-bottom
                                                 (js/parseInt (.getPropertyValue (js/getComputedStyle canvas nil) "padding-bottom")))
                                          (swap! canvas-state assoc :padding-width (+ (:style-padding-left @canvas-state)
                                                                                      (:style-padding-right @canvas-state)))
                                          (swap! canvas-state assoc :padding-height (+ (:style-padding-top @canvas-state)
                                                                                       (:style-padding-bottom @canvas-state)))
                                          (swap! canvas-state assoc :html (.-parentNode (.-body js/document)))
                                          (swap! canvas-state assoc :html-top (.-offsetTop (:html @canvas-state)))
                                          (swap! canvas-state assoc :html-left (.-offsetLeft (:html @canvas-state)))))

                                  :component-render (fn [state filter-state]
                                                        (component-render-fn state filter-state @dom-node canvas-state))
                                  :update-condition update?
                                  })));  }}}
(defn hoverable-and-clickable-canvas [width state filter-state canvas-state];  {{{
  [:canvas (if (not (:has-result @state))
             {:style {:display "none"}}
             {:width width
              :on-click (fn [e]
                          (let [idx (:hovered-index @state)]
                            (if (not (nil? idx))
                              (let [item (nth (:result @state) idx)]
                                (swap! filter-state assoc :borough (:name item))))))
              :on-mouse-move
              (fn [event]
                (let [[mx my]  (get-canvas-position event canvas-state)
                      xs       (map vector (:positions @canvas-state) (range))
                      filtered (filter (fn [[[x1 y1 x2 y2] idx]]
                                         (and (>= my y1) (<= my y2)))
                                       xs)
                      [_ idx]   (first filtered)]
                  ;(debug "setting hover to idx: " idx)
                  (swap! state assoc :hovered-index idx)))
              :on-mouse-out #(swap! state assoc :hovered-index nil)
              :class (if (nil? (:hovered-index @state)) "" "hovered")
              })]);  }}}
;;
;; Season Component
;;
(defn season-component [filter-state];  {{{
  (let [url (str (service-url) "/rpc/stats_season_cached_by_filter_accidents?select=year,month,count&order=year,month")]
    (canvas-base-filtered-component {:name "season-component"
                                     :filter-state filter-state
                                     :canvas-state (atom {:positions (vector)})
                                     :state (reagent/atom {:has-result false
                                                           :url url
                                                           :result (list)
                                                           :last-filter-state @filter-state})
                                     :update-state-on-load (fn [canvas-state state filter-state response]
                                                             (reset! state {:result response
                                                                            :url url
                                                                            :last-filter-state @filter-state
                                                                            :has-result true})
                                                             ; reset existing shape positions
                                                             (swap! canvas-state assoc :positions (vector)))
                                     :on-draw vertical-barchart
                                     :draw-options (let [padded-month (fn [x] (if (< x 10) (str "0" x) x))]
                                                     (default-vertical-barchart-options
                                                       (fn [x]
                                                         (+ (:year x) (padded-month (:month x))))
                                                       (fn [x]
                                                         (str (:year x) "-" (padded-month (:month x))))
                                                       :count
                                                       -60))
                                     :canvas-at (fn [dom-node] (.-nextSibling (.-firstChild dom-node)))
                                     :component-render
                                     (fn [state filter-state dom-node canvas-state]
                                       [:div#season.with-canvas
                                        [:h2 "Timeline"]
                                        (hoverable-and-clickable-canvas (* 0.9 (.-clientWidth (.-body js/document))) state filter-state canvas-state)
                                        ])})));  }}}

;;
;; Year Component
;;
(defn year-component [filter-state];  {{{
    (let [url (str (service-url) "/rpc/stats_year_cached_by_filter_accidents?select=year,count&order=year")]
        (canvas-base-filtered-component {:name "year-component"
                                         :filter-state filter-state
                                         :canvas-state (atom {:positions (vector)})
                                         :state (reagent/atom {:has-result false
                                                               :url url
                                                               :result (list)
                                                               :last-filter-state @filter-state})
                                         :update-state-on-load (fn [canvas-state state filter-state response]
                                                                 (reset! state {:result response
                                                                                :url url
                                                                                :last-filter-state @filter-state
                                                                                :has-result true})
                                                                 ; reset existing shape positions
                                                                 (swap! canvas-state assoc :positions (vector)))
                                         :on-draw vertical-barchart
                                         :draw-options (default-vertical-barchart-options (fn [x] (:year x)) (fn [x] (str (:year x))) :count -45)
                                         :canvas-at (fn [dom-node] (.-nextSibling (.-firstChild dom-node)))
                                         :component-render
                                         (fn [state filter-state dom-node canvas-state]
                                           [:div#year.with-canvas
                                            [:h2 "Year"]
                                            (hoverable-and-clickable-canvas (/ (.-clientWidth (.-body js/document)) 8) state filter-state canvas-state)
                                            ])})));  }}}

;;
;; Month Component
;;
(defn month-component [filter-state];  {{{
    (let [url (str (service-url) "/rpc/stats_month_cached_by_filter_accidents?select=month,count&order=month")]
      (canvas-base-filtered-component {:name "month-component"
                                       :filter-state filter-state
                                       :canvas-state (atom {:positions (vector)})
                                       :state (reagent/atom {:has-result false
                                                             :url url
                                                             :result (list)
                                                             :last-filter-state @filter-state})
                                       :update-state-on-load (fn [canvas-state state filter-state response]
                                                               (reset! state {:result response
                                                                              :url url
                                                                              :last-filter-state @filter-state
                                                                              :has-result true})
                                                               ; reset existing shape positions
                                                               (swap! canvas-state assoc :positions (vector)))
                                       :on-draw vertical-barchart
                                       :draw-options (default-vertical-barchart-options (fn [x] (:month x)) (fn [x] (str (:month x))) :count)
                                       :canvas-at (fn [dom-node] (.-nextSibling (.-firstChild dom-node)))
                                       :component-render
                                       (fn [state filter-state dom-node canvas-state]
                                         [:div#month.with-canvas
                                          [:h2 "Month"]
                                          (hoverable-and-clickable-canvas (/ (.-clientWidth (.-body js/document)) 4) state filter-state canvas-state)
                                          ])})
        ));  }}}

;;
;; Weekday Component
;;
(defn weekday-component [filter-state];  {{{
  (let [url (str (service-url) "/rpc/stats_weekday_cached_by_filter_accidents?select=name,count")]
    (canvas-base-filtered-component {:name "weekday-component"
                                     :filter-state filter-state
                                     :canvas-state (atom {:positions (vector)})
                                     :state (reagent/atom {:has-result false
                                                           :url url
                                                           :result (list)
                                                           :last-filter-state @filter-state})
                                     :update-state-on-load (fn [canvas-state state filter-state response]
                                                             (reset! state {:result response
                                                                            :url url
                                                                            :last-filter-state @filter-state
                                                                            :has-result true})
                                                             ; reset existing shape positions
                                                             (swap! canvas-state assoc :positions (vector))
                                                             )
                                     :on-draw sample-draw-chart ;horizontal-barchart
                                     :draw-options (default-horizontal-barchart-options)
                                     :canvas-at (fn [dom-node]
                                                  (.-nextSibling (.-firstChild dom-node)))
                                     :component-render
                                     (fn [state filter-state dom-node canvas-state]
                                       [:div#weekday.with-canvas
                                        [:h2 "Weekdays"]
                                        (hoverable-and-clickable-canvas (/ (.-clientWidth (.-body js/document)) 5) state filter-state canvas-state)
                                        ;(if (:has-result @state)
                                          ;[:ul
                                           ;(for [item (:result @state)]
                                             ;^{:key item} [:li (:name item) ": " (:count item)])])
                                        ])})));  }}}

;;
;; Hour Component
(defn hour-component [filter-state];  {{{
    (let [url (str (service-url) "/rpc/stats_hour_cached_by_filter_accidents?select=hour,count")]
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
    (let [url (str (service-url) "/rpc/stats_intersection_cached_by_filter_accidents?select=name,count&limit=25")]
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
    (let [url (str (service-url) "/rpc/stats_off_street_cached_by_filter_accidents?select=name,count&limit=25")]
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
(defn casualty-component [filter-state];  {{{
    (let [url (str (service-url) "/rpc/stats_casualties_cached_by_filter_accidents")]
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
                                                                (swap! app-state assoc :filtered-result (:count x))
                                                                ; filtered result is based on the user's casualty type selection
                                                                ;(swap! app-state assoc :filtered-result
                                                                       ;(case (:casualty-type @filter-state)
                                                                           ;"persons_injured" (:total-number-persons-injured @state)
                                                                           ;"persons_killed"  (:total-number-persons-killed @state)
                                                                           ;"pedestrians_injured" (:total-number-pedestrians-injured @state)
                                                                           ;"pedestrians_killed"  (:total-number-pedestrians-killed @state)
                                                                           ;"cyclist_injured"     (:total-number-cyclist-injured @state)
                                                                           ;"cyclist_killed"      (:total-number-cyclist-killed @state)
                                                                           ;"motorist_injured"    (:total-number-motorist-injured @state)
                                                                           ;"motorist_killed"     (:total-number-motorist-killed @state)
                                                                           ;(:count @state)))
                                                                ))
                                  :component-render (fn [state filter-state]
                                                        [:div#casualty-component
                                                         ;[:h2 "Casualties"]
                                                         (if (:has-result @state)
                                                             [:ul
                                                              [:li.total [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type nil)}
                                                               [:div.value (int-comma (:count @state))]
                                                               [:div.label "Accidents In Total"]]]
                                                              [:li.persons [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type "persons_injured")}
                                                               [:div.value (int-comma (:total-number-persons-injured @state))]
                                                               [:div.label "Persons Injured"]]]
                                                              [:li.persons [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type "persons_killed")}

                                                               [:div.value (int-comma (:total-number-persons-killed @state))]
                                                               [:div.label "Persons Killed"]]]
                                                              [:li.motorist [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type "motorist_injured")}
                                                               [:div.value (int-comma (:total-number-motorist-injured @state))]
                                                               [:div.label "Motorist Injured"]]]
                                                              [:li.motorist [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type "motorist_killed")}
                                                               [:div.value (int-comma (:total-number-motorist-killed @state))]
                                                               [:div.label "Motorist Killed"]]]
                                                              [:li.cyclist [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type "cyclist_injured")}
                                                               [:div.value (int-comma (:total-number-cyclist-injured @state))]
                                                               [:div.label "Cyclist Injured"]]]
                                                              [:li.cyclist [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type "cyclist_killed")}
                                                               [:div.value (int-comma (:total-number-cyclist-killed @state))]
                                                               [:div.label "Cyclist Killed"]]]
                                                              [:li.pedestrians [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type "pedestrians_injured")}
                                                               [:div.value (int-comma (:total-number-pedestrians-injured @state))]
                                                               [:div.label "Pedestrians Injured"]]]
                                                              [:li.pedestrians [:a {:href "#" :on-click #(swap! filter-state assoc :casualty-type "pedestrians_killed")}
                                                               [:div.value (int-comma (:total-number-pedestrians-killed @state))]
                                                               [:div.label "Pedestrians Killed"]]]
                                                              ])]
                                                        )})));  }}}

;;
;; Borough Component
;;
(defn borough-component [filter-state];  {{{
    (let [url (str (service-url) "/rpc/stats_borough_cached_by_filter_accidents?select=name,count")]
        (canvas-base-filtered-component {:name "borough-component"
                                         :filter-state filter-state
                                         :canvas-state (atom {:positions (vector)})
                                         :state (reagent/atom {:has-result false
                                                               :result (list)
                                                               :url url
                                                               :last-filter-state @filter-state})
                                         :update-state-on-load (fn [canvas-state state filter-state response]
                                                                   (reset! state {:result response
                                                                                  :url url
                                                                                  :last-filter-state @filter-state
                                                                                  :has-result true})
                                                                   ; reset existing shape positions
                                                                   (swap! canvas-state assoc :positions (vector)))
                                         :on-draw sample-draw-chart ;horizontal-barchart
                                         :draw-options (default-horizontal-barchart-options)
                                         :canvas-at (fn [dom-node]
                                                        (.-nextSibling (.-firstChild dom-node)))
                                         :component-render
                                         (fn [state filter-state dom-node canvas-state]
                                             [:div#boroughs.with-canvas
                                              [:h2 (if (:borough @filter-state)
                                                     (str "Boroughs = " (:borough @filter-state))
                                                     "Boroughs")]
                                              (hoverable-and-clickable-canvas (/ (.-clientWidth (.-body js/document)) 5) state filter-state canvas-state)
                                              ;(if (:has-result @state)
                                              ;[:ul
                                              ;(for [item (:result @state)]
                                              ;^{:key item} [:li (:name item) ": " (:count item)])])
                                              ])})));  }}}

;;
;; Factor Component
;;
(defn factor-component [filter-state];  {{{
    (let [url (str (service-url) "/rpc/stats_factors_cached_by_filter_accidents?select=name,count")]
        (canvas-base-filtered-component {:name "factor-component"
                                  :filter-state filter-state
                                  :canvas-state (atom {:hovering false
                                                       :positions (vector)})
                                  :state (reagent/atom {:has-result false
                                                        :result (list)
                                                        :url url
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [canvas-state state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :on-draw horizontal-barchart
                                  :canvas-at (fn [dom-node]
                                                 (.-nextSibling (.-firstChild dom-node)))
                                  :component-render (fn [state filter-state dom-node canvas-state]
                                                        [:div#factors.with-canvas
                                                         [:h2 "Factors"]
                                                         [:canvas (if (not (:has-result @state))
                                                                      {:style {:display "none"}}
                                                                      {:width  "800px"
                                                                       :height "1000px"})]
                                                         ;(if (:has-result @state)
                                                             ;[:ul
                                                              ;(for [item (:result @state)]
                                                                  ;^{:key item} [:li (:name item) ": " (:count item)])])
                                                         ]
                                                        )})));  }}}

;;
;; Vehicle Type Component
;;
(defn vehicle-type-component [filter-state];  {{{
    (let [url (str (service-url) "/rpc/stats_vehicle_types_cached_by_filter_accidents?select=name,count")]
        (canvas-base-filtered-component {:name "vehicle-type-component"
                                  :filter-state filter-state
                                  :canvas-state (atom {:hovering false
                                                       :positions (list)})
                                  :state (reagent/atom {:has-result false
                                                        :result (vector)
                                                        :url url
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [canvas-state state filter-state response]
                                                            (reset! state {:result response
                                                                           :url url
                                                                           :last-filter-state @filter-state
                                                                           :has-result true}))
                                  :on-draw horizontal-barchart
                                  :canvas-at (fn [dom-node]
                                                 (.-nextSibling (.-firstChild dom-node)))
                                  :component-render (fn [state filter-state dom-node canvas-state]
                                                        [:div
                                                         [:h2 "Vehicle Types"]
                                                         [:canvas (if (not (:has-result @state))
                                                                      {:style {:display "none"}}
                                                                      {:width  "800px"
                                                                       :height "1000px"})]
                                                         ;(if (:has-result @state)
                                                             ;[:ul
                                                              ;(for [item (:result @state)]
                                                                  ;^{:key item} [:li (:name item) ": " (:count item)])])
                                                         ]
                                                        )})));  }}}

(defn cluster-component [filter-state];  {{{
    (let [url (str (service-url) "/rpc/stats_cluster_cached_by_filter_accidents?cluster_size=eq.25m&limit=10")]
        (base-filtered-component {:name "cluster-component"
                                  :filter-state filter-state
                                  :state (reagent/atom {:has-result false
                                                        :result nil
                                                        :url (str url "&order=accident_count.desc")
                                                        :order-by "&order=accident_count.desc"
                                                        :last-filter-state @filter-state})
                                  :update-state-on-load (fn [state filter-state response]
                                                            (reset! state {:result response
                                                                           :url (str url (:order-by @state))
                                                                           :last-filter-state @filter-state
                                                                           :order-by (:order-by @state)
                                                                           :has-result true}))
                                  :component-render (fn [state filter-state]
                                                        [:div
                                                         [:h2 "Clusters (Top 10)"]
                                                         ;[:select {:value (:order-by @state) :on-change (fn [e]
                                                                                                            ;(swap! state assoc :order-by (-> e .-target .-value)))}
                                                          ;[:option {:value "&order=accident_count.desc"}"by accident count"]
                                                          ;[:option {:value "&order=total_number_persons_injured.desc"}"by persons injured"]
                                                          ;[:option {:value "&order=total_number_persons_killed.desc"}"by persons killed"]
                                                          ;[:option {:value "&order=total_number_cyclist_injured.desc"}"by cyclist injured"]
                                                          ;[:option {:value "&order=total_number_cyclist_killed.desc"}"by cyclist killed"]
                                                          ;[:option {:value "&order=total_number_pedestrians_injured.desc"}"by pedestrians injured"]
                                                          ;[:option {:value "&order=total_number_pedestrians_killed.desc"}"by pedestrians killed"]
                                                          ;[:option {:value "&order=total_number_motorist_injured.desc"}"by motorist injured"]
                                                          ;[:option {:value "&order=total_number_motorist_killed.desc"}"by motorist killed"]
                                                          ;]
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
                       (debug "handle intersection!")
                       (= ftype "off street")
                       (debug "handle off street")
                       (= ftype "factor")
                       (debug "handle factor")
                       (= ftype "borough")
                       (debug "handle borough")
                       (= ftype "vehicle_type")
                       (debug "handle vehicle type")
                       :else (debug "Unknown filter: " ftype))
                 (debug {:text ftext :type ftype} " in controller!")
                 (recur))))

(defn body-component [state]
    (let [filter-state (reagent/cursor state [:filters])]
        [:div
         [autocomplete-component filter-state]
         [casualty-component filter-state]
         [:div.flex-row
          [season-component filter-state]
         ]
         [:div.flex-row
          [borough-component filter-state]
          [year-component filter-state]
          [month-component filter-state]
          [weekday-component filter-state]
          ]
         [cluster-component filter-state]
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
    (fn []
        [:div
         [header-component]
         [body-component state]
         [footer-component]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Initialize App

(defn dev-setup []
  (when debug?
    ;(enable-console-print!)
    ;(println "dev mode")
    ))

(defn reload []
  (reagent/render [page app-state]
                  (.getElementById js/document "app")))

(defn ^:export main []
  (dev-setup)
  (reload))
