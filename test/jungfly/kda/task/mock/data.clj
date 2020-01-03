(ns jungfly.kda.task.mock.data
  (:require [clojure.tools.logging :as log]
            [camel-snake-kebab.core :as csk]
            [cheshire.core :as json]))

(defn parse[d]
  (json/parse-string (json/generate-string d) (fn[x] (keyword (csk/->camelCase x))) )
  )

(defn picture-update []
  {
       :OPTYPE         "SQL COMPUPDATE",
       :EVENTTABLE     "ADLOAD.PICTURES",
       :EVENTTIMESTAMP "2020-01-02 19:53:16.015448",
       :LAG            "5",
       :PICTURE_ID     "696391171",
       :VEHICLE_ID     "123456789",
       :PRIMARY        "0",
       :IS_HIDDEN      "1",
       :FILE_PATH      "98lGS3sJenIuoUpEypAjI9p9Jr759N1XsSs84GBp9N8.jpg",
       :ACTIVE         "1"
   })



(defn vehicle-update[]
  {
   :OPTYPE "SQL COMPUPDATE",
   :EVENTTABLE "ADLOAD.VEHICLES",
   :EVENTTIMESTAMP "2020-01-02 18:09:38.039809",
   :LAG "9",
   :VEHICLE_ID "123456789",
   :VIN "1C4RJFBG8HC915190",
   :SYSTEM_ID "3",
   :VEHICLE_STATUS_ID "13",
   :YEAR "2017",
   :MAKE_NAME "JEEP",
   :MODEL_NAME "GRAND CHEROKEE-",
   :SERIES_NAME "4dr Limited 3.6L\r",
   :BODY_STYLE_NAME "",
   :ENGINE_NAME "",
   :DISPLACEMENT "",
   :CYLINDERS "",
   :TRANSMISSION "",
   :DRIVETRAIN "4WD",
   :INTERIOR_COLOR_DESCRIPTION "Black",
   :EXTERIOR_COLOR_DESCRIPTION "BLK",
   :VEHICLE_TYPE_ID "",
   :MILEAGE "15000",
   :LOCATION_ID "1265231",
   :CAR_GROUP_CONFIG_ID "1123",
   :COUNTRY_ID "1",
   :SELLER_ORGANIZATION_ID "394466",
   :ASSET_TYPE_ID "",
   :UNIT_OF_MEASURE_ID ""
   })

(defn auction-update[]
  {
   :OPTYPE "SQL COMPUPDATE",
   :EVENTTABLE "ADLOAD.VEHICLES",
   :EVENTTIMESTAMP "2020-01-02 18:09:38.039809",
   :LAG "9",
   :VEHICLE_ID "123456789",
   :VIN "1C4RJFBG8HC915190",
   :SYSTEM_ID "3",
   :VEHICLE_STATUS_ID "13",
   :YEAR "2017",
   :MAKE_NAME "JEEP",
   :MODEL_NAME "GRAND CHEROKEE-",
   :SERIES_NAME "4dr Limited 3.6L\r",
   :BODY_STYLE_NAME "",
   :ENGINE_NAME "",
   :DISPLACEMENT "",
   :CYLINDERS "",
   :TRANSMISSION "",
   :DRIVETRAIN "4WD",
   :INTERIOR_COLOR_DESCRIPTION "Black",
   :EXTERIOR_COLOR_DESCRIPTION "BLK",
   :VEHICLE_TYPE_ID "",
   :MILEAGE "15000",
   :LOCATION_ID "1265231",
   :CAR_GROUP_CONFIG_ID "1123",
   :COUNTRY_ID "1",
   :SELLER_ORGANIZATION_ID "394466",
   :ASSET_TYPE_ID "",
   :UNIT_OF_MEASURE_ID ""
   })
(defn vehicle-addtional-info-update[]
  {
   :OPTYPE "SQL COMPUPDATE"
   :EVENTTABLE "ADLOAD.VEHICLE_ADDITIONAL_INFOS"
   :EVENTTIMESTAMP "2020-01-02 18:20:21.039755"
   :LAG "5"
   :VEHICLE_ID "123456789"
   :CHROME_STYLE_ID ""
   :CHROME_MAKE_NAME "Chevrolet"
   :CHROME_MODEL_NAME "Malibu"
   :CHROME_TRIM_NAME "LT w/1LT"
   :CHROME_YEAR "2010"
   :IS_READY ""
   :AMS_LOT_NUMBER "A"
   :RL_LB_CONDITION_TYPE_ID "2"
   :IS_DRIVEABLE ""
   :NUMBER_OF_KEYS ""
   :NUMBER_OF_KEY_FOBS ""
   :NUMBER_OF_BOOK_MANUALS ""
   :PSI_OPTION ""
   })
