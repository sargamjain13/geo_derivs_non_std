## The following script compiles the entire database for evaluating
## the impact of introduction of futures contracts on farmers.
## The period of analysis is Oct, 2003 to Oct, 2016. The commodities
## set covered in the analysis include:
## 1. RM seed
## 2. Soybean
## 3. Turmeric
## 4. Maize
## 5. Coriander (Dhaniya)
## 6. Cummin (jeera)

## The market prices used for the purpose of the analysis include:
## 1. Future market prices
## 2. Mandi prices
## 3. Farm-gate prices

#####################
## Input variables ##
#####################
## If boot toggle is TRUE, it allows following data extraction
## processes to run again:
## 1. Futures data
## 2. Mandi data
## 3. Distance of all mandis from all basis centers for all commodities
## If FALSE, load .rda files saved in DATA/POLICY_PAPER repository

boot <- FALSE
data_path <- "../../DATA/POLICY_PAPER/"
                                        # COMMODITY NAMES 
fut_commodities <- c("DHANIYA", "JEERA", "MAIZ", "RMSEED", "SYBEAN",
                     "TMCFGR")
spot_commodities <- c("CORIANDER", "CUMMIN", "MAIZE", "MUSTARD",
                      "SOYBEAN", "TURMERIC")
                                        # BASIS CENTERS
basis_centers <- data.frame("commodity" = c("coriander", "cummin",
                                            "maize", "mustard", "soybean",
                                            "turmeric"),
                            "basis_c" = c("Kota", "Unjha",
                                          "Nizamabad", "Jaipur", "Indore",
                                          "Nizamabad"),
                            "basis_state" = c("Rajasthan", "Gujarat",
                                              "Andhra Pradesh", "Rajasthan",
                                              "Madhya Pradesh", "Andhra Pradesh"))
                                        # POLLING CENTERS
polling_centers <- read.csv(
    file = paste0(data_path,
        "commodity_contract_specs_pollingcenters.csv"),
    header = T)
                                        # DELIVERY CENTERS
delivery_centers <- read.csv(
    file = paste0(data_path,
        "commodity_contract_specs_deliverycenters.csv"),
    header = T)

####################
## Libraries used ##
####################
## library(gmapsdistance)
library(stringr)

###################
## Files sourced ##
###################
source("compute_distance.R")
source("../TECHNICAL_PAPER/validations.R")
source("../TECHNICAL_PAPER/extractData.R")
source("../TECHNICAL_PAPER/extractContracts.R")

#############################################
## Data extraction: Mandi and Futures data ##
#############################################
## For boot = FALSE, .rda files with futures data and mandi data
## exists in DATA/POLICY_PAPER repository for all commodities.
if(boot == T){                            # MANDI DATA
    source("extract_mandi_prices.R")
    save(mandidata, file = "../../DATA/POLICY_PAPER/mandidata.rda")
                                        # FUTURES DATA
    futdata <- lapply(fut_commodities, function(x){
       print(paste("EXTRACTING FUTURES DATA FOR ", x))
       tmp <- extractFuturesData(x)
       futuresContracts <- unique(tmp$symbolFut)
       each_contract <- lapply(futuresContracts, function(y){
         print(y)
         contract <- tmp[tmp$symbolFut == y, ]
         ans <- extractNearContract(contract)
         return(ans)
       })
       names(each_contract) <- futuresContracts
       return(each_contract)
     })
    names(futdata) <- spot_commodities
    save(futdata, file = "../../DATA/POLICY_PAPER/futdata.rda")
} else {
    cat("\n >> Extracting mandi data for every commodity. \n")
    load("../../DATA/POLICY_PAPER/mandidata.rda", verbose = T)
    cat("\n >> Extracting futures data for every commodity. \n")
    load("../../DATA/POLICY_PAPER/futdata.rda", verbose = T)
}

cat("\n >> Extracting relevent near-month futures contracts for every commodity. \n")
relevent_futures_contracts <- lapply(futdata, function(x){
     contractnames <- names(x)
     if(contractnames[1] %in% c("DHANIYA", "JEERAUNJHA", "TMCFGRNZM")){
         contract <- x[[contractnames]]
     }
     if(substr(contractnames[1], 1, 4) == "MAIZ"){
         contract <- rbind(x[["MAIZE"]], x[["MAIZEKHRF"]])
     }
     if(substr(contractnames[1], 1, 6) == "RMSEED"){
         contract <- rbind(x[["RMSEEDJPR"]], x[["RMSEED"]])
     }
     if(substr(contractnames[1], 1, 6) == "SYBEAN"){
         contract <- x[["SYBEANIDR"]]
     }
     return(contract)
 })
                                          

#####################################
## Distance from all basis centers ##
#####################################
## For boot = FALSE, .rda files with mandi-distance data along with
## prices and volumes exists in DATA/POLICY_PAPER repository for all
## commodities. Computation of distance is limited to 2500 queries
## per day.

if(boot == T){    
    commodity_distance <- lapply(spot_commodities, function(spot){
     print(paste0("Analysis for ", spot))
                                        # Spot market address
     spot_data <- mandidata[[spot]]
     spot_data$address <- paste0(spot_data$market, ", ",
                                 spot_data$state)
     uniqueAddress <- unique(spot_data$address)
                                        # Picking polling centers
     polling_center_state <- pick_polling_center_state(spot)
     for(i in 1:nrow(polling_center_state)){
         spot_data$tmp_basis_address <- polling_center_state$address[i]
         colnames(spot_data)[match(
             "tmp_basis_address",
             colnames(spot_data))] <- paste0("address_polling_center_",
                                             i)
     }
                                        # Picking delivery centers
     delivery_center_state <- pick_delivery_center_state(spot)
     delivery_center_state <- delivery_center_state[!(delivery_center_state$Delivery_C %in% polling_center_state$Basis_C), ]
     for(i in 1:nrow(delivery_center_state)){
         spot_data$tmp_delivery_address <- delivery_center_state$address[i]
         colnames(spot_data)[match(
             "tmp_delivery_address",
             colnames(spot_data))] <- paste0(
                 "address_delivery_center_",
                 i)
     }
     noOfPolls <- colnames(spot_data)[grep("address_polling_center_",
                                           colnames(spot_data))]
     noOfDelivery <- colnames(spot_data)[grep("address_delivery_center_",
                                           colnames(spot_data))]
                                        # Computing distance w.r.t
                                        # each polling center
     for(x in noOfPolls){
         print(x)
         uniquePollAddress <- unique(eval(parse(text = paste0(
                                                    "spot_data$",
                                                    x))))
         polldistance <- distance_calculator(
             origin_p = uniqueAddress,
             destination_p = uniquePollAddress)
         
         spot_data$tmp_distance <- rep(NA, nrow(spot_data))
         for(i in 1:nrow(polldistance)){
             spot_data$tmp_distance[spot_data$address %in% polldistance$origin[i]] <- polldistance$distance[i]
         }
         colnames(spot_data)[match(
             "tmp_distance",
             colnames(spot_data))] <- paste0(
                 "distance_polling_center_",
                 strsplit(x, "_")[[1]][4])
     }
                                        # Computing distance w.r.t
                                        # each delivery center
     for(x in noOfDelivery){
         print(x)
         uniqueDeliveryAddress <- unique(eval(parse(text = paste0(
                                                    "spot_data$",
                                                    x))))
         deliverydistance <- distance_calculator(
             origin_p = uniqueAddress,
             destination_p = uniqueDeliveryAddress)

         spot_data$tmp_distance <- rep(NA, nrow(spot_data))
         for(i in 1:nrow(deliverydistance)){
             spot_data$tmp_distance[spot_data$address %in% deliverydistance$origin[i]] <- deliverydistance$distance[i]
         }
         colnames(spot_data)[match(
                     "tmp_distance",
             colnames(spot_data))] <- paste0(
                 "distance_delivery_center_",
                 strsplit(x, "_")[[1]][4])
     }
                                        # Saving the distance file for
                                        # each commodity
     save(spot_data,
          file = paste0("../../DATA/POLICY_PAPER/distance_mandi_data_",
              tolower(spot), "_dc1.rda"))
     return(NULL)
 })
} else {
  cat("\n >> Compiling mandi data and distances of mandis from polling and delivery centers. \n")
  distance_files <- list.files(
      "../../DATA/POLICY_PAPER/MANDI_DISTANCE_DATA/")
  mandidata <- lapply(distance_files, function(x){
      cat("\n", strsplit(x, "_")[[1]][3], "\n")
      load(paste0("../../DATA/POLICY_PAPER/MANDI_DISTANCE_DATA/",
                  x), verbose = T)
      tmp <- spot_data
      noOfCenters <- colnames(tmp)[grep("distance",
                                        colnames(tmp))]
      tmp[ , noOfCenters] <- tmp[ , noOfCenters] / 10^3
      return(tmp)
  })
  names(mandidata) <- spot_commodities
}




