rm(list = ls())
options(scipen = 999)

library(xts)
library(parallel)
library(sandwich)
library(xtable)
library(readstata13)
library(lmtest)
library(zoo)
library(dplyr)
library(data.table)

source("compile_database.R")

graph_path <- "../../../DOC/POLICY_PAPER/GRAPHS/"
commdetails <- data.frame(commname = spot_commodities,
                          futIntroDate = as.Date(
                              c("2008-08-11", "2005-02-03",
                                "2010-05-21", "2003-12-15",
                                "2003-12-15", "2004-07-24")))
commdetails <- commdetails[!(commdetails$commname == "CUMMIN"), ]

comm_major_states <- data.frame(
    commname = spot_commodities,
    state1 = c("MadhyaPradesh", "Rajasthan", "AndhraPradesh",
               "Rajasthan", "MadhyaPradesh", "AndhraPradesh"),
    state2 = c("Rajasthan", "Gujarat", "Karnataka", "MadhyaPradesh",
               "Maharashtra", "TamilNadu"),
    state3 = c("Gujarat", "MadhyaPradesh", "Maharashtra", "Haryana",
               "Rajasthan", "Maharashtra"))

## Changes in contracts: Include everything -- grade specifications etc.

## Major changes in the contract
contract_dummies <- data.frame(
  "commname" = commdetails$commname,
  "contract1_start" = c("2008-08-11", "2005-01-05", "2003-12-15", "2003-12-15", "2004-07-24"),
  "contract2_start"= c("2016-12-31", "2010-05-21", "2010-11-10", "2015-02-02", "2016-12-31"),
  "contract3_start" = c("2016-12-31", "2013-02-08", "2015-01-01", "2016-12-31", "2016-12-31"),
  "contract1_end" = c("2016-12-31", "2010-09-20", "2011-01-20", "2016-12-31", "2016-12-31"),
  "contract2_end"= c("2016-12-31", "2013-02-08", "2016-12-31", "2016-12-31", "2016-12-31"),
  "contract3_end"  = c("2016-12-31", "2016-12-31", "2015-12-11", "2016-12-31", "2016-12-31"))


#########################
## Read spot data sets ##
#########################
basedata <- lapply(spot_commodities, function(x){
    spot <- mandidata[[x]]
    futures <- relevent_futures_contracts[[x]]
    futures <- futures[ , c("tradeDate", "basisCentre",
                            "futpriceNear")] ## 15 days rollover?
    tmp <- merge(spot, futures,
                 by.x = "arrival_date",
                 by.y = "tradeDate",
                 all.x = TRUE)
    return(tmp)
})
names(basedata) <- spot_commodities

#######################################
## Function calls for preparing data ##
#######################################

## Create a master dataset that could be used across all the three
## hypotheses.

## A function for determining the start and end date of data for mandi
## declared as a center
countRowsCenter <- function(center_address, pdata){
    tmp <- pdata[pdata$address == center_address,]
    if (center_address == "Delhi, Delhi"){
        cat("No rows found for center address.", "\n")
        tmp <-  pdata[grep("Delhi", pdata$address),] ## Pick all mandis from delhi
    }

    tmp <- tmp[order(tmp$arrival_date),]
    stdate <- tmp[1, "arrival_date"]
    eddate <- tmp[nrow(tmp), "arrival_date"]
    obsCenter <- nrow(tmp)
    comm <- unique(pdata$commodity)
    allObs <- nrow(pdata)
    ans <- data.frame(comm,
                      center_address,
                      stdate,
                      eddate,
                      obsCenter,
                      allObs)
    return(ans)
}

## All polling centers are delivery centers
prepData <- function(commname, futIntroDate){
    pdata <- basedata[[commname]] ## Restricting the analysis till Dec, 2015
    pdata <- pdata[pdata$arrival_date <= as.Date("2015-12-31"), ] ## Removing junk observations
    pdata <- pdata[!is.na(pdata$arrival_date), ]
    ## pdata <- pdata[!is.na(pdata$arrivalstonnes), ] ## We should not do
    ## this. In case a commodity trades multiple varieties on a
    ## particular day, total arrivals are reported against the first variety. Rest is NA.
    
    pdata <- pdata[!is.na(pdata$minimumpricersquintal), ]
    pdata <- pdata[!is.na(pdata$maximumpricersquintal), ]
    pdata <- pdata[!is.na(pdata$modalpricersquintal), ]

    ## Discard data below and above certain percentile
                                        # Filter on prices
    pdata <- pdata[which(pdata$minimumpricersquintal > quantile(pdata$minimumpricersquintal, 0.005)
                         & pdata$minimumpricersquintal < quantile(pdata$minimumpricersquintal,
                                                                  0.995)), ]
                                        # Filter on arrivals 
    pdata <- pdata[which(pdata$arrivalstonnes > quantile(pdata$arrivalstonnes, 0.005, na.rm =TRUE) &
                         pdata$arrivalstonnes < quantile(pdata$arrivalstonnes, 0.995, na.rm = TRUE)), ]
    ## Calling function to clean discrepancy in mandi names
    pdata$address <- cleanMandiNames(pdata$address)
    ## Clean names of centers in data
    pdata <- cleanCenterNames(pdata)

    ## Compute a simple average of minimum prices in case of multiple
    ## varieties. This will yield only one row for each mandi commodity day.
    
    ddata <- split(pdata, pdata$arrival_date)
    
    pdata <- mclapply(ddata, mc.cores = 3, function(d){
        mdata <- split(d, d$address)
        tmp.mdata <- lapply(mdata, function(m){
            if (nrow(m) > 1){
                cat("Taking an average for mandi ", unique(m$address), "and date ", as.character(unique(m$arrival_date)), " nrow ", nrow(m), "\n")
                tmp <- m[!is.na(m$arrivalstonnes),]
                tmp$minimumpricersquintal <- mean(m$minimumpricersquintal)
                tmp$maximumpricersquintal <- mean(m$maximumpricersquintal)
                tmp$modalpricersquintal <- mean(m$modalpricersquintal)
                return(tmp)
            } else {
                cat("Only one variety traded for mandi ", unique(m$address), "and date ", as.character(unique(m$arrival_date)), " nrow ", nrow(m), "\n")
                return(m)
            }
        })
        tmp.mdata <- rbindlist(tmp.mdata)
        rm(mdata)
        return(tmp.mdata)
    })

    rm(ddata)
    pdata <- data.frame(rbindlist(pdata))
    
    ## Creating dummy for futures
    pdata$futures_intro <- ifelse(pdata$arrival_date < futIntroDate, 0, 1)
    
    if (any(pdata$address == "Ramgang Mandi, Rajasthan") == TRUE){
        pdata[which(pdata$address == "Ramgang Mandi, Rajasthan"), "address"] <- "Ramganjmandi, Rajasthan"
    }
    
    pdata$address <- gsub("RAJASTHAN", "Rajasthan", pdata$address)
    
    uniqueMandis <- unique(pdata$address)
    
    ## Find the nearest centre for every mandi:
    ncenters <- mclapply(uniqueMandis, mc.cores = 3, function(m){
        tmp <- pdata[pdata$address == m, ]
        ans <- t(unique(tmp[, grep("center_", colnames(tmp))]))
        
        ans <- data.frame("address" = ans[grep("address", rownames(ans))],
                          "distance" = ans[grep("distance", rownames(ans))])
        ans$address <- gsub("RULL", "Rajasthan", ans$address) ## This is an error arising out of one of your clean functions, Sargam. PLEASE CHECK.
        ans$address <- gsub("Davengere", "Davangere", ans$address)
        ans$address <- gsub("Warangal, Karnataka", "Warangal, Telangana", ans$address)
        ans$address <- gsub("Karimnagar, AndhraPradesh", "Karimnagar, Telangana", ans$address)
        ans$address <- gsub("Harayana", "Haryana", ans$address)
        ans$address <- gsub("Hisar", "Hissar", ans$address)
        ans$address <- gsub("Maundsaur", "Mandsaur", ans$address)
        ans$address <- gsub("RAJASTHAN", "Rajasthan", ans$address) 
        ans$address <- gsub("Nizamabad, AndhraPradesh", "Nizamabad, Telangana", ans$address) 
        
        ans$distance <- as.numeric(as.character(ans$distance))
        ans <- ans[order(ans$distance, decreasing = FALSE), ]
        if (nrow(ans) != 0){
            nearestCenter <- ans[1,]
            colnames(nearestCenter) <- c("minimum_distance_centeraddress",
                                         "minimum_distance")
            nearestCenter$address <- m
            return(nearestCenter)
        }
    })
    ncenters <- rbindlist(ncenters)
    ncenters <- data.frame(ncenters)
    if (length(ncenters[is.na(ncenters[ , 2]), "address"]) != 0){
        cat("Distance for nearest mandi not available for",
            ncenters[is.na(ncenters[ , 2]), "address"], "commodity: ", commname, "\n")
        ncenters <- ncenters[!(is.na(ncenters[ , 2])),]
    }
    pdata <- merge(pdata, ncenters, by = "address")
    pdata <- pdata[order(pdata$arrival_date),]

    ncenters$minimum_distance_centeraddress <- gsub("Sri Ganganagar", "SriGanganagar",
                                                    ncenters$minimum_distance_centeraddress)

    pdata$address <- gsub("Sri Ganganagar", "SriGanganagar", pdata$address)
    
    obs4center <- do.call(rbind, lapply(unique(ncenters[,"minimum_distance_centeraddress"]), function(j){
        print(j)
        countRowsCenter(center_address = j, pdata)
    }))
    
    ## Now pick the price for the nearest center..
    tmp <- mclapply(1:nrow(pdata), mc.cores = 3, function(p){
        print(p)
        tmp.p <- pdata[p, ]
        if (tmp.p$minimum_distance_centeraddress == "Delhi, Delhi"){
            tmp.ncenter <- pdata[grep("Delhi", pdata$address),] ## Pick all mandis from delhi
        } else {
            ## Pick the price of the nearest cenetr
            tmp.ncenter <- pdata[pdata$address == tmp.p$minimum_distance_centeraddress, ]
        }
        tmp.ncenter <- tmp.ncenter[order(tmp.ncenter$arrival_date),]
        tmp.ncenter$currdate <- tmp.p$arrival_date
        tmp.ncenter$diffdate <- tmp.ncenter$arrival_date - tmp.ncenter$currdate
        tmp.ncenter <- tmp.ncenter[tmp.ncenter$diffdate <= 0, ]
        if (nrow(tmp.ncenter) == 0){
            ## Discuss the issue below.
            cat ("Nearest center",
                 as.character(tmp.p$minimum_distance_centeraddress),
                 "data not available for date prior or equal to",
                 as.character(tmp.p$arrival_date), "\n")
            tmp.p$minimum_distance_centerprice <- NA
            return(tmp.p)
        }
        tmp.ncenter <- tmp.ncenter[which(tmp.ncenter$diffdate == max(tmp.ncenter$diffdate)),]
        if (unique(abs(as.numeric(tmp.ncenter$diffdate))) > 7){
            cat ("Price for the nearest center is stale -- beyond 7 days.",
                 "Details: center is ", tmp.ncenter$address,
                 " date is ",
                 as.character(tmp.ncenter$arrival_date),
                 " currdate is ",
                 as.character(tmp.ncenter$currdate),  ", \n")
            ## Notes2self: several obs getting missed out because
            ## of this constraint. Will probably increase it from
            ## 7 to 10 / 15 in the next version.
            tmp.p$minimum_distance_centerprice <- NA
            return(tmp.p)
        }
        
        if (nrow(tmp.ncenter) > 1){
            tmp.ncenter <- tmp.ncenter[which(tmp.ncenter$arrivalstonnes == max(tmp.ncenter$arrivalstonnes)),]
            
            if (nrow(tmp.ncenter) > 1){
                print(unique(tmp.ncenter[,3]))
                cat("Nrow tmp.ncenter greater than 1 for date: ",
                    tmp.p$arrival_date, ", observation ", p, "\n")
                
                tmp.ncenter <- tmp.ncenter[1, ]  ## This issue arises
                                                 ## when there are
                                                 ## subyards trading
                                                 ## the same day. Example is Ramganj mandi.
                
                ## Notes2self: Take unique rows in the regression.
                
            }
        }
        tmp.p$minimum_distance_centerprice <- tmp.ncenter$minimumpricersquintal
        return(tmp.p)
    })
    
    tmp <- rbindlist(tmp)
    tmp$price_diff <- (tmp$minimumpricersquintal - tmp$minimum_distance_centerprice)*100/tmp$minimum_distance_centerprice
    ## Notes2self: Remove center as the reference price in final
    ## regressions. Or do both ways -- with the center and without.
    
    tmp$year <- ifelse(format(tmp$arrival_date, "%m") %in% c("01", "02",
                                                             "03", "04",
                                                             "05", "06"),
                       as.numeric(format(tmp$arrival_date, "%Y")),
                       as.numeric(format(tmp$arrival_date, "%Y")) + 1)
    return(list(tmp, obs4center))
}

## Read supplementary data:

########################################
## Create basis/delivery center dummy ##
########################################
create_center_dummy <- function(fdata, commdetails){
    x <- data.frame(fdata)
    commname <- as.character(unique(x$commodity))
    print(commname)
    futIntroDate <- commdetails$futIntroDate[commdetails$commname == commname]
    cat("\nCreating center dummies for", commname, "\n")  
    x$center_dummy <- 0
    predata <- x[x$arrival_date < futIntroDate, ]
    postdata <- x[x$arrival_date >= futIntroDate, ]
    ## List of polling/distance centers for specific commodity
    pc <- polling_centers[toupper(polling_centers$Commodity) %in%
                          commname, ]
    dc <- delivery_centers[toupper(delivery_centers$Commodity) %in%
                           commname, ]
    colnames(dc) <- colnames(pc) <- c("commodity", "center", "state", "from", "to")
    ## centers <- rbind(pc, dc[!(dc$center %in% pc$center), ]) #
    ## this will not work in case a center was made a dc /pc
    ## multiple times.
    centers <- unique(rbind(pc, dc))
    centers$commodity <- as.character(centers$commodity)
    centers$center <- as.character(centers$center)
    centers$state <- as.character(centers$state)
    centers$state <- gsub(" ", "", centers$state)
    centers$to <- gsub("-04-31", "-04-30", centers$to) 
    centers$from <- as.Date(as.character(centers$from))
    centers$to <- as.Date(as.character(centers$to))
    
    ## Bug fixes from stuff imported from compute_distance.R)
    
    centers$center <- gsub("Davengere", "Davangere", centers$center)
    if (any(centers$center == "Warangal") == TRUE){
        centers$state[which(centers$center == "Warangal")] <- "Telangana"
    }
    if (any(centers$center == "Nizamabad") == TRUE){
        centers$state[which(centers$center == "Nizamabad")] <- "Telangana"
    }
    if (any(centers$center == "Karimnagar") == TRUE){
        centers$state[which(centers$center == "Karimnagar")] <- "Telangana"
    }
    
    centers$state <- gsub("Harayana", "Haryana", centers$state)
    centers$center <- gsub("Hisar", "Hissar", centers$center)
    centers$center <- gsub("Maundsaur", "Mandsaur", centers$center)
    centers$center <- gsub("Sri Ganganagar", "SriGanganagar", centers$center)
    
    ## Need to fix jaipur, jodhpur, bikaner, ramganj etc. -- TODO
    centers$address <- paste0(centers$center, ", ", centers$state)
    
    post_tmp <- mclapply(1:nrow(postdata), mc.cores = 3, function(i){
        cat(i, "/", nrow(postdata), "\n")
        center_existence <- centers[centers$address %in% postdata$minimum_distance_centeraddress[i], ]
        
        if (nrow(center_existence) > 1){
            existence_date <- simplify2array(sapply(1:nrow(center_existence), function(u){
                existence_date <- seq(from = center_existence$from[u],
                                      to = center_existence$to[u],
                                      by = 1)
                return(existence_date)
            }))
        } else {
            existence_date <- seq(from = center_existence$from,
                                  to = center_existence$to,
                                  by = 1)
        }
        
        postdata$center_dummy[i] <- ifelse(postdata$arrival_date[i] %in% existence_date,
                                           1, 0)
        return(postdata[i, ])
    })
    postdata <- rbindlist(post_tmp)
    x <- data.frame(rbindlist(list(predata, postdata)))
    x <- x[order(x$arrival_date), ]
    x <- x[, colnames(x) %in% c("address", "arrival_date",
                                "market", "state", "commodity",
                                "arrivalstonnes",
                                "minimumpricersquintal",
                                "maximumpricersquintal",
                                "modalpricersquintal", "variety",
                                "basisCentre", "futpriceNear",
                                "futures_intro",
                                "minimum_distance_centeraddress",
                                "minimum_distance",
                                "minimum_distance_centerprice",
                                "price_diff", "year", "center_dummy")]
    return(x)
}

##################################
## Computing mandi market share ##
##################################
compute_mandi_mkt_share <- function(fdata){
    x <- data.frame(fdata)
    x$commodity <- as.character(x$commodity)
    x$address <- as.character(x$address)
    x$state <- as.character(x$state)
    cat("\nComputing mandi market share for",
        unique(x$commodity)[1], "\n")  
    
    ## Adding a variable of number of mandis reporting each day --
    ## may want to use in the future.
    
    dateSplit <- split(x, x$arrival_date)
    ## rm(x)
    
    dateSplit <- mclapply(dateSplit, mc.cores = 3, function(d){
        d$numReportingMandis <- nrow(d)
        return(d)
        })
    
    dateSplit <- rbindlist(dateSplit)
    
    ## Market share needs to be computed over a longer period,
    ## certainly not at a daily level. This is because trading
    ## happens on specific days of the week. A small mandi which
    ## probably trades only on Saturday with no major mandis
    ## trading on that day will show up as a big mandi. Computing
    ## at an yearly level.
    
    yrSplit <- split(dateSplit, dateSplit$year)
    
    ## Compute total arrivals in an year, compute total arrivals
    ## of a mandi.
        rm(dateSplit)
    
    ## Stuck with Warangal, Turmeric year 2012.
    msdata <- lapply(yrSplit, function(y){
        print(unique(y$year))
        y <- unique(data.frame(y))
        totArrivals <- sum(y$arrivalstonnes)
        mandiSplit <- split(y, y$address)
        ms <- rbindlist(mclapply(mandiSplit, mc.cores = 3, function(m){
            return(data.frame(year = unique(m$year),
                              yearlyArrivals = sum(m$arrivalstonnes),
                              address = unique(m$address)))
        }))
        ms <- data.frame(ms)
        rm(mandiSplit)
        ms$marketShare <- ms$yearlyArrivals*100/totArrivals
        ms <- merge(data.frame(ms)[,c("address", "marketShare")],
                    data.frame(y),
                    by = "address")
        
        ## Now find the market share of each center
        
        ms <- rbindlist(mclapply(1:nrow(ms), mc.cores = 3, function(i){
            print(i)
                tmp <- ms[i,]
            if (is.na(tmp$minimum_distance_centerprice)){
                return(NULL)
            }
            if (tmp$minimum_distance_centeraddress == "Delhi, Delhi"){
                tmp.delhi <-  ms[grep("Delhi", ms$address), "marketShare"] ## Pick all mandis from delhi
                tmp$marketShare_center <- max(tmp.delhi)
            } else {
                tmp$marketShare_center <- unique(ms[which(ms$address == tmp$minimum_distance_centeraddress), "marketShare"])
            }
            return(tmp)
        }))
        ms <- data.frame(ms)
        return(ms)
    })
        
    msdata <- rbindlist(msdata)
    return(msdata)
}

combine_dist_codes <- function(fdata){
                                        # Dataset with district codes
    cat("\nCombining district codes data set with mandi data.", "\n")
    district_file <- read.dta13("../../DATA/POLICY_PAPER/201706mandis_with_districtcodes.dta")
    district_file <- district_file[ , c("stdistcode", "districtmatch",
                                        "statematch", "mandi")]
                                        # Cleaning state names in
                                        # district with code files
    district_file$statematch <- gsub(" ", "", district_file$statematch)
    district_file$statematch[district_file$statematch == "MP"] <-
        "MADHYAPRADESH"
    district_file$statematch[district_file$statematch == "TN"] <-
        "TAMILNADU"
    district_file$statematch[district_file$statematch == "UP"] <-
        "UTTARPRADESH"
    district_file$statematch[district_file$statematch == "WB"] <-
        "WESTBENGAL"
    district_file$statematch[district_file$statematch == "UTTARANCHAL"] <- "UTTRAKHAND"
    district_file$statematch[district_file$statematch == "KERELA"] <- "KERALA"
    district_file$stdistcode <- gsub(" ", "", district_file$stdistcode)
                                        # Creating matching unit
    district_file$matching_unit <- paste0(district_file$mandi, "+",
                                          district_file$statematch)
    district_file <- district_file[order(district_file$matching_unit),
                                   ]
                                        # Data set w/o district codes
    fdata <- data.frame(fdata)
    fdata$state[fdata$state == "Telangana"] <- "AndhraPradesh"
    fdata$matching_unit <- paste0(fdata$market, "+",
                                  toupper(fdata$state))
    new_data <- fdata[ , c("market", "state", "matching_unit")]
    new_data <- new_data[!duplicated(new_data$matching_unit), ]
    new_data <- new_data[order(new_data$matching_unit), ]
                                        # Merging the data set
    final_data <- merge(new_data, district_file[ , c("matching_unit",
                                                     "districtmatch",
                                                     "stdistcode")],
                        by = "matching_unit",
                        all.x = TRUE, all.y = FALSE)
    final_data$stdistcode <- as.character(final_data$stdistcode)
    rem_set <- final_data[is.na(final_data$stdistcode), ]
    final_data <- final_data[!is.na(final_data$stdistcode), ]

    missing_districts <- data.frame(
        "matching_unit" = c("Aizawl+MIZORAM", "Alathur+KERALA",
                            "Arreria+BIHAR", "Aruppukottai+TAMILNADU",
                            "Balasinor+GUJARAT", "Banswara+RAJASTHAN",
                            "Bara Bazar+MIZORAM",
                            "Bazar Atriya +CHATTISGARH",
                            "Begusarai+BIHAR",
                            "Bharthana+UTTARPRADESH", "Bihta+BIHAR",
                            "Birpur+BIHAR", "Charkhari+UTTARPRADESH",
                            "Dahivadi+MAHARASHTRA",
                            "Dahod(Himalaya)+GUJARAT",
                            "Danapur+BIHAR", "Darbhanga+BIHAR",
                            "Dhara+KERALA", "Dungarpur+RAJASTHAN",
                            "Forbesganj+BIHAR", "Gulabbagh+BIHAR",
                            "Jaggayyapeta+ANDHRAPRADESH",
                            "Jaipur(Grain)(Sodala)+RAJASTHAN",
                            "Jalalabad+UTTARPRADESH",
                            "Jammalamadugu+ANDHRAPRADESH",
                            "Kaswa+BIHAR", "Khagaria+BIHAR",
                            "Khuzama+NAGALAND", "Kishanganj+BIHAR",
                            "Lakhisarai+BIHAR", "Lathi+GUJARAT",
                            "Mangalagiri+ANDHRAPRADESH",
                            "Mau(Chitrakut)+UTTARPRADESH",
                            "Mohamadabad+UTTARPRADESH",
                            "Muzaffarpur+BIHAR",
                            "Naharlagun+ARUNACHALPRADESH",
                            "Narkatiaganj+BIHAR",
                            "Paderu,RBZ+ANDHRAPRADESH",
                            "Panthawada+GUJARAT",
                            "Pasighat+ARUNACHALPRADESH",
                            "Puwaha+UTTARPRADESH",
                            "Ramanathapuram(phase 3)+TAMILNADU",
                            "R and B Guest House,RBZ,Vizianagaram+ANDHRAPRADESH",
                            "Sajapur+MADHYAPRADESH",
                            "Sandi+UTTARPRADESH",
                            "Santrampur+GUJARAT",
                            "Shahabad+UTTARPRADESH",
                            "Sringeri+KARNATAKA",
                            "Tawang+ARUNACHALPRADESH",
                            "Thara(Shihori)+GUJARAT",
                            "Thungathurthy+ANDHRAPRADESH",
                            "Tiruchengode(Agri. Coop. Marketing Society)+TAMILNADU",
                            "Urai+UTTARPRADESH",
                            "Vellankovil+TAMILNADU",
                            "Vilathikulam+TAMILNADU",
                            "Virudhunagar+TAMILNADU"),
        "stdistcode" = c("1503", "3206", "1007", "3326", "2422",
                         "0828", "1503", "2209", "1020", "0931",
                         "1028", "1006", "0939", "2731", "2418",
                         "1028", "1013", "3208", "0827", "1007",
                         "1009", "2816", "0812", "0922", "2820",
                         "1009", "1021", "1307", "1008", "1025",
                         "2413", "2817", "0962", "0965", "1014",
                         "1204", "1001", "2813", "2402", "1208",
                         "0922", "3312", "2812", "2306", "0925",
                         "2418", "0925", "2917", "1201", "2402",
                         "3608", "3309", "0935", "3310", "3328",
                         "3326"))
    rem_set <- merge(rem_set[c("matching_unit", "market", "state",
                               "districtmatch")],
                     missing_districts, by = "matching_unit")
    final_data <- rbind(final_data, rem_set)
    final_data$stdistcode[final_data$districtmatch == "KANPUR"] <-
        "0910"
    final_data$stdistcode[final_data$districtmatch == "MUMBAI"] <-
        "2723"
    final_data$stdistcode[final_data$stdistcode == "36066"] <- "3606"
    final_data <- final_data[ , c("matching_unit", "stdistcode")]   
    final_data <- final_data[!is.na(final_data$stdistcode), ]
    ans <- mclapply(unique(final_data$matching_unit), mc.cores = 2, function(x){
        tmp <- fdata[fdata$matching_unit == x, ]
        tmp$stdistcode <- final_data$stdistcode[final_data$matching_unit == x][1]
        return(tmp)
    })
    ans <- rbindlist(ans)
    return(ans)
}

###################################
## Read million-plus cities data ##
###################################
## add_million_plus_cities_data <- function(fdata){
##     cat("\nCombining million-plus cities data with mandi data.", "\n")
##     mpc_data <- read.dta13(
##         "../../DATA/POLICY_PAPER/201706million_plus_cities.dta")
##     mpc_data <- mpc_data[ , c("statename", "districtname",
##                               "stdistcode", "millcities")]
##     tmp <- merge(fdata, mpc_data[ , c("stdistcode", "millcities")],
##                  by = "stdistcode", all.x = TRUE)
##     return(tmp)
## }

########################
## Read rainfall data ##
########################
add_rainfall_data <- function(fdata){
    cat("\nCombining rainfall data set with mandi data.", "\n")
    rain_data <- read.dta13("../../DATA/POLICY_PAPER/201706mthly_rainfallDist_NASA2001_16_withcodes.dta")
    rain_data <- rain_data[ , c("district", "state", "td",
                                "stdistcode", "avgprecipitation")]
    
    fdata <- data.frame(fdata)
    ans <- lapply(unique(rain_data$stdistcode), function(x){
        rain <- rain_data[rain_data$stdistcode == x, ]
        rain <- rain[order(rain$td), ]
        rain <- rain[!duplicated(rain$td), ]
        rain$yr_mnth <- substr(rain$td, 1, 7)
        rain$surplus <- NA
        rain$normal_surplus <- NA
                                        # Compute surplus and
                                        # normalised surplus
        for (i in 1:length(rain$yr_mnth)){
            avg_rain <- mean(rain$avgprecipitation[substr(rain$yr_mnth, 6, 7) ==
                                                   substr(rain$yr_mnth[i], 6, 7)])
            sd_rain <- sd(rain$avgprecipitation[substr(
                                   rain$yr_mnth, 6, 7) ==
                                   substr(rain$yr_mnth[i], 6, 7)])
            rain$surplus[i] <- rain$avgprecipitation[i] - avg_rain
            rain$normal_surplus[i] <- (rain$avgprecipitation[i] - avg_rain) / sd_rain
        }
        
        rain <- data.frame(
            rain, do.call(cbind, lapply(1:12, function(l){
                lag_val <- as.data.frame(c(rep(NA, l), rain$surplus))
                lag_val <- lag_val[1:(nrow(lag_val) - l), ,
                                   drop = FALSE]
                colnames(lag_val) <- paste0("lag", l, "m_surplus")
                return(lag_val)
            })))
        rain <- data.frame(
            rain, do.call(cbind, lapply(1:12, function(l){
                lag_val <- as.data.frame(c(rep(NA, l),
                                           rain$normal_surplus))
                lag_val <- lag_val[1:(nrow(lag_val) - l), ,
                                   drop = FALSE]
                colnames(lag_val) <- paste0("lag", l,
                                            "m_normal_surplus")
                return(lag_val)
            })))
                                        # Compute measures of deficit
                                        # and surplus
        s_cols <- do.call(cbind, lapply(c(1:12), function(coln){
            m_pos_surplus <- ifelse(
                rain[ , paste0("lag", coln, "m_surplus")] > 0,
                rain[ , paste0("lag", coln, "m_surplus")], 0)
            m_neg_surplus <- ifelse(
                rain[ , paste0("lag", coln, "m_surplus")] < 0,
                rain[ , paste0("lag", coln, "m_surplus")], 0)
            ans <- data.frame(m_neg_surplus, m_pos_surplus)
            colnames(ans) <- c(paste0("lag", coln, "m_neg_surplus"),
                               paste0("lag", coln, "m_pos_surplus"))
            return(ans)
        }))
        ns_cols <- do.call(cbind, lapply(c(1:12), function(coln){
            m_pos_norm_surplus <- ifelse(
                rain[ , paste0("lag", coln, "m_normal_surplus")] > 0,
                rain[ , paste0("lag", coln, "m_normal_surplus")], 0)
            m_neg_norm_surplus <- ifelse(
                rain[ , paste0("lag", coln, "m_normal_surplus")] < 0,
                rain[ , paste0("lag", coln, "m_normal_surplus")], 0)
            ans <- data.frame(m_neg_norm_surplus,
                              m_pos_norm_surplus)
            colnames(ans) <- c(
                paste0("lag", coln, "m_neg_norm_surplus"),
                paste0("lag", coln, "m_pos_norm_surplus"))
            return(ans)
        }))
        rain <- data.frame(rain, s_cols, ns_cols)
        tmp <- fdata[fdata$stdistcode == x, ]
        tmp$yr_mnth <- substr(tmp$arrival_date, 1, 7)
        pre_obs <- nrow(tmp)
        yr_basis <- merge(tmp, rain[ , -match(c("district",
                                                "state",
                                                "stdistcode",
                                                "avgprecipitation"),
                                              colnames(rain))],
                          by = "yr_mnth", all.x = TRUE)
        post_obs <- nrow(yr_basis)
        if (post_obs < pre_obs){
            print("Data dropped.")
        }
        return(yr_basis)
    })
    ans <- rbindlist(ans)
    return(ans)
}    

#######################################
## Read NCDEX monthly traded volumes ##
#######################################
compile_traded_volumes <- function(commdetails){
  data_path <- "../../DATA/POLICY_PAPER/"
  
  all_files <- list.files(paste0(data_path, "NCDEX_VOLUMES/"), pattern = ".csv")
  ans <- lapply(commdetails$commname, function(x){
      x <- as.character(x)
      print(x)
      tmp <- all_files[grep(tolower(x), all_files)]
      comm <- do.call(rbind, lapply(tmp, function(y){
          print(y)
          file <- read.csv(file = paste0(data_path, "NCDEX_VOLUMES/", y),
                           header = TRUE)
          return(file)
      }))
      comm <- comm[ , c("CPD_PriceDate", "Quantity")]
      colnames(comm) <- c("date", "qty_traded")
      comm$date <- as.Date(as.character(comm$date), "%d-%b-%Y")
      comm <- comm[order(comm$date), ]
      unique_dates <- unique(comm$date)
      tmp <- lapply(unique_dates, function(y){
          print(y)
          eachdate <- comm[comm$date == y, ]
          if(nrow(eachdate) == 1){
              return(eachdate)
      } else {
          eachdate <- data.frame("date" = y,
                                 "qty_traded" = sum(eachdate$qty_traded))
          return(eachdate)
      }
      })
      tmp <- rbindlist(tmp)
      tmp <- tmp[order(tmp$date), ]
      tmp <- data.frame(tmp)
      ## final <- apply.monthly(xts(tmp$qty_traded, order = tmp$date),
      ##                        sum)
      ## final <- data.frame("yr_month" = substr(index(final), 1, 7),
      ##                     "qty_traded" = coredata(final))
      write.csv(tmp, file = paste0(data_path,
                                     "NCDEX_VOLUMES/CLEAN_DATA/",
                                     "vol_", x, ".csv"))
      return(NULL)
  })
}

add_ncdex_vol <- function(fdata, commdetails){
    tmp <- data.frame(fdata)
    x <- as.character(unique(fdata$commodity))
    print(x)
    futIntroDate <- commdetails$futIntroDate[commdetails$commname == x]
    tmp <- tmp[order(tmp$arrival_date), ]
    traded_vol <- read.csv(file = paste0(data_path,
                               "NCDEX_VOLUMES/CLEAN_DATA/vol_", x, ".csv"),
                           header = TRUE)[,-1]
    traded_vol$date <- as.Date(traded_vol$date)
    
    ## traded_vol$qty_traded <- traded_vol$qty_traded / 25 -- @Saragam: why have you divided it by 25?
    ## Ask NCDEX why on certain days NCDEX volumes are zero
    
    tmp <- merge(tmp, traded_vol,
                 by.x = "arrival_date", by.y = "date",
                 all.x = TRUE )#, allow.cartesian = TRUE)
    tmp$qty_traded <- ifelse(tmp$qty_traded == 0, 1, tmp$qty_traded)
    
    ## tmp$qty_traded[is.na(tmp$qty_traded)] <- 0
    ## tmp$qty_traded <- log(1 + tmp$qty_traded)
    return(tmp)
}

###############################################
## Add dummy for different contracts regimes ##
###############################################

add_contract_dummies <- function(fdata, commdetails, contract_dummies){
    tmp <- data.frame(fdata)
    x <- unique(tmp$commodity)
    cat("Defining dummy variables for different regimes:", x, "\n")
    cdetails <- contract_dummies[contract_dummies$commname == x,]
    c1 <- seq(as.Date(cdetails$contract1_start),
              as.Date(cdetails$contract1_end), 1)
    c2 <- seq(as.Date(cdetails$contract2_start),
              as.Date(cdetails$contract2_end), 1)
    c3 <- seq(as.Date(cdetails$contract3_start),
              as.Date(cdetails$contract3_end), 1)
    tmp$c1_dummy <- ifelse(tmp$arrival_date %in% c1, 1, 0)
    tmp$c2_dummy <- ifelse(tmp$arrival_date %in% c2, 1, 0)
    tmp$c3_dummy <- ifelse(tmp$arrival_date %in% c3, 1, 0)
    tmp <- tmp[order(tmp$arrival_date), ]
    return(tmp)
}


##############################################
## Add international prices for commodities ##
##############################################

add_intll_prices <- function(tmp){
    intll_prices <- read.csv(
        file = "../../DATA/POLICY_PAPER/201709intll_commodity_prices.csv",
        skip = 2, header = TRUE)
    colnames(intll_prices) <- c("date", "rmseed_oil", "maize",
                                "soybean_meal", "soybean_oil",
                                "soybean_futures", "wheat")
    intll_prices <- intll_prices[-1, ]
    intll_prices <- intll_prices[ , c("date", "rmseed_oil",
                                      "maize", "soybean_oil")]
    intll_prices$year <- as.numeric(substr(intll_prices$date,
                                           1, 4))
    intll_prices <- intll_prices[intll_prices$year %in% 2003:2015, ]
    months <- c("01", "02", "03", "04", "05", "06", "07", "08", "09",
                "10", "11", "12")
    intll_prices$yr_month <- paste0(intll_prices$year, "-", months)
    intll_prices <- intll_prices[ , c("yr_month", "rmseed_oil",
                                      "maize", "soybean_oil")]
    colnames(intll_prices) <- c("yr_month", "MUSTARD", "MAIZE",
                                "SOYBEAN")
    intll_prices$SOYBEAN <- as.numeric(intll_prices$SOYBEAN)
    intll_prices$MUSTARD <- as.numeric(intll_prices$MUSTARD)
    intll_prices$MAIZE <- as.numeric(intll_prices$MAIZE)

    intll_prices$date <- as.Date(paste0(intll_prices$yr_month, "-01"))

    pdf("intll_prices.pdf")
    par(mfrow = c(3,1))
    par(mar = c(2,4,2,2))
    plot(intll_prices$date, intll_prices[,"SOYBEAN"], type = "l", lwd = 2,
         ylab = "Soybean", xlab = "", col = "blue")
    abline (h = 1, lwd = 2, col = "black", lty = 2)
    grid()
    plot(intll_prices$date, intll_prices[,"MUSTARD"], type = "l", lwd = 2,
         ylab = "Mustard", xlab = "", col = "blue")
    abline (h = 1, lwd = 2, col = "black", lty = 2)
    grid()
    plot(intll_prices$date, intll_prices[,"MAIZE"], type = "l", lwd = 2,
         ylab = "Maize", xlab = "", col = "blue")
    abline (h = 1, lwd = 2, col = "black", lty = 2)
    grid()
    dev.off()

    x <- unique(tmp$commodity)
    tmp$yr_mnth <- as.character(tmp$yr_mnth)
    
    if (x %in% colnames(intll_prices)){
        iprice <- intll_prices[ , c("yr_month", x)]
        colnames(iprice) <- c("yr_mnth", "intll_price")
        tmp <- merge(tmp, iprice, by = "yr_mnth",
                     all.x = TRUE)
    } else {
        tmp$intll_price <- NA
    }
    return(tmp)
}

createMasterData <- function(commdetails){
    ## basedir <- "../../DATA/POLICY_PAPER/FINAL_DATABASE/"
    basedir <- "../../DATA/POLICY_PAPER/FINAL_DATABASE/"
    pdiffFiles <- list.files(basedir, pattern  = "basedata", full.names = TRUE)
    masterDataFile  <- paste0(basedir, "pdiff_with_intll_prices.RData")
    
    if (file.exists(masterDataFile) == TRUE){
        load(masterDataFile, verbose = TRUE)
    } else {
        if (file.exists(paste0(basedir, "pdiff_data.RData")) == TRUE){
            load(paste0(basedir, "pdiff_data.RData"))
        } else {
            for (i in 1:nrow(commdetails)){
                comm <- as.character(commdetails$commname[i])
                var_name <- paste0("basedata",
                                   toupper(substring(comm, 1,1)),
                                   tolower(substring(comm, 2)))
                assign(var_name,
                       prepData(commname = comm,
                                futIntroDate = commdetails[i, "futIntroDate"]))
            }
            
            for (i in pdiffFiles){
                load(i, verbose = TRUE)
            }
            
            centerObsData <- list(basedataCoriander[[2]], basedataMaize[[2]],
                                  basedataMustard[[2]], basedataSoybean[[2]],
                                  basedataTurmeric[[2]])
            
            save(centerObsData, file = paste0(basedir, "centerObsData.RData"))
            
            pdiff_data <- list(basedataCoriander[[1]], basedataMaize[[1]],
                               basedataMustard[[1]], basedataSoybean[[1]],
                               basedataTurmeric[[1]])
        
            save(pdiff_data, file = paste0(basedir, "pdiff_data.RData"))
        }
     ## Check if market share data exists, else generate
        if (file.exists(paste0(basedir, "pdiff_with_mktshare.RData")) == TRUE){
            load(paste0(basedir, "pdiff_with_mktshare.RData"))
        } else {
            
            ## Creating dummy for center
            pdiff_with_center <- lapply(pdiff_data, function(df){
                return(create_center_dummy(fdata = df,
                                           commdetails = commdetails))
            })
            
            pdiff_with_mktshare <- lapply(pdiff_with_center, function(df){
                return(compute_mandi_mkt_share(fdata = df))
            })
            
            ## Save this once since it takes a lot of time to generate this dataset.
            save(pdiff_with_mktshare, file = paste0(basedir, "pdiff_with_mktshare.RData"))
        }## Add district codes to mandis
        
        if (file.exists(paste0(basedir, "pdiff_with_district.RData")) == TRUE){
            load(paste0(basedir, "pdiff_with_district.RData"))
        } else {
            pdiff_with_district <-  lapply(pdiff_with_mktshare, function(df){
                return(combine_dist_codes(fdata = df))
            })
            save(pdiff_with_district, file = paste0(basedir, "pdiff_with_district.RData"))
        }
        
        ## All million plus cities dummy --- this needs to be fixed
        ## pdiff_with_urban <- add_million_plus_cities_data(fdata = pdiff_with_district)
        
        ## Add rainfall data
        
        if (file.exists(paste0(basedir, "pdiff_with_rain.RData")) == TRUE){
            load(paste0(basedir, "pdiff_with_rain.RData"))
        } else {
            pdiff_with_rain <- lapply(pdiff_with_district, function(df){
                return(add_rainfall_data(fdata = df))
            })
            save(pdiff_with_rain, file = paste0(basedir, "pdiff_with_rain.RData"))
        }
        
        ## Add ncdex volumes
        ncdex_monthly_volumes <- compile_traded_volumes(commdetails)
        
        pdiff_with_ncdex_vol <- lapply(pdiff_with_rain, function(df){
            return(add_ncdex_vol(fdata = df,
                                 commdetails = commdetails))
        })
        ## Add weekday
        pdiff_with_ncdex_vol_with_weekday <- lapply(pdiff_with_ncdex_vol, function(df){
            df$weekday <- weekdays(df$arrival_date)
            return(df)
        })
        
        ## Add contract dummies
        pdiff_with_regimes <- lapply(pdiff_with_ncdex_vol_with_weekday, function(df){
            return(add_contract_dummies(fdata = df,
                                        commdetails,
                                        contract_dummies))
        })
        
        ## Add international prices
        pdiff_with_intll_prices <- lapply(pdiff_with_regimes, function(df){
            return(add_intll_prices(tmp = df))
        })
        save(pdiff_with_intll_prices, file = paste0(basedir, "pdiff_with_intll_prices.RData"))        
    }
    return(pdiff_with_intll_prices)
}


## Get bhavcopy data

getFuturesVolumesData <- function(symbolPattern, dirname){
  files <- list.files(dirname, full.names = TRUE)
  fdat <- do.call(rbind, lapply(files, function(f){
    print(f)
    fdata <- read.csv(f)
    fdata$Symbol <- as.character(fdata$Symbol)
    fdata <- fdata[grep(symbolPattern, fdata$Symbol),]
    fdata$Quantity.Traded.Today <- as.character(fdata$Quantity.Traded.Today)
    uniqueSymbol <- unique(fdata$Symbol)
    dat <- do.call(rbind, lapply(uniqueSymbol, function(s){
      tmp <- fdata[fdata$Symbol == s,]
      tmp <- data.frame(symbol = s,
                        date = gsub(".csv", "", basename(f)),
                        tradedQty = sum(as.numeric(tmp$Quantity.Traded.Today)))
      return(tmp)
    }))
    return(dat)
  }))
  return(fdat)
}
 
allFuturesVolumesData <- function(symbolPattern, filename){
  if (length(list.dirs("/database/SECMKT/NCDEX/BHAVCOPY")) != 0){
    dirs <- list.dirs("/database/SECMKT/NCDEX/BHAVCOPY")
    dat <- do.call(rbind, lapply(dirs, function(d){
      if (length(strsplit(d, "/")[[1]]) <= 6){
        return(NULL)
      } else {
        futVolumes <- getFuturesVolumesData(symbolPattern, d)
      }
    }))
    save(dat,
         file = paste0("../../DATA/POLICY_PAPER/NCDEX_VOLUMES/CLEAN_DATA/", filename, ".RData"))
  }
}
         
allFuturesVolumesData("RMSEED", filename = "MUSTARD")
allFuturesVolumesData("MAIZ", filename = "MAIZE")
allFuturesVolumesData("SYBEAN", filename = "SOYBEAN")
allFuturesVolumesData("DHANIYA", filename = "CORIANDER")
allFuturesVolumesData("TMCFGRNZM", filename = "TURMERIC")

## Re-add the column for maize volumes 
addVolumesData <- function(filename, commdetails){
  load(paste0("../../DATA/POLICY_PAPER/NCDEX_VOLUMES/CLEAN_DATA/",
              filename,
              ".RData"),
       verbose = TRUE)
  
  dat$symbol <- as.character(dat$symbol)
  dat$date <- as.character(dat$date)
  dat <- dat[order(dat$date),]
  dat$symbol <- gsub(" ", "", dat$symbol)
  dat$date <- as.Date(dat$date, "%Y%m%d")
  
  plot(dat$date, log(dat$tradedQty), lwd =3, type = "l")

  checkData <- do.call(rbind, lapply(unique(dat$symbol), function(s){
    tmp <- dat[dat$symbol == s, ]
    tmp <- tmp[order(tmp$date), ]
    ans <- data.frame(symbol = s,
                      startdate = head(tmp$date, 1),
                      enddate = tail(tmp$date, 1),
                      nrow = nrow(tmp))
    return(ans)
  }))
  print(checkData)
  
  dat_totvol <- do.call(rbind, lapply(unique(dat$date), function(d){
    tmp <- dat[dat$date == d, ]
    return(data.frame(date = d,
                      qty_traded = sum(unique(tmp$tradedQty))))
  }))
  
  plot(dat_totvol$date, log(dat_totvol$qty_traded), lwd = 3,
       type = "l")
  grid()           
  
  regData <- createMasterData(commdetails)

  if (filename == "CORIANDER"){
    mdata <- regData[[1]]
    stopifnot(unique(mdata$commodity) == "CORIANDER")
  }

  if (filename == "MAIZE"){
    mdata <- regData[[2]]
    stopifnot(unique(mdata$commodity) == "MAIZE")
  }

  if (filename == "MUSTARD"){
    mdata <- regData[[3]]
    stopifnot(unique(mdata$commodity) == "MUSTARD")
  }

  if (filename == "SOYBEAN"){
    mdata <- regData[[4]]
    stopifnot(unique(mdata$commodity) == "SOYBEAN")
  }

  if (filename == "TURMERIC"){
    mdata <- regData[[5]]
    stopifnot(unique(mdata$commodity) == "TURMERIC")
  }

  
  mdata <- merge(mdata, dat_totvol, by.x = "arrival_date", by.y = "date", all.x = TRUE)
  tmp <- unique(mdata[, c("arrival_date", "qty_traded.x", "qty_traded.y")])
  plot(tmp[,1], tmp[,2], col = "red", lwd = 2)
  lines(tmp[,1], tmp[,3], col = "blue", lwd = 2)
  mdata$qty_traded <- mdata$qty_traded.y
  mdata$qty_traded.y <- mdata$qty_traded.x <- NULL
  return(mdata)
}


regData <- lapply(as.character(commdetails[,1]),
                  function(x){
                    addVolumesData(filename = x, commdetails)
                  })

save(regData, file = paste0("../../DATA/POLICY_PAPER/FINAL_DATABASE/regData.RData"))



######################################################################
## Prepare data for hypothesis II and III:                          ##
## volatility in arrivals and prices                                ##
######################################################################

regData <- get(load(paste0("../../DATA/POLICY_PAPER/FINAL_DATABASE/regData.RData"),
                    verbose = TRUE))

captureVariation <- function(cdata){
    uniqueMandis <- unique(cdata$address)

    graphsDir <- "../../../DOC/POLICY_PAPER/GRAPHS/"
    aggregateArrivals <- tapply(cdata$arrivalstonnes,
                                cdata$arrival_date, "sum")

    meanPrice <- tapply(cdata$minimumpricersquintal,
                        cdata$arrival_date, "mean")

    aggData <- data.frame(aggregateArrivals, meanPrice)

    commname <- unique(cdata$commodity)
    
    png(paste0(graphsDir, commname, "_aggegrateArrivals.png"),
        width = 400, height = 250)
    par(mar = c(2, 4, 2.5, 2))
    plot(as.Date(rownames(aggData)), aggData[,"aggregateArrivals"],
         lwd = 2, col = "black", ylab = "Daily arrivals (in tonnes)",
         xlab = "", type ="l", main = commname)
    grid()
    dev.off()

    png(paste0(graphsDir, commname, "_meanPrices.png"),
        width = 400, height = 250)
    par(mar = c(2, 4, 2.5, 2))
    plot(as.Date(rownames(aggData)), aggData[,"meanPrice"],
         lwd = 2, col = "black", ylab = "Minimum price (Rs. quintal)",
         xlab = "", type ="l", main = commname)
    grid()
    dev.off()

    mandiTmp <- mclapply(uniqueMandis, mc.cores = 2, function(m){
        tmp <- cdata[cdata[,"address"] == m,]
        
        yearlyTmp <- lapply(unique(tmp$year), function(y){
            tmp.y <- tmp[tmp[,"year"] == y, ]
            mean_arrivals <- mean(tmp.y[,"arrivalstonnes"], na.rm = TRUE)
            sd_arrivals <- sd(tmp.y[,"arrivalstonnes"], na.rm = TRUE)
            tmp.y$std_arrivals <- (tmp.y[,"arrivalstonnes"] - mean_arrivals)/sd_arrivals
            mean_minprice <- mean(tmp.y[,"minimumpricersquintal"], na.rm = TRUE)
            sd_minprice <- sd(tmp.y[,"minimumpricersquintal"], na.rm = TRUE)
            tmp.y$std_minprice <- (tmp.y[,"minimumpricersquintal"] - mean_minprice)/sd_minprice
            return(tmp.y)
        })
                        
        yearlyTmp <- rbindlist(yearlyTmp)
        return(yearlyTmp)
    })

    mandiTmp <- rbindlist(mandiTmp)
    return(mandiTmp)
}

regData <- lapply(regData, captureVariation)

save(regData, file = paste0("../../DATA/POLICY_PAPER/FINAL_DATABASE/regData.RData"))
