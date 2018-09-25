########################################
## Create basis/delivery center dummy ##
########################################
create_center_dummy <- function(fdata, commdetails){
    fdata <- mclapply(fdata, mc.cores = 1, function(x){
        
        x <- data.frame(x)
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
        colnames(pc) <- c("commodity", "center", "state", "from", "to")
        colnames(dc) <- c("commodity", "center", "state", "from", "to")
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
    })
    return(fdata)
}

##################################
## Computing mandi market share ##
##################################
compute_mandi_mkt_share <- function(fdata){
    fdata <- mclapply(fdata, mc.cores = 1, function(x){
        x <- data.frame(x)
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

        ## The following code was getting stuck for Maize-- where
        ## center name is "Delhi, Delhi". The
        ## mandis listed for Delhi include: Narela, Najafgarh, and
        ## Shahdara. I have changed the center name to the mandi that
        ## trades the most in Delhi, i.e. altered the name of the center to
        ## "Narela, Delhi".
           msdata <- lapply(yrSplit, function(y){
            print(unique(y$year))
            y <- unique(data.frame(y))
            y$minimum_distance_centeraddress[y$minimum_distance_centeraddress
                                             == "Delhi, Delhi"] <- "Narela, Delhi"
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
            ms$address <- as.character(ms$address)
            ## Now find the market share of each center
            
            ms <- rbindlist(mclapply(1:nrow(ms), mc.cores = 1, function(i){
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
    })
    fdata <- rbindlist(fdata)
    return(fdata)
}

##########################################
## Read mandi district codes data file  ##
##########################################

## Not reviewed this code -- Nidhi

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
        "matching_unit" = c(
            "Aizawl+MIZORAM", "Alathur+KERALA", "Arreria+BIHAR",
            "Aruppukottai+TAMILNADU", "Balasinor+GUJARAT",
            "Banswara+RAJASTHAN", "Bara Bazar+MIZORAM",
            "Bazar Atriya +CHATTISGARH", "Begusarai+BIHAR",
            "Bharthana+UTTARPRADESH", "Bihta+BIHAR",                                          
            "Birpur+BIHAR", "Charkhari+UTTARPRADESH",                               
            "Dahivadi+MAHARASHTRA", "Dahod(Himalaya)+GUJARAT",                              
            "Danapur+BIHAR", "Darbhanga+BIHAR",                                      
            "Dhara+KERALA", "Dungarpur+RAJASTHAN",                                  
            "Forbesganj+BIHAR", "Gulabbagh+BIHAR",
            "Jaggayyapeta+ANDHRAPRADESH", "Jaipur(Grain)(Sodala)+RAJASTHAN",                      
            "Jalalabad+UTTARPRADESH", "Jammalamadugu+ANDHRAPRADESH",                          
            "Kaswa+BIHAR", "Khagaria+BIHAR", "Khuzama+NAGALAND",                                     
            "Kishanganj+BIHAR", "Lakhisarai+BIHAR",                                     
            "Lathi+GUJARAT", "Mangalagiri+ANDHRAPRADESH",                            
            "Mau(Chitrakut)+UTTARPRADESH", "Mohamadabad+UTTARPRADESH",                             
            "Muzaffarpur+BIHAR", "Naharlagun+ARUNACHALPRADESH",                          
            "Narkatiaganj+BIHAR", "Paderu,RBZ+ANDHRAPRADESH",                             
            "Panthawada+GUJARAT", "Pasighat+ARUNACHALPRADESH",                            
            "Puwaha+UTTARPRADESH", "Ramanathapuram(phase 3)+TAMILNADU",                    
            "R and B Guest House,RBZ,Vizianagaram+ANDHRAPRADESH",   
            "Sajapur+MADHYAPRADESH", "Sandi+UTTARPRADESH",
            "Santrampur+GUJARAT", "Shahabad+UTTARPRADESH",
            "Sringeri+KARNATAKA", "Tawang+ARUNACHALPRADESH",
            "Thara(Shihori)+GUJARAT", "Thungathurthy+ANDHRAPRADESH",
            "Tiruchengode(Agri. Coop. Marketing Society)+TAMILNADU",
            "Urai+UTTARPRADESH", "Vellankovil+TAMILNADU",
            "Vilathikulam+TAMILNADU", "Virudhunagar+TAMILNADU"),
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
add_million_plus_cities_data <- function(fdata){
    cat("\nCombining million-plus cities data with mandi data.", "\n")
    mpc_data <- read.dta13(
        "../../DATA/POLICY_PAPER/201706million_plus_cities.dta")
    mpc_data <- mpc_data[ , c("statename", "districtname",
                              "stdistcode", "millcities")]
    tmp <- merge(fdata, mpc_data[ , c("stdistcode", "millcities")],
                 by = "stdistcode", all.x = TRUE)
    
    return(tmp)
}

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
        rain <- rain_data[rain_data$stdistcode %in% x, ]
        rain <- rain[order(rain$td), ]
        rain <- rain[!duplicated(rain$td), ]
        rain$yr_mnth <- substr(rain$td, 1, 7)
        rain$surplus <- NA
        rain$normal_surplus <- NA
                                        # Compute surplus and
                                        # normalised surplus

        ## @Sargam: we need to take historical average of rain. Right
        ## now it is taking the overall average. If surplus or deficit
        ## should be based on prior years rainfall. Not on future years.
        ## Done this....
        
        ## for(i in 1:length(rain$yr_mnth)){
        ##     avg_rain <- mean(rain$avgprecipitation[substr(
        ##                               rain$yr_mnth, 6, 7) ==
        ##                                            substr(rain$yr_mnth[i], 6, 7)])
        ##     sd_rain <- sd(rain$avgprecipitation[substr(
        ##                            rain$yr_mnth, 6, 7) ==
        ##                            substr(rain$yr_mnth[i], 6, 7)])
        ##     rain$surplus[i] <- rain$avgprecipitation[i] - avg_rain
        ##     rain$normal_surplus[i] <- (rain$avgprecipitation[i] - avg_rain) / sd_rain
        ## }
        
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
        tmp <- fdata[fdata$stdistcode %in% x, ]
        tmp$yr_mnth <- substr(tmp$arrival_date, 1, 7)
        pre_obs <- nrow(tmp)
        yr_basis <- merge(tmp, rain[ , -match(c("district",
                                                "state",
                                                "stdistcode",
                                                "avgprecipitation"),
                                              colnames(rain))],
                          by = "yr_mnth")
        post_obs <- nrow(yr_basis)
        if (post_obs < pre_obs){print("Data dropped.")}
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
      eachdate <- comm[comm$date %in% y, ]
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
    final <- apply.monthly(xts(tmp$qty_traded, order = tmp$date),
                           sum)
    final <- data.frame("yr_month" = substr(index(final), 1, 7),
                        "qty_traded" = coredata(final))
    write.csv(final, file = paste0(data_path,
                                   "NCDEX_VOLUMES/CLEAN_DATA/",
                                   "vol_", x, ".csv"))
    return(NULL)
  })
}

add_ncdex_vol <- function(fdata, commdetails){
    ans <- lapply(commdetails$commname, function(x){
        x <- as.character(x)
        print(x)
        futIntroDate <- commdetails$futIntroDate[
            commdetails$commname == x]
        tmp <- fdata[fdata$commodity == x, ]
        tmp <- tmp[order(tmp$arrival_date), ]
        traded_vol <- read.csv(file = paste0(data_path,
                      "NCDEX_VOLUMES/CLEAN_DATA/vol_", x, ".csv"),
        header = TRUE)
        traded_vol <- traded_vol[ , c("yr_month", "qty_traded")]
        traded_vol$qty_traded <- traded_vol$qty_traded / 25
        tmp <- merge(tmp, traded_vol,
                     by.x = "yr_mnth", by.y = "yr_month",
                     all.x = TRUE, allow.cartesian = TRUE)
        tmp$qty_traded[is.na(tmp$qty_traded)] <- 0
        tmp$qty_traded <- log(1 + tmp$qty_traded)
      return(tmp)
    })
  ans <- rbindlist(ans)
  return(ans)
}

###############################################
## Add dummy for different contracts regimes ##
###############################################

add_contract_dummies <- function(fdata, commdetails, contract_dummies){
  commdetails <- commdetails[-which(commdetails$commname == "CUMMIN"), ]
  foreverycomm <- lapply(commdetails$commname, function(x){
    x <- as.character(x)
    cat("Defining dummy variables for different regimes:", x, "\n")
    tmp <- fdata[fdata$commodity == x, ]
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
  })
  foreverycomm <- rbindlist(foreverycomm)
  return(foreverycomm)
}

##############################################
## Add international prices for commodities ##
##############################################

add_intll_prices <- function(fdata){
    intll_prices <- read.csv(
        file = paste0(data_path, "201709intll_commodity_prices.csv"),
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
    rel_comm <- c("MUSTARD", "MAIZE", "SOYBEAN")
    ans <- lapply(rel_comm, function(x){
        tmp <- fdata[fdata$commodity %in% x, ]
        tmp$yr_mnth <- as.character(tmp$yr_mnth)
        iprice <- intll_prices[ , c("yr_month", x)]
        colnames(iprice) <- c("yr_month", "intll_price")
        tmp <- merge(tmp, iprice, by.x = "yr_mnth", by.y = "yr_month",
                     all.x = TRUE)
        return(tmp)
    })
    ans <- rbindlist(ans)
    ans$intll_price <- log(1 + ans$intll_price) ##@Sargam: why add 1? There can't be zero or negative prices here.
    return(ans)
}






