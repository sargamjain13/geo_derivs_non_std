## The following script compiles the entire analysis for evaluating the
## impact of introduction of futures contracts on farmers. The period
## of analysis is Oct, 2003 to Oct, 2016. The commodities set covered
## in the analysis include:
## 1. RM seed
## 2. Soybean
## 3. Turmeric
## 4. Maize
## 5. Coriander (Dhaniya)
## 6. Cummin (jeera)

rm(list = ls())
options(scipen = 999)
#######################
## Include libraries ##
#######################
library(xts)
library(parallel)
library(sandwich)
library(xtable)
library(readstata13)

#######################
## Compile databases ##
#######################
## The following file consists of boot toggle as input variable.
## If TRUE, it will start the data extraction process for:
## 1. Near-month futures data
## 2. Mandi data
## 3. Distance of all mandis form polling and basis centers
## If FALSE, it will load .rda files saved in DATA/POLICY_PAPER
## repository.
## By default, boot takes the value FALSE.
source("compile_database.R")
source("regression_data.R")
source("plot_graphs.R")
source("regression_analysis.R")

#####################
## Input variables ##
#####################
graph_path <- "../../../DOC/POLICY_PAPER/GRAPHS/"
commdetails <- data.frame(commname = spot_commodities,
                          futIntroDate = as.Date(
                              c("2008-08-11", "2005-02-03",
                                "2010-05-21", "2003-12-15",
                                "2003-12-15", "2004-07-24")))
comm_major_states <- data.frame(
    commname = spot_commodities,
    state1 = c("MadhyaPradesh", "Rajasthan", "Telangana", "Rajasthan",
               "MadhyaPradesh", "Telangana"),
    state2 = c("Rajasthan", "Gujarat", "Karnataka", "MadhyaPradesh",
               "Maharashtra", "TamilNadu"),
    state3 = c("Gujarat", "MadhyaPradesh", "Maharashtra", "Haryana",
               "Rajasthan", "Maharashtra"))

#########################
## Read spot data sets ##
#########################
basedata <- lapply(spot_commodities, function(x){
               spot <- mandidata[[x]]
               futures <- relevent_futures_contracts[[x]]
               futures <- futures[ , c("tradeDate", "basisCentre",
                                       "futpriceNear")]
               tmp <- merge(spot, futures,
                            by.x = "arrival_date",
                            by.y = "tradeDate",
                            all = TRUE)
               return(tmp)
           })
names(basedata) <- spot_commodities

#######################################
## Function calls for preparing data ##
#######################################

## DATA FOR HYPOTHESIS I: PRICE DIFFERENCES w.r.t BENCHMARK PRICES
## If files exist in DATA/POLICY_PAPER/FINAL_DATABASE/, the function is
## not called else it is sourced from regression_data.R, **prepData()**

final_database <- list.files("../../DATA/POLICY_PAPER/FINAL_DATABASE/",
                             pattern = "ready")
if(length(final_database) == 6){
    for(i in final_database){
        cat(i, "\n")
        load(paste0("../../DATA/POLICY_PAPER/FINAL_DATABASE/", i))
         }
    } else {
        for(i in 1:nrow(commdetails)){
            comm <- as.character(commdetails$commname[i])
            var_name <- paste0("basedata",
                               toupper(substring(comm, 1,1)),
                               tolower(substring(comm, 2)))
            assign(var_name,
                   prepData(
                     commname = as.character(commdetails[i,
                       "commname"]),
                     futIntroDate = commdetails[i, "futIntroDate"]))
            save(eval(parse(text = var_name)),
                 file =
                     paste0("../../DATA/POLICY_PAPER/FINAL_DATABASE/",
                            "ready_file_", tolower(comm), ".rda"))
        }
    }

## DATA FOR HYPOTHESIS II and III :-
## IMPACT OF COMM DERIVS on ON PRICE VOLATILITY AND MANDI ARRIVALS
## If files exist in DATA/POLICY_PAPER/FINAL_DATABASE/, the function is
## not called else it is sourced from regression_data.R,
## **prepdata_arrivals_pvol()**

final_database <- list.files("../../DATA/POLICY_PAPER/FINAL_DATABASE/",
                             pattern = "pvol_arrival")
if(length(final_database) == 6){
    for(i in final_database){
        cat(i, "\n")
        load(paste0("../../DATA/POLICY_PAPER/FINAL_DATABASE/", i))
        ## comm <- strsplit(strsplit(i, "_")[[1]][3], ".rda")[[1]][1]
        ## assign(paste0("price_arrival_", comm), ans)
    }
} else {
    for(i in 1:nrow(commdetails)){
        comm <- as.character(commdetails$commname[i])
        cat(comm, "\n")
                                        # monthly
        var_name_1 <- paste0("price_arrival_", tolower(comm),
                             "_monthly")
        assign(var_name_1,
               prepdata_arrivals_pvol(
                   cdata = eval(parse(
                       text = paste0("basedata",
                           toupper(substring(comm, 1, 1)),
                           tolower(substring(comm, 2)))))[[1]],
                   freq = "monthly"))
                                        # weekly
        var_name_2 <- paste0("price_arrival_", tolower(comm),
                             "_weekly")
        assign(var_name_2,
               prepdata_arrivals_pvol(
                   cdata = eval(parse(
                       text = paste0("basedata",
                           toupper(substring(comm, 1, 1)),
                           tolower(substring(comm, 2)))))[[1]],
                   freq = "weekly"))
        ans1 <- eval(parse(text = var_name_1))
        ans2 <- eval(parse(text = var_name_2))
        save(ans1, ans2, file = paste0(
                             "../../DATA/POLICY_PAPER/FINAL_DATABASE/",
                             "pvol_arrival_", tolower(comm), ".rda"))
    }
}

names(pdiff_data) <- commdetails$commname
#################
## Plot prices ##
#################
for(i in 1:nrow(commdetails)){
    comm <- as.character(commdetails$commname[i])
    plot_prices(cdata = pdiff_data[[comm]],
                futIntroDate = commdetails[i, "futIntroDate"],
                filename = paste0(graph_path, "prices", comm,
                    ".png"))
}

############################
## Plot price differences ##
############################
for(i in 1:nrow(commdetails)){
    comm <- as.character(commdetails$commname[i])
    var_name <- paste0("basedata",
                       toupper(substring(comm, 1,1)),
                       tolower(substring(comm, 2)))
    plot_price_diff(cdata = eval(parse(text = var_name))[[1]],
                    futIntroDate = commdetails[i, "futIntroDate"],
                    filename = paste0(graph_path, "price_diff", comm,
                                      ".png"))
}

################################################
## Plot NCDEX monthly average trading volumes ##
################################################
for(i in c(1, 3:6)){
    plot_ncdex_vol(
        commodity = commdetails$commname[i],
        futIntroDate = commdetails$futIntroDate[i],
        regimes = contract_dummies[i, 1:4],
        filename = paste0(graph_path, "ncdex_volumes_",
                          commdetails$commname[i], ".pdf"))
}

#######################################################
## Plot mandi arrivals and price volatility: monthly ##
#######################################################
for(i in 1:nrow(commdetails)){
    comm <- as.character(commdetails$commname[i])
    var_name <- paste0("price_arrival_", tolower(comm), "_monthly")
    plot_arrivals(cdata = eval(parse(text = var_name))[[2]],
                  futIntroDate = commdetails[i, "futIntroDate"],
                  filename = paste0(graph_path, "mandi_arrivals_",
                      tolower(comm), "_monthly.png"))
    plot_pvol(cdata = eval(parse(text = var_name))[[2]],
              futIntroDate = commdetails[i, "futIntroDate"],
              filename = paste0(graph_path, "price_volatility_",
                  tolower(comm), "_monthly.png"))
}

######################################################
## Plot mandi arrivals and price volatility: weekly ##
######################################################
for(i in 1:nrow(commdetails)){
    comm <- as.character(commdetails$commname[i])
    var_name <- paste0("price_arrival_", tolower(comm), "_weekly")
    plot_arrivals(cdata = eval(parse(text = var_name))[[2]],
                  futIntroDate = commdetails[i, "futIntroDate"],
                  filename = paste0(graph_path, "mandi_arrivals_",
                      tolower(comm), "_weekly.png"))
    plot_pvol(cdata = eval(parse(text = var_name))[[2]],
              futIntroDate = commdetails[i, "futIntroDate"],
              filename = paste0(graph_path, "price_volatility_",
                  tolower(comm), "_weekly.png"))
}

###############################################
## Regression analysis for price differences ##
###############################################
pdiff_Coriander <- regression_analysis(cdata = basedataCoriander[[1]])
pdiff_Cummin <- regression_analysis(cdata = basedataCummin[[1]])
pdiff_Maize <- regression_analysis(cdata = basedataMaize[[1]])
pdiff_Mustard <- regression_analysis(cdata = basedataMustard[[1]])
pdiff_Soybean <- regression_analysis(cdata = basedataSoybean[[1]])
pdiff_Turmeric <- regression_analysis(cdata = basedataTurmeric[[1]])

##############################################################
## Regression analysis: Price volatility and mandi arrivals ##
##############################################################

#############
## Monthly ##
#############
pvol_arrival_Coriander_monthly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_coriander_monthly[[1]],
    futIntroDate = commdetails$futIntroDate[1])
pvol_arrival_Cummin_monthly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_cummin_monthly[[1]],
    futIntroDate = commdetails$futIntroDate[2])
pvol_arrival_Maize_monthly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_maize_monthly[[1]],
    futIntroDate = commdetails$futIntroDate[3])
pvol_arrival_Mustard_monthly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_mustard_monthly[[1]],
    futIntroDate = commdetails$futIntroDate[4])
pvol_arrival_Soybean_monthly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_soybean_monthly[[1]],
    futIntroDate = commdetails$futIntroDate[5])
pvol_arrival_Turmeric_monthly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_turmeric_monthly[[1]],
    futIntroDate = commdetails$futIntroDate[6])

############
## Weekly ##
############
pvol_arrival_Coriander_weekly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_coriander_weekly[[1]],
    futIntroDate = commdetails$futIntroDate[1])
pvol_arrival_Cummin_weekly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_cummin_weekly[[1]],
    futIntroDate = commdetails$futIntroDate[2])
pvol_arrival_Maize_weekly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_maize_weekly[[1]],
    futIntroDate = commdetails$futIntroDate[3])
pvol_arrival_Mustard_weekly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_mustard_weekly[[1]],
    futIntroDate = commdetails$futIntroDate[4])
pvol_arrival_Soybean_weekly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_soybean_weekly[[1]],
    futIntroDate = commdetails$futIntroDate[5])
pvol_arrival_Turmeric_weekly <- pvol_arrival_reg_analysis(
    cdata = price_arrival_turmeric_weekly[[1]],
    futIntroDate = commdetails$futIntroDate[6])

## Table 1
t1 <- rbind(c(t(pvol_arrival_Coriander[[1]][1:2, 1:2]),
              pvol_arrival_Coriander[[1]][3, 1]),
            c(t(pvol_arrival_Cummin[[1]][1:2, 1:2]),
              pvol_arrival_Cummin[[1]][3, 1]),
            c(t(pvol_arrival_Maize[[1]][1:2, 1:2]),
              pvol_arrival_Maize[[1]][3, 1]),
            c(t(pvol_arrival_Mustard[[1]][1:2, 1:2]),
              pvol_arrival_Mustard[[1]][3, 1]),
            c(t(pvol_arrival_Soybean[[1]][1:2, 1:2]),
              pvol_arrival_Soybean[[1]][3, 1]),
            c(t(pvol_arrival_Turmeric[[1]][1:2, 1:2]),
              pvol_arrival_Turmeric[[1]][3, 1]))
## Table 2
t2 <- rbind(c(t(pvol_arrival_Coriander[[2]][1:2, 1:2]),
              pvol_arrival_Coriander[[2]][3, 1]),
            c(t(pvol_arrival_Cummin[[2]][1:2, 1:2]),
              pvol_arrival_Cummin[[2]][3, 1]),
            c(t(pvol_arrival_Maize[[2]][1:2, 1:2]),
              pvol_arrival_Maize[[2]][3, 1]),
            c(t(pvol_arrival_Mustard[[2]][1:2, 1:2]),
              pvol_arrival_Mustard[[2]][3, 1]),
            c(t(pvol_arrival_Soybean[[2]][1:2, 1:2]),
              pvol_arrival_Soybean[[2]][3, 1]),
            c(t(pvol_arrival_Turmeric[[2]][1:2, 1:2]),
              pvol_arrival_Turmeric[[2]][3, 1]))
## Table 3
t3 <- rbind(pvol_arrival_Coriander[[3]], pvol_arrival_Cummin[[3]],
            pvol_arrival_Maize[[3]], pvol_arrival_Mustard[[3]],
            pvol_arrival_Soybean[[3]], pvol_arrival_Turmeric[[3]])
## Table 4
t4 <- rbind(pvol_arrival_Coriander[[4]], pvol_arrival_Cummin[[4]],
            pvol_arrival_Maize[[4]], pvol_arrival_Mustard[[4]],
            pvol_arrival_Soybean[[4]], pvol_arrival_Turmeric[[4]])
