library(data.table)
library(parallel)
library(lmtest)
library(sandwich)
source("regression.R")

## Winsorize the final data

winsorize <- function(data){
    data[which(data > quantile(data, 0.99, na.rm = TRUE))] <- quantile(data, 0.99, na.rm = TRUE)
    data[which(data <= quantile(data,0.01, na.rm = TRUE))] <- quantile(data, 0.01, na.rm = TRUE)
    return(data)
}

regData <- get(load(paste0("../../DATA/POLICY_PAPER/FINAL_DATABASE/regData.RData"),
                    verbose = TRUE))

## Winsorize price diff data, and turn NA futures volumes values to 0,
## else those rows will be deleted in the regression -- winsorize
## futures volumes as well. Rest all is alreadt winsorized.

regData <- lapply(regData, function(x){
    x <- data.frame(x)
    x[,"price_diff"] <- winsorize(x[,"price_diff"])
    x[,"qty_traded"] <- winsorize(x[,"qty_traded"])
    x[,"arrivalstonnes"] <- ifelse(x[,"arrivalstonnes"] < 1,
                                   1+x[,"arrivalstonnes"],
                                   x[,"arrivalstonnes"])

    x[,"qty_traded"] <- ifelse(is.na(x[,"qty_traded"]), 1, x[,"qty_traded"])
    x[,"qty_traded"] <- ifelse(x[,"qty_traded"] == 0, 1, x[,"qty_traded"])

    x[,"minimum_distance"] <- ifelse(x[,"minimum_distance"] == 0, 1, x[,"minimum_distance"])
    x[,"month"] <- substr(x[,"yr_mnth"], 6, 7)
    return(x)
})

analysis <- function(x, dependentVariable, hypothesis,
                     commodityEffects, monthEffects,
                     contractChangeEffects){ # x is the dataset

    if (dependentVariable == "abs(std_arrivals)"){
        includeArrivals = FALSE
    } else {
        includeArrivals = TRUE
    }

    bmodel <- regression_model(cdata = x,
                               monthEffect = FALSE,
                               stateYearInteraction = FALSE,
                               contractChanges = FALSE,
                               contractChangesWithInteraction = FALSE,
                               marketShare_center = FALSE,
                               center_dummyInteraction = FALSE,
                               pooled = commodityEffects,
                               includeArrivals = includeArrivals,
                               dependentVariable = dependentVariable)
    
    bmodel_me <- regression_model(cdata = x,
                                  monthEffect = monthEffects,
                                  stateYearInteraction = FALSE,
                                  contractChanges = FALSE,
                                  contractChangesWithInteraction = FALSE,
                                  marketShare_center = FALSE,
                                  center_dummyInteraction = FALSE,
                                  pooled = commodityEffects,
                                  includeArrivals = includeArrivals,
                                  dependentVariable = dependentVariable)

    bmodel_me_cchanges <- regression_model(cdata = x,
                                           monthEffect = monthEffects,
                                           stateYearInteraction = FALSE,
                                           contractChanges = TRUE,
                                           contractChangesWithInteraction = FALSE,
                                           marketShare_center = FALSE,
                                           center_dummyInteraction = FALSE,
                                           pooled = commodityEffects,
                                           includeArrivals = includeArrivals,
                                           dependentVariable = dependentVariable)

    if (contractChangeEffects == TRUE){
        contractChangesWithInteraction = TRUE
    } else {
        contractChangesWithInteraction = FALSE
    }
    
    bmodel_me_cdummyInter <- regression_model(cdata = x,
                                              monthEffect = monthEffects,
                                              stateYearInteraction = FALSE,
                                              contractChanges = contractChangeEffects,
                                              contractChangesWithInteraction = contractChangesWithInteraction,
                                              marketShare_center = FALSE,
                                              center_dummyInteraction = TRUE,
                                              pooled = commodityEffects,
                                              includeArrivals = includeArrivals,
                                              dependentVariable = dependentVariable) 
    
    bmodel_me_cdummyInter_mscenter <- regression_model(cdata = x,
                                                       monthEffect = monthEffects,
                                                       stateYearInteraction = FALSE,
                                                       contractChanges = contractChangeEffects,
                                                       contractChangesWithInteraction = contractChangesWithInteraction,
                                                       marketShare_center = TRUE,
                                                       center_dummyInteraction = TRUE,
                                                       pooled = commodityEffects,
                                                       includeArrivals = includeArrivals,
                                                       dependentVariable = dependentVariable)
    
    bmodel_me_cdummyInter_mscenter_syInter <- regression_model(cdata = x,
                                                               monthEffect = monthEffects,
                                                               stateYearInteraction = TRUE,
                                                               contractChanges = contractChangeEffects,
                                                               contractChangesWithInteraction = contractChangesWithInteraction,
                                                               marketShare_center = TRUE,
                                                               center_dummyInteraction = TRUE,
                                                               pooled = commodityEffects,
                                                               includeArrivals = includeArrivals,
                                                               dependentVariable = dependentVariable)
    
    results <- list(bmodel,
                    bmodel_me,
                    bmodel_me_cchanges,
                    bmodel_me_cdummyInter,
                    bmodel_me_cdummyInter_mscenter,
                    bmodel_me_cdummyInter_mscenter_syInter)
    
    commname <- unique(x$commodity)
    
    if (length(commname) > 1){
        commname <- "ALL"
    }
    
    if (monthEffects == TRUE){
        monthEffects <- "with month effects"
    } else {
        monthEffects <- "without month effects"
    }

    if (commodityEffects == TRUE){
        commodityEffects <- "withce"
    } else {
        commodityEffects <- "woce"
    }
    
    resultsDir <- "../../../DOC/POLICY_PAPER/TABLES/"
    stargazerOutput(resultsObject = results,
                    dat = x,
                    resultsFile = paste0(resultsDir, hypothesis, "_",
                                         commname, "_", gsub(" ", "", monthEffects), "_",
                                         commodityEffects, ".tex"),
                    resultsTitle = paste0(toupper(strsplit(hypothesis, "_")[[1]][2]),
                                          ": Regression results for commodity, ",
                                          commname, " ", monthEffects),
                    dependentVariable = dependentVariable)
}

## Individual commodities

eachHypothesis_comm <- function(regData, hypothesis, dependentVariable, monthEffects){
    mclapply(c(1:5), mc.cores = 5, function(i){
        analysis(x = regData[[i]],
                 hypothesis = hypothesis,
                 dependentVariable = dependentVariable,
                 commodityEffects = FALSE,
                 monthEffects = monthEffects,
                 contractChangeEffects = TRUE)
    })
}

eachHypothesis_comm(regData,
                    hypothesis = "fulldata_h1",
                    dependentVariable = "abs(price_diff)",
                    monthEffects = TRUE)

eachHypothesis_comm(regData,
                    hypothesis = "fulldata_h2",
                    dependentVariable = "abs(std_arrivals)",
                    monthEffects = FALSE)

eachHypothesis_comm(regData,
                    hypothesis = "fulldata_h3",
                    dependentVariable = "abs(std_minprice)",
                    monthEffects = FALSE)

## Pooled regression

analysis(x = data.frame(rbindlist(regData)),
         hypothesis = "fulldata_h1",
         dependentVariable = "abs(price_diff)",
         commodityEffects = FALSE,
         monthEffects = TRUE,
         contractChangeEffects = FALSE)

analysis(x = data.frame(rbindlist(regData)),
         hypothesis = "fulldata_h2",
         dependentVariable = "abs(std_arrivals)",
         commodityEffects = FALSE,
         monthEffects = FALSE,
         contractChangeEffects = FALSE)

analysis(x = data.frame(rbindlist(regData)),
         hypothesis = "fulldata_h3",
         dependentVariable = "abs(std_minprice)",
         commodityEffects = FALSE,
         monthEffects = FALSE,
         contractChangeEffects = FALSE)

## Low priority regressions

analysis(x = data.frame(rbindlist(regData)),
         hypothesis = "fulldata_h1",
         dependentVariable = "abs(price_diff)",
         commodityEffects = TRUE,
         monthEffects = TRUE,
         contractChangeEffects = FALSE)

analysis(x = data.frame(rbindlist(regData)),
         hypothesis = "fulldata_h2",
         dependentVariable = "abs(std_arrivals)",
         commodityEffects = TRUE,
         monthEffects = FALSE,
         contractChangeEffects = FALSE)

analysis(x = data.frame(rbindlist(regData)),
         hypothesis = "fulldata_h3",
         dependentVariable = "abs(std_minprice)",
         commodityEffects = TRUE,
         monthEffects = FALSE,
         contractChangeEffects = FALSE)

analysis(x = data.frame(rbindlist(regData)),
         hypothesis = "fulldata_h2",
         dependentVariable = "abs(std_arrivals)",
         commodityEffects = FALSE,
         monthEffects = TRUE,
         contractChangeEffects = FALSE)

analysis(x = data.frame(rbindlist(regData)),
         hypothesis = "fulldata_h3",
         dependentVariable = "abs(std_minprice)",
         commodityEffects = FALSE,
         monthEffects = TRUE,
         contractChangeEffects = FALSE)

eachHypothesis_comm(regData,
                    hypothesis = "fulldata_h2",
                    dependentVariable = "abs(std_arrivals)",
                    monthEffects = TRUE)

eachHypothesis_comm(regData,
                    hypothesis = "fulldata_h3",
                    dependentVariable = "abs(std_minprice)",
                    monthEffects = TRUE)


regData_woCenters <- lapply(regData, function(x){
    x <- x[!(x$address %in% unique(x$minimum_distance_centeraddress)),]
    return(x)
})

