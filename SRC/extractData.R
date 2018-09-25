#########################################################################
## The program consists of following functions:
## 1. defineDateFormat: assign date formats for mixed formats in date
## 2. handleSplChar: handles special characters in numeric series
## 3. extractSpotData: compiles spot market data for the mentioned
## commodity using data stored in /database/SECMKT/NCDEX/SPOT/.
## 4. extractFuturesData : compiles data for all traded days during
## the period of analysis for a particular commodity using data stored in
## /database/SECMKT/NCDEX/BHAVCOPY/.
#########################################################################

## Libraries
library(data.table)
library(parallel)

## Assign date format
defineDateFormat <- function(expDate){
  formatDate <- as.Date(expDate, format = "%m/%d/%Y")
  if(any(is.na(formatDate))){
    formatDate <- as.Date(expDate, format = "%d-%b-%y")
  }
  if(any(is.na(formatDate))){
    formatDate <- as.Date(expDate, format = "%m-%d-%Y")
  }                          
  return(formatDate)
}


## Global variables
futuresPath <- "/database/SECMKT/NCDEX/BHAVCOPY/"

## Futures
futureFiles <- list.files(futuresPath, recursive = TRUE, pattern = ".csv")

futureFiles <- futureFiles[!(futureFiles %in% "2012/APR/04-11-2012.csv")]


# Extract futures data
extractFuturesData <- function(futSymbol){
  cat("Extracting futures data...\n")
  readFiles <- lapply(futureFiles[1:3095], function(f){
    
    print(f)
    cfile <- read.csv(file = paste0(futuresPath, f),
                      stringsAsFactors = FALSE,
                      header = TRUE)
    cfile <- cfile[grep("MAIZ", cfile$Symbol),]
    if (length(cfile) == 0){
      return(NULL)
    }
                                        # Sanity check for empty files
    if(nrow(cfile) < 5){
      return(NULL)
    }
                                        # Removing junk extra column in
                                        # files read
    if(ncol(cfile) > 16){
      cfile <- cfile[ , -ncol(cfile)]
    }
                                        # Picking data for futSymbol
                                        # passed as argument

    colnames(cfile) <- c("symbolFut", "expiryDate", "commodity",
                         "basisCentre", "priceUnit", "previousClose",
                         "open", "high", "low", "close", "qtyTraded",
                         "measure", "noOfTrades", "tradedValueInLacs",
                         "openInterestInQty", "lastTradeDate")
                                        # Remove junk data values in
                                        # base data
    if(any(grepl("NA", cfile$close, ignore.case = TRUE)) == TRUE){
      cfile <- cfile[-grep("NA", cfile$close, ignore.case = TRUE), ]
    }
                                        # Defining new column, tradeDate
                                        # for each days' file
    tradeDate <- as.Date(substr(f, 10, 17), format = "%Y%m%d")
    cfile$tradeDate <- rep(tradeDate, nrow(cfile))
                                        # Defining date format for date
                                        # columns
    cfile$expiryDate <- defineDateFormat(cfile$expiryDate)
    cfile$lastTradeDate <- defineDateFormat(cfile$lastTradeDate)
                                        # Computing days to expiry of
                                        # the contract
    cfile$daysToExpiry <- as.numeric(cfile$expiryDate - cfile$tradeDate)
    cfile <- cfile[ , c("symbolFut", "commodity", "expiryDate",
                        "tradeDate", "daysToExpiry", "basisCentre",
                        "priceUnit", "open", "high",
                        "low", "close", "qtyTraded", "measure",
                        "noOfTrades", "tradedValueInLacs",
                        "openInterestInQty")]
    return(cfile)
  })
  commData <- rbindlist(readFiles)
  commData <- commData[order(commData$tradeDate), ]
  return(commData)
}


fdata <- extractFuturesData("MAIZ")

fdata <- fdata[order(fdata$tradeDate),]
tradeDay <- unique(fdata$tradeDate)
extract <- lapply(tradeDay, #mc.cores = corestouse,
                  function(x){
                    tmp <- fdata[which(as.Date(fdata$tradeDate) == x),]
                    tmp <- tmp[order(tmp$expiryDate),]
                    h1 <- grep(x, tmp$tradeDate)
                    if(tmp$daysToExpiry[h1[1]] >= 15) {
                      symbolFut <- tmp$symbolFut[h1[1]]
                      tradeDate <- as.Date(tmp$tradeDate[h1[1]])
                      expiryDate <- as.Date(tmp$expiryDate[h1[1]])
                      futpriceNear <- tmp$close[h1[1]]
                      futpriceNext <- tmp$close[h1[2]]
                      tradedValue <- tmp$tradedValueInLacs[h1[1]]
                      noOfTrades <- tmp$noOfTrades[h1[1]]
                      qtyTraded <- tmp$qtyTraded[h1[1]]
                      oiQty <- tmp$openInterestInQty[h1[1]]
                      basisCentre <- tmp$basisCentre[h1[1]]
                      daysToexpiry <- tmp$daysToExpiry[h1[1]]
                    } else if(tmp$daysToExpiry[h1[1]]<15 &&
                              nrow(tmp) <= 2){ # Check for missing data
                      return(NULL)
                    } else if(tmp$daysToExpiry[h1[1]]<15){
                      symbolFut <- tmp$symbolFut[h1[2]]
                      tradeDate <- as.Date(tmp$tradeDate[h1[2]])
                      expiryDate <- as.Date(tmp$expiryDate[h1[2]])
                      futpriceNear <- tmp$close[h1[2]]
                      futpriceNext <- tmp$close[h1[3]]
                      tradedValue <- tmp$tradedValueInLacs[h1[2]]
                      noOfTrades <- tmp$noOfTrades[h1[2]]
                      qtyTraded <- tmp$qtyTraded[h1[2]]
                      oiQty <- tmp$openInterestInQty[h1[2]]
                      basisCentre <- tmp$basisCentre[h1[2]]
                      daysToexpiry <- tmp$daysToExpiry[h1[2]]
                    }
                    return(data.frame(symbolFut, tradeDate, expiryDate,
                                      futpriceNear, futpriceNext,
                                      tradedValue, noOfTrades, qtyTraded,
                                      oiQty, basisCentre, daysToexpiry))
                  })
extract <- do.call(rbind, extract)

save(extract, file = "maize_nzm_fut.RData")
