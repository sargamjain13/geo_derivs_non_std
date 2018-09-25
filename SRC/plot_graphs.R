## The follwoing script consists of functions to plot various time
## series graphs for the policy paper. The function calls are towards
## the end of the script.

options(scipen = 999)
###############
## Libraries ##
###############
library(zoo)

#####################
## Input variables ##
#####################
basedir <- "../../DATA/POLICY_PAPER/FINAL_DATABASE/"
data_path <- "../../DATA/POLICY_PAPER/"
graph_path <- "../../../DOC/POLICY_PAPER/GRAPHS/"

spot_commodities <- c("CORIANDER", "CUMMIN", "MAIZE", "MUSTARD",
                      "SOYBEAN", "TURMERIC")

commdetails <- data.frame(commname = spot_commodities,
                          futIntroDate = as.Date(
                              c("2008-08-11", "2005-02-03",
                                "2005-01-05", "2003-12-15",
                                "2003-12-15", "2004-07-24")))
commdetails <- commdetails[!(commdetails$commname == "CUMMIN"), ]

contract_dummies <- data.frame(
  "commname" = commdetails$commname,
  "contract1_start" = c("2008-08-11", "2005-01-05", "2003-12-15", "2003-12-15", "2004-07-24"),
  "contract2_start"= c("2016-12-31", "2010-05-21", "2010-11-10", "2015-02-02", "2016-12-31"),
  "contract3_start" = c("2016-12-31", "2013-02-08", "2015-01-01", "2016-12-31", "2016-12-31"),
  "contract1_end" = c("2016-12-31", "2010-09-20", "2011-01-20", "2016-12-31", "2016-12-31"),
  "contract2_end"= c("2016-12-31", "2013-01-01", "2016-12-31", "2016-12-31", "2016-12-31"),
  "contract3_end"  = c("2016-12-31", "2016-12-31", "2015-12-11", "2016-12-31", "2016-12-31"))

###########################
## Load master data sets ##
###########################
load(paste0(basedir, "pdiff_with_intll_prices.RData"))
names(pdiff_with_intll_prices) <- commdetails$commname

#########################################
## Functions to plot time seried graphs #
#########################################

## Plot prices  ##
plot_prices <- function(cdata, futIntroDate, filename){
    if(unique(cdata$commodity) == "MUSTARD"){
        cdata$futpriceNear[cdata$arrival_date < as.Date("2011-01-30")] <- cdata$futpriceNear[cdata$arrival_date < as.Date("2011-01-30")] * 5
    }
    yhilo <- range(na.omit(cdata$minimum_distance_centerprice),
                   na.omit(cdata$futpriceNear))
    yhilo[2] <- yhilo[2] + (yhilo[2] - yhilo[1])*0.5

    png(filename, width = 400, height = 250, pointsize = 32)
    par(mar = c(2, 5, 2.5, 2), mgp = c(3.5, 0.6, 0))
    
    plot(cdata$arrival_date, cdata$minimum_distance_centerprice,
         type = "l", lwd = 1, col = "black", ylim = yhilo,
         cex.lab = 1.3, cex.axis = 1.3,
         las = 1, xlab = "",
         ylab = expression(bold("Price (Rs. per quintal)")))
    lines(cdata$arrival_date, cdata$minimum_distance_centerprice,
         type = "l", lwd = 1, col = "black")
    lines(cdata$arrival_date, cdata$futpriceNear,
          type = "l", col = "firebrick", lwd = 3.5)
    abline(v = futIntroDate, col = "orange", lwd = 3.5, lty = 1)
    mtext(expression(bold("Futures launch")), side = 3, line = 1.4,
          at = futIntroDate, cex = 1.3)
    mtext(futIntroDate, side = 3, line = 0.1,
          at = futIntroDate,
          cex = 1.3)
    legend(x = "topright", lty = c(1, 1), lwd = c(2.5, 2.5),
           legend =
               c("Price at nearest centers",
                 "Near-month futures price"),
           col = c("black", "firebrick"),
           bty = "n",
           cex = 1)
    dev.off()
    return(NULL)
}

## Plot price differences ##
plot_price_diff <- function(cdata, futIntroDate, filename){
    cdata$yr_mnth <- substr(cdata$arrival_date, 1, 7)
    unique_mnths <- unique(cdata$yr_mnth)
    forevery_yrmnth <- lapply(unique_mnths, function(x){
        print(x)
        tmp <- cdata[cdata$yr_mnth == x, c("yr_mnth", "price_diff")]
        ans <- data.frame("yr_mnth" = x,
                          "price_diff" = median(tmp$price_diff))
        return(ans)
    })
    forevery_yrmnth <- do.call(rbind, forevery_yrmnth)
    forevery_yrmnth$yr_mnth <- as.yearmon(
        as.character(forevery_yrmnth$yr_mnth), "%Y-%m")
    yhilo <- range(na.omit(forevery_yrmnth$price_diff))
    png(filename, width = 400, height = 250, pointsize = 32)
    par(mar = c(2, 5, 2.5, 2), mgp = c(3.5, 0.6, 0))
    plot(forevery_yrmnth$yr_mnth, forevery_yrmnth$price_diff,
         type = "l", lwd = 2.5, col = "darkblue", ylim = yhilo,
         cex.lab = 1.3, cex.axis = 1.3,
         las = 1, xlab = "",
         ylab = expression(bold("Price difference (Rs. per quintal)")))
    abline(v = as.yearmon(substr(futIntroDate, 1, 7), "%Y-%m"),
           col = "black", lwd = 2, lty = 1)
    abline(v = pretty(forevery_yrmnth$yr_mnth), col = "darkgray",
           lty = 2, lwd = 1)
    abline(h = pretty(forevery_yrmnth$price_diff), col = "darkgray",
           lty = 2, lwd = 1)
    mtext(expression(bold("Futures launch")), side = 3, line = 1.2,
          at = as.yearmon(substr(futIntroDate, 1, 7), "%Y-%m"),
          cex = 1.3)
    mtext(as.yearmon(substr(futIntroDate, 1, 7), "%Y-%m"), side = 3,
          line = 0.2,
          at = as.yearmon(substr(futIntroDate, 1, 7), "%Y-%m"),
          cex = 1.3)
    dev.off()
    return(NULL)
}


## Plot arrivals ##
plot_arrivals <- function(cdata, futIntroDate, filename){
    png(filename, width = 400, height = 250, pointsize = 32)
    par(mar = c(2, 5, 2.5, 2), mgp = c(2, 0.6, 0))
    plot(cdata$date, cdata$arrival_diff, pch = 20, cex = 1.5,
         col = "darkblue", cex.lab = 1.5, lty = 2,
         cex.axis = 1.3, las = 1, xlab = "",
         ylab = expression(bold("Mean deviation" ~
                               (A[monthly] ~ "-" ~ bar(A)[year]))))
    lines(cdata$date, cdata$arrival_diff,
          type = "l", lwd = 1, lty = 2, col = "darkblue")
    abline(v = futIntroDate, col = "black", lwd = 2, lty = 1)
    abline(h = 0, col = "black", lwd = 2, lty = 1)
    mtext(expression(bold("Futures launch")), side = 3, line = 1.2,
          at = futIntroDate, cex = 1.5)
    mtext(futIntroDate, side = 3, line = 0.2, at = futIntroDate,
          cex = 1.3)
    dev.off()
    return(NULL)
}

## Plot price volatility ##
plot_pvol <- function(cdata, futIntroDate, filename){
    png(filename, width = 400, height = 250, pointsize = 32)
    par(mar = c(2, 5, 2.5, 2), mgp = c(3, 0.6, 0))
    plot(cdata$date, cdata$price_diff, pch = 20, cex = 1.2,
         col = "darkred", cex.lab = 1.5, lty = 2,
         cex.axis = 1.3, las = 1, xlab = "",
         ylab = expression(bold("Mean deviation" ~
                               (P[monthly] ~ "-" ~ bar(P)[year]))))
    lines(cdata$date, cdata$price_diff,
          type = "l", lwd = 1, lty = 2, col = "darkred")
    abline(v = futIntroDate, col = "black", lwd = 2, lty = 1)
    abline(h = 0, col = "black", lwd = 2, lty = 1)
    mtext(expression(bold("Futures launch")), side = 3, line = 1.2,
          at = futIntroDate,
          cex = 1.5)
    mtext(futIntroDate, side = 3, line = 0.2, at = futIntroDate,
          cex = 1.3)
    dev.off()
    return(NULL)
}

## Plot NCDEX monthly average trading volumes ##
plot_ncdex_vol <- function(commodity, futIntroDate, regimes, filename){
    commodity <- as.character(commodity)
    cat("Plotting for:", commodity, "\n")
    cdata <- read.csv(file = paste0(data_path,
                                   "NCDEX_VOLUMES/CLEAN_DATA/vol_",
                                   commodity, ".csv"),
                      header = TRUE)
    cdata <- cdata[ , c("yr_month", "qty_traded")]
    cdata$qty_traded <- cdata$qty_traded / 25
    tmp <- data.frame("yr_month" = c(sapply(2002:2015, paste0, 
                                          c("-01", "-02", "-03", "-04",
                                            "-05", "-06", "-07", "-08",
                                            "-09", "-10", "-11",
                                            "-12"),
                                          sep = "")))
    cdata <- merge(tmp, cdata, by = "yr_month", all.x = TRUE)
    cdata$yr_month <- as.yearmon(as.character(cdata$yr_month))
    cdata$qty_traded[is.na(cdata$qty_traded)] <- 0
    png(filename, width = 400, height = 250, pointsize = 32)
    par(mar = c(2, 5, 2.5, 2), mgp = c(3.5, 0.6, 0))
    plot(cdata$yr_month, cdata$qty_traded, col = "darkblue", lwd = 1.5,
         lty = 2, type = "l", xlab = "", las = 1,
         ylab = "Monthly traded volumes (MT)")
    points(cdata$yr_month, cdata$qty_traded, col = "darkblue",
           pch = 16)
    abline(v = cdata$yr_month[cdata$yr_month == substr(futIntroDate,
                                                       1, 7)],
           col = "black", lty = 1, lwd = 3)
    mtext("Futures introduced", side = 3, line = 0.5, cex = 1.2,
          at = as.yearmon(substr(futIntroDate, 1, 7)))
    abline(v = pretty(cdata$yr_month), lty = 2, lwd = 1,
           col = "darkgray")
    abline(h = pretty(cdata$qty_traded), lty = 2, lwd = 1,
           col = "darkgray")
    abline(v = as.yearmon(as.character(regimes$contract1_start)),
           lty = 2, lwd = 3, col = "orange")
    abline(v = as.yearmon(as.character(regimes$contract2_start)),
           lty = 2, lwd = 3, col = "orange")
    abline(v = as.yearmon(as.character(regimes$contract3_start)),
           lty = 2, lwd = 3, col = "orange")
    if(commodity %in% c("CORIANDER", "TURMERIC")){
        mtext("C1", side = 3, line = 1.5, cex = 1.2, col = "red",
              at = as.yearmon(as.character(regimes$contract1_start)))
    }
    if(commodity == "SOYBEAN"){
        mtext("C1", side = 3, line = 1.5, cex = 1.2, col = "red",
              at = as.yearmon(as.character(regimes$contract1_start)))
        mtext("C2", side = 3, line = 1.5, cex = 1.2, col = "red",
              at = as.yearmon(as.character(regimes$contract2_start)))
    }
    if(commodity %in% c("MUSTARD", "MAIZE")){
        mtext("C1", side = 3, line = 1.5, cex = 1.2, col = "red",
              at = as.yearmon(as.character(regimes$contract1_start)))
        mtext("C2", side = 3, line = 1.5, cex = 1.2, col = "red",
              at = as.yearmon(as.character(regimes$contract2_start)))
        mtext("C3", side = 3, line = 1.5, cex = 1.2, col = "red",
              at = as.yearmon(as.character(regimes$contract3_start)))
    }
    dev.off()
}

####################
## Function calls ##
####################

## Plot prices ##
for(i in 1:nrow(commdetails)){
    comm <- as.character(commdetails$commname[i])
    plot_prices(cdata = pdiff_with_intll_prices[[comm]],
                futIntroDate = commdetails[i, "futIntroDate"],
                filename = paste0(graph_path, "prices", comm,
                    ".png"))
}

## Plot price differences ##
for(i in 1:nrow(commdetails)){
    comm <- as.character(commdetails$commname[i])
    plot_price_diff(cdata = pdiff_with_intll_prices[[comm]],
                    futIntroDate = commdetails[i, "futIntroDate"],
                    filename = paste0(graph_path, "price_diff", comm,
                                      ".png"))
}

## Plot NCDEX traded volumes ##
for(i in 1:nrow(commdetails)){
    plot_ncdex_vol(
        commodity = commdetails$commname[i],
        futIntroDate = commdetails$futIntroDate[i],
        regimes = contract_dummies[i, 1:4],
        filename = paste0(graph_path, "ncdex_volumes_",
                          commdetails$commname[i], ".png"))
}
