library(stargazer)

regression_model <- function(cdata, monthEffect, stateYearInteraction,
                             contractChanges,
                             contractChangesWithInteraction,
                             marketShare_center,
                             center_dummyInteraction, pooled,
                             dependentVariable, includeArrivals){
    
    xVar <- c("minimum_distance", "log(qty_traded)", "center_dummy",
              "marketShare", "factor(state)", "factor(year)",
              "lag1m_neg_norm_surplus", "lag1m_pos_norm_surplus",
              "lag2m_neg_norm_surplus", "lag2m_pos_norm_surplus",
              "lag3m_neg_norm_surplus", "lag3m_pos_norm_surplus",
              "lag4m_neg_norm_surplus ", "lag4m_pos_norm_surplus",
              "lag5m_neg_norm_surplus", "lag5m_pos_norm_surplus",
              "lag6m_neg_norm_surplus ", "lag6m_pos_norm_surplus",
              "lag7m_neg_norm_surplus", "lag7m_pos_norm_surplus",
              "lag8m_neg_norm_surplus ", "lag8m_pos_norm_surplus",
              "lag9m_neg_norm_surplus", "lag9m_pos_norm_surplus",
              "lag10m_neg_norm_surplus ", "lag10m_pos_norm_surplus",
              "lag11m_neg_norm_surplus", "lag11m_pos_norm_surplus",
              "lag12m_neg_norm_surplus ", "lag12m_pos_norm_surplus",
              "factor(weekday)")

    if (includeArrivals == TRUE){
        xVar <- c(xVar, "log(arrivalstonnes)")
    }

    if (center_dummyInteraction == TRUE){
        xVar <- c(xVar, "center_dummy*log(qty_traded)")
    }

    if (stateYearInteraction == TRUE){
        xVar <- c(xVar, "factor(year)*factor(state)")
    }

    if (monthEffect == TRUE){
        xVar <- c(xVar, "factor(month)")
    }

    if (contractChanges == TRUE){
        xVar <- c(xVar, "c1_dummy", "c2_dummy", "c3_dummy")
    }

    if (contractChangesWithInteraction == TRUE){
        xVar <- c(xVar, "c1_dummy", "c2_dummy", "c3_dummy",
                  "c1_dummy* log(qty_traded)",
                  "c2_dummy* log(qty_traded)",
                  "c3_dummy*log(qty_traded)")
    }

    if (marketShare_center == TRUE){
        xVar <- c(xVar, "marketShare_center")
    }

    if (pooled == TRUE){
        xVar <- c(xVar, "factor(commodity)")
    }

    ## if (unique(cdata$commodity) == "CORIANDER"){
    ##     xVar <- xVar[!(xVar %in% grep("c1_dummy", xVar, value= TRUE))]
    ##     ## Remove coriander because c1_dummy essentially captures the
    ##     ## introduction date of the contract. --or maybe we do not
    ##     ## need to.
    ## }
        
    basicRegSpec <- as.formula(paste0(dependentVariable, " ~ ", paste(xVar, collapse = "+")))
    
    print(basicRegSpec)
    m1 <- lm(basicRegSpec, data = cdata)
    u1 <- coeftest(m1, vcov = vcovHC(m1, "HC1"))
    
    vNames <- c("minimum_distance", "log(arrivalstonnes)",
                "log(qty_traded)", "marketShare",
                "marketShare_center", "center_dummy",
                "log(qty_traded):center_dummy",
                "log(qty_traded):c1_dummy",
                "log(qty_traded):c2_dummy",
                "log(qty_traded):c3_dummy", "c1_dummy", "c2_dummy",
                "c3_dummy")
    
    r1 <- rbind(round(u1[rownames(u1) %in% vNames, c(1, 2, 4)], 4))
    
    rownames(u1)[which(rownames(u1) == "minimum_distance")] <- "Distance"
    rownames(u1)[which(rownames(u1) == "log(arrivalstonnes)")] <- "ln(Arrivals)"
    rownames(u1)[which(rownames(u1) == "log(qty_traded)")] <- "ln(NCDEX Volumes)"
    rownames(u1)[which(rownames(u1) == "marketShare")] <- "Market share mandi"
    rownames(u1)[which(rownames(u1) == "marketShare_center")] <- "Market share center"
    rownames(u1)[which(rownames(u1) == "center_dummy")] <- "Center dummy"
    rownames(u1)[which(rownames(u1) == "log(qty_traded):center_dummy")] <- "Center NCDEXVolumes Intrn"
    rownames(u1)[which(rownames(u1) == "c1_dummy")] <- "C1 dummy"
    rownames(u1)[which(rownames(u1) == "c2_dummy")] <- "C2 dummy"
    rownames(u1)[which(rownames(u1) == "c3_dummy")] <- "C3 dummy"
    rownames(u1)[which(rownames(u1) == "log(qty_traded):c1_dummy")] <- "C1 NCDEXVolumes Intrn"
    rownames(u1)[which(rownames(u1) == "log(qty_traded):c2_dummy")] <- "C2 NCDEXVolumes Intrn"
    rownames(u1)[which(rownames(u1) == "log(qty_traded):c3_dummy")] <- "C3 NCDEXVolumes Intrn"
    
    toadd <- rbind(c(summary(m1)$adj.r.squared, NA, NA),
                   c(sum(cdata$c1_dummy), NA, NA),
                   c(sum(cdata$c2_dummy), NA, NA),
                   c(sum(cdata$c3_dummy), NA, NA),
                   c(sum(cdata$center_dummy), NA, NA),
                   c(nrow(cdata), NA, NA))
    
    rownames(toadd) <- c("Adj. R2", "Obs. C1", "Obs. C2", "Obs. C3",
                         "Obs. center_dummy", "No. of obs")
    
    toadd <- round(toadd, 2)
    r1 <- rbind(r1, toadd)
    
    return(list(results = r1,
                lmResults = m1,
                robustResults = u1))
}

stargazerOutput <- function(resultsObject, dat, resultsFile,
                            resultsTitle, dependentVariable){
    
    xVar <- c("minimum_distance", "log(qty_traded)", "center_dummy",
              "marketShare", "factor(state)", "factor(year)",
              "lag1m_neg_norm_surplus", "lag1m_pos_norm_surplus",
              "lag2m_neg_norm_surplus", "lag2m_pos_norm_surplus",
              "lag3m_neg_norm_surplus", "lag3m_pos_norm_surplus",
              "lag4m_neg_norm_surplus ", "lag4m_pos_norm_surplus",
              "lag5m_neg_norm_surplus", "lag5m_pos_norm_surplus",
              "lag6m_neg_norm_surplus ", "lag6m_pos_norm_surplus",
              "lag7m_neg_norm_surplus", "lag7m_pos_norm_surplus",
              "lag8m_neg_norm_surplus ", "lag8m_pos_norm_surplus",
              "lag9m_neg_norm_surplus", "lag9m_pos_norm_surplus",
              "lag10m_neg_norm_surplus ", "lag10m_pos_norm_surplus",
              "lag11m_neg_norm_surplus", "lag11m_pos_norm_surplus",
              "lag12m_neg_norm_surplus ", "lag12m_pos_norm_surplus",
              "factor(weekday)", "c1_dummy", "c2_dummy", "c3_dummy")

    dat <- na.omit(dat[, colnames(dat) %in% xVar])
    
    covariates_label <- c("Distance", "Arrivals",
                          "NCDEX Volumes", "Market share mandi",
                          "Market share center", "Center dummy",
                          "Center NCDEXVolumes Intrn","C1 dummy",
                          "C2 dummy", "C3 dummy", "C1 NCDEXVolumes Intrn",
                          "C2 NCDEXVolumes Intrn", "C3 NCDEXVolumes Intrn")
    
    toadd <- list(c("State Effects", "YES", "YES", "YES", "YES", "YES",
                    "YES"),
                  c("Year Effects", "YES", "YES", "YES", "YES", "YES",
                    "YES"),
                  c("Weekday Effects", "YES", "YES", "YES", "YES", "YES",
                    "YES"),
                  c("State Year Interaction", "NO", "NO", "NO",
                    "NO", "NO", "YES"),
                  c("Adj. R2",
                    round(c(summary(resultsObject[[1]][["lmResults"]])$adj.r.squared,
                            summary(resultsObject[[2]][["lmResults"]])$adj.r.squared,
                            summary(resultsObject[[3]][["lmResults"]])$adj.r.squared,
                            summary(resultsObject[[4]][["lmResults"]])$adj.r.squared,
                            summary(resultsObject[[5]][["lmResults"]])$adj.r.squared,
                            summary(resultsObject[[6]][["lmResults"]])$adj.r.squared),
                          2)),
                  c("FStat pval",
                    round(c(anova(resultsObject[[1]][["lmResults"]])$'Pr(>F)'[1],
                            anova(resultsObject[[2]][["lmResults"]])$'Pr(>F)'[1],
                            anova(resultsObject[[3]][["lmResults"]])$'Pr(>F)'[1],
                            anova(resultsObject[[4]][["lmResults"]])$'Pr(>F)'[1],
                            anova(resultsObject[[5]][["lmResults"]])$'Pr(>F)'[1],
                            anova(resultsObject[[6]][["lmResults"]])$'Pr(>F)'[1]),
                          2)),
                  c("Obs. C1", rep(sum(dat["c1_dummy"]), 6)),
                  c("Obs. C2", rep(sum(dat["c2_dummy"]), 6)),
                  c("Obs. C3", rep(sum(dat["c3_dummy"]), 6)),
                  c("Obs. center dummy", rep(sum(dat["center_dummy"]), 6)),
                  c("No. of Obs.", rep(nrow(dat), 6)))

    commname <- unique(dat$commodity)
    
    if (length(commname) > 1){
        commname <- "ALL"
    }

    if (dependentVariable == "abs(price_diff)"){
        dependentVariable <- "\\textbf{Price differential from the nearest center}"
    }

    if (dependentVariable == "abs(std_arrivals)"){
        dependentVariable <- "\\textbf{Arrivals variation through the year}"
    }
    
    if (dependentVariable == "abs(std_minprice)"){
        dependentVariable <- "\\textbf{Spot price variation through the year}"
    }
    
    ans <- capture.output(stargazer(resultsObject[[1]][["robustResults"]],
                                    resultsObject[[2]][["robustResults"]],
                                    resultsObject[[3]][["robustResults"]],
                                    resultsObject[[4]][["robustResults"]],
                                    resultsObject[[5]][["robustResults"]],
                                    resultsObject[[6]][["robustResults"]],
                                    order = covariates_label,
                                    keep = covariates_label, style = "qje",
                                    add.lines = toadd,
                                    dep.var.caption = commname,
                                    dep.var.labels = dependentVariable,
                                    out = resultsFile,
                                    model.numbers = TRUE,
                                    table.layout = "-d#-t-a-s",
                                    title = resultsTitle))
    return(NULL)
}
