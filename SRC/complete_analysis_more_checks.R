####################################
## Function to pick basis centres ##
####################################

pick_polling_center_state <- function(spot){
    center <- polling_centers[tolower(polling_centers$Commodity) %in% tolower(spot), ]
    center$address <- paste0(center$Basis_C, ", ", center$Basis_C_state)
    return(center)
}

pick_delivery_center_state <- function(spot){
    center <- delivery_centers[tolower(delivery_centers$Commodity) %in% tolower(spot), ]
    center$address <- paste0(center$Delivery_C, ", ",
                             center$Delivery_C_state)
    return(center)
}

#########################################################
## Function to clean names sent to distance calculator ##
#########################################################
clean_names <- function(pdata){
    pdata <- str_replace_all(pdata, "[[:punct:]]", " ")
    pdata <- gsub("   ", " ", pdata)
    pdata <- gsub("  ", " ", pdata)
    pdata <- gsub(" ", "+", pdata)
    return(pdata)
}
                            
#####################################
## Function to calculate distances ##
#####################################

distance_calculator <- function(origin_p, destination_p){
  origin_place <- clean_names(origin_p)
  destination_place <- clean_names(destination_p)
  tmp <- data.frame(matrix(ncol = 2, nrow = 0))
  colnames(tmp) <- c("origin", "distance")
  for(y in 1:length(origin_place)){
      print(y)
      ans <- gmapsdistance(origin = origin_place[y],
                           destination = destination_place,
                           mode = "driving",
                           traffic_model = "best_guess")$Distance
      tmp[y, "origin"] <- origin_p[y]
      tmp[y, "distance"] <- ans
  }
   return(tmp)
}

###################################
## Function to clean mandi names ##
###################################
cleanMandiNames <- function(mdata){
    mdata <- gsub("\\s*\\([^\\)]+\\)", "", mdata)
    mdata[grep("Ramaganj ", mdata)] <- "Ramganjmandi, Rajasthan"
    mdata[grep("Ramagang ", mdata)] <- "Ramganjmandi, Rajasthan"
    mdata[grep("ganganagar", mdata)] <- "Sri Ganganagar, Rajasthan"  
    mdata[grep("Sriganganagar", mdata)] <- "Sri Ganganagar, Rajasthan" 
    mdata[grep("Nizamabad", mdata)] <- "Nizamabad, Telangana"
    return(mdata)
}

########################################
## Function to clean names of centers ##
########################################
cleanCenterNames <- function(pdata){
    allColumns <- colnames(pdata)[grep("address_", colnames(pdata))]
    for(i in allColumns){
        ans <- unique(pdata[ , i])
        print(ans)
        ans <- gsub(" ", "", ans)
        ans <- gsub(",", ", ", ans)
        sw <- strsplit(as.character(ans), ", ")
        if(grepl("^[[:upper:]]+$", sw[[1]][2]) == TRUE){
            sw[[1]][2] <- tolower(sw[[1]][2])
            sw[[1]][2] <- paste0(toupper(substring(sw[[1]][2],
                                                   1, 1)),
                                 substring(sw[2], 2))
            ans <- paste0(sw[[1]][1], ", ", sw[[1]][2])
        } else {
            ans <- paste0(sw[[1]][1], ", ", sw[[1]][2])
        }
        ans[grep("ganganagar", ans)] <- "Sri Ganganagar, Rajasthan"
        ans[grep("Sriganganagar", ans)] <- "Sri Ganganagar, Rajasthan"
        ans[grep("Nizamabad", ans)] <- "Nizamabad, Telangana"
        pdata[ , i] <- ans
    }
    return(pdata)
}
