#reconstructing WGFP Hydrology
library(tidyverse) 
library(lubridate)
library(sf)
library(readxl)
library(dataRetrieval) #this is the usgs package for getting up to date data

#make function
#"USGS-09034250", #code for windy gap
codeID = "09019000"
getDailyand15MinUSGSData <- function(codeID, startDate = "2020-08-06", endDate = Sys.Date(), waterTemp = TRUE) {
  ##windy gap/hitching post 
  #reading in USGS data with upt to date data
  USGSDataDaily <- read_waterdata_daily(monitoring_location_id = paste0("USGS-", codeID),
                                        parameter_code = c("00060", "00010"), #this is parameter codes for discharge and celsius water temp; more can be added if needed. https://help.waterdata.usgs.gov/codes-and-parameters/parameters
                                        time = c(startDate, endDate)
  )
  USGSDataDaily <- USGSDataDaily %>%
    st_drop_geometry() %>%
    pivot_wider(id_cols = time, names_from = parameter_code, values_from = value, values_fn = ~ mean(.x, na.rm = TRUE)) %>%
    rename(Flow = `00060`, 
           Date = time)
  
  if(waterTemp){
    USGSDataDaily <- USGSDataDaily %>%
      rename(tempC = `00010`)
    USGSDataDaily <- USGSDataDaily %>%
      mutate(WtempF = (tempC * 9/5) + 32)
  }
  
  #sometimes this can fail if USGS is having issues on their end
  #maybe this function readNWISuv should be replaced with read_waterdata_latest_continuous but no error on that yet. monitor
  USGSData <- readNWISuv(siteNumbers = codeID, #code for windy gap
                         parameterCd = c("00060", "00010"), #this is parameter code for discharge; more can be added if needed
                         startDate = startDate, #if you want to do times it is this format: "2014-10-10T00:00Z",
                         endDate = endDate,
                         tz = "America/Denver")
  
  USGSData <- renameNWISColumns(USGSData) 
  
  if(waterTemp){
    USGSData <- USGSData %>%
      mutate(USGSWatertemp = (Wtemp_Inst * 9/5) + 32) 
  }
  USGSData <- USGSData %>%
    rename(USGSDischarge = Flow_Inst)
  #this is to make attaching these readings to detections later
  USGSData$dateTime <- lubridate::force_tz(USGSData$dateTime, tzone = "UTC") 
  
  return(list("USGSData" = USGSData, 
              "USGSDataDaily" = USGSDataDaily))
}

##windy gap/hitching post 

windayGpData <- getDailyand15MinUSGSData("09034250")
windyGap <- windayGpData$USGSData %>%
  rename(`CR Below WG Flow` = Flow_Inst_cd)
windyGapDaily <- windayGpData$USGSDataDaily %>%
  select(Date, Flow) %>%
  rename(`CR Below WG Flow` = Flow)

belowGranbyData <- getDailyand15MinUSGSData("09019000", waterTemp = FALSE)
belowGranby <- belowGranbyData$USGSData %>%
  rename(`CR Below Lake Granby Flow` = Flow_Inst_cd)
belowGranbyDaily <- belowGranbyData$USGSDataDaily %>%
  select(Date, Flow) %>%
  rename(`CR Below Lake Granby Flow` = Flow)

nearGranbyData <- getDailyand15MinUSGSData("09019500") 
nearGranby <- nearGranbyData$USGSData %>%
  rename(`CR Near Lake Granby Flow` = Flow_Inst_cd)
nearGranbyDaily <- nearGranbyData$USGSDataDaily %>%
  select(Date, Flow) %>%
  rename(`CR Near Lake Granby Flow` = Flow)

#willow creek only has discharge, no temperature
willowCreekData <- getDailyand15MinUSGSData("09021000", waterTemp = FALSE)
willowCreek <- willowCreekData$USGSData %>%
  rename(`WC Below Res Flow` = Flow_Inst_cd)
willowCreekDaily <- willowCreekData$USGSDataDaily %>%
  select(Date, Flow) %>%
  rename(`WC Below Res Flow` = Flow)

#bring in fraer data to join
WGFP_Hydrology_Qdaily_2020_to_2022 <- read_excel("Inputs/WGFP_Hydrology_Qdaily_2020_to_2022.xlsx", 
                                                 sheet = "data_all")

fraserOnly <- WGFP_Hydrology_Qdaily_2020_to_2022 %>%
  select(Date, `FR Above Con`)

allDataDaily <- windyGapDaily %>%
  left_join(fraserOnly, by = "Date") %>%
  left_join(belowGranbyDaily, by = "Date") %>%
  left_join(nearGranbyDaily, by = "Date") %>%
  left_join(willowCreekDaily, by = "Date")

allDataDaily1 <- allDataDaily %>%
  mutate(AssumedFraserFlow = `CR Below WG Flow` - (`CR Near Lake Granby Flow` + `WC Below Res Flow`), 
         difference = `FR Above Con` -  AssumedFraserFlow)

write_csv(allDataDaily1, "Outputs/reconstructedHydrology.csv")
summary(allDataDaily1$difference)
