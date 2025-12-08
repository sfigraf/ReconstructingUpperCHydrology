#reconstructing WGFP Hydrology
library(tidyverse) 
library(lubridate)
library(sf)
library(readxl)
library(dataRetrieval) #this is the usgs package for getting up to date data
library(writexl)

# nearGranbyGageOldNWISInstant <- read_csv("Inputs/nearGranbyGageOldNWISInstant.csv")
#can only get these fall dates, otherwise if you go back more than 120 days it will show up as "Ssn"
#only works for isntant as well
fall2025nearGranbyGageNWISInstant <- read_csv("Inputs/fall2025nearGranbyGageNWISInstant.csv")
WindyGapPumpingRecord <- read_csv("Inputs/WindyGapPumpingRecord.csv")

fall2025nearGranbyGageNWISInstant1 <- fall2025nearGranbyGageNWISInstant %>%
  mutate(datetime = mdy_hm(datetime), 
         Date = as.Date(datetime)) %>%
  group_by(Date) %>%
  summarise(flowNearGRanbyNWISOld = mean(as.numeric(`60`), na.rm = TRUE))

# nearGranbyGageOldNWISInstant1 <- nearGranbyGageOldNWISInstant %>%
#   mutate(datetime = mdy_hm(datetime), 
#          date = as.Date(datetime)) %>%
#   group_by(date) %>%
#   summarise(flowNearGRanbyNWISOld = mean(as.numeric(`60`), na.rm = TRUE))
#assuming NA pumping values are not pumoping
#only goers back to June 2021
WindyGapPumpingRecord1 <- WindyGapPumpingRecord %>%
  mutate(Date = mdy(meas_date), 
         windyGapPumpingFLow = replace_na(as.numeric(`Streamflow Value`), 0)) %>%
  select(Date, windyGapPumpingFLow)

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
fraserCDSS <- read_csv("Inputs/moreFraserFlows_CDSS.csv") %>%
  mutate(Date = mdy(meas_date), 
         FraserCDSSFlow = `Streamflow Value`) %>%
  select(Date, FraserCDSSFlow)

fraserOnly <- WGFP_Hydrology_Qdaily_2020_to_2022 %>%
  select(Date, `FR Above Con`) %>%
  right_join(fraserCDSS, by = "Date") %>%
  #merge() might be more tidy but whatever
  mutate(`Fraser Flow` = if_else(!is.na(`FR Above Con`), `FR Above Con`, FraserCDSSFlow), 
         #gettin rid of weird 0 cfs flow 
         `Fraser Flow` = ifelse(`Fraser Flow` < 1, NA, `Fraser Flow`)) %>%
  select(Date, `Fraser Flow`)

allDataDaily <- windyGapDaily %>%
  left_join(fraserOnly, by = "Date") %>%
  left_join(belowGranbyDaily, by = "Date") %>%
  left_join(nearGranbyDaily, by = "Date") %>%
  left_join(willowCreekDaily, by = "Date") %>%
  left_join(fall2025nearGranbyGageNWISInstant1, by = "Date") %>%
  left_join(WindyGapPumpingRecord1, by = "Date")

allDataDaily1 <- allDataDaily %>%
  mutate(
         
         #granby difference in gages for all data
         differenceCFSGranbyGages = `CR Near Lake Granby Flow` - `CR Below Lake Granby Flow`, 
         differencePercentGranbyGages = round((differenceCFSGranbyGages/`CR Below Lake Granby Flow`)*100, 2),
         
         #granby difference in gagews for fall 2025 instanteous data
         differenceCFSFall2025Granby = flowNearGRanbyNWISOld - `CR Below Lake Granby Flow`, 
         differenceCFSFall2025Granbypercent = round((differenceCFSFall2025Granby/`CR Below Lake Granby Flow`)*100, 2)
         
         # comments = ifelse(is.na(windyGapPumpingFLow), "No Windy Gap Pumping Data Avaialble for this time period", "")
         )
##differences between assumed fraser and actual for the data we have
# mean(allDataDaily1$differenceFraserCFS, na.rm = T)
# percentageDifFraser <- mean(allDataDaily1$differenceFraserPercent, na.rm = T)

###all differnece between granby gages
mean(allDataDaily1$differenceCFSGranbyGages, na.rm = T)
percentageChangeAllGranby <- mean(allDataDaily1$differencePercentGranbyGages, na.rm = T)

#for fall 2025, data is typically off only by 2 cfs, 
# but that equates to an average of 14%
###So if we are going to extrapolate fall values, maybe it would make sense to add 14% more flow to the "below WG gage"
#for times when we use that
mean(allDataDaily1$differenceCFSFall2025Granby, na.rm = T)
percentageChangeFall2025 <- mean(allDataDaily1$differenceCFSFall2025Granbypercent, na.rm = T)

granbygagesOveralppingDates <- allDataDaily1 %>%
  filter(!is.na(differencePercentGranbyGages))
allDataDaily2 <- allDataDaily1 %>%
  #if data is available for the near granby gagae, use that bc it's most treliable before adding into willow creek
  #if that's not available, use the isntanteous flow from the legacy site (only available fall 2025)
  #if that's not avaialble, use the correction based off below lake granby gage; 
  ###NOT SURE IF WE WANT TO USE CORRECTION FOR ALL DATA (percentageChangeAllGranby, -1% ish) OR FOR JUST FALL 2025 (percentageChangeFall2025, 14%)
  #for now using percentageChangeFall2025
  mutate(`Actual/Assumed UpperC Above WillowCreek Flow` = case_when(!is.na(`CR Near Lake Granby Flow`) ~ `CR Near Lake Granby Flow`, 
                                                       !is.na(`flowNearGRanbyNWISOld`) ~ `flowNearGRanbyNWISOld`, 
                                                       !is.na(`CR Below Lake Granby Flow`) ~ round(`CR Below Lake Granby Flow`*(1+(percentageChangeFall2025/100)), 2), 
                                                       TRUE ~ NA), 
         `Assumed/Actual UpperC Flow` = `Actual/Assumed UpperC Above WillowCreek Flow` + `WC Below Res Flow`, 
         
         `Assumed Fraser Flow` = `CR Below WG Flow` + windyGapPumpingFLow - `Assumed/Actual UpperC Flow`, 
         differenceFraserCFS = `Fraser Flow` - `Assumed Fraser Flow`, 
         `Assumed Windy Gap Release` = differenceFraserCFS,
         differenceFraserPercent = round((differenceFraserCFS/`Fraser Flow`)*100, 2)
         ) 

allDataDaily3 <- allDataDaily2 %>%
  mutate(`Assumed/Actual Fraser Flow` = ifelse(!is.na(`Fraser Flow`), `Fraser Flow`, `Assumed Fraser Flow`)) %>%
  select(Date, `CR Below WG Flow`, `Assumed/Actual Fraser Flow`, `Assumed/Actual UpperC Flow`
         
         ) #`Assumed Fraser Flow`, `Actual FR Flow` = `Fraser Flow`,

metadata <- tibble(
  variable = character(),
  description = character()
)

metadata1 <- metadata %>%
  add_row(variable = "CR Below WG Flow", 
          description = "USGS Gage data from Hitching Post (09034250). No gaps in data."
          ) %>%
  add_row(variable = "Assumed/Actual Fraser Flow", 
          description = "Used available data from gage FRAGRACO provided by Northern water to CDSS https://dwr.state.co.us/Tools/Stations/FRAGRACO. For data gaps, 
          assumed Fraser Flow was calculated by subtracting 'Assumed/Actual Upper C Flow' from 'CR Below WG Flow' + 
          'Windy gap pumping flow' (https://dwr.state.co.us/Tools/Stations/WGPPMPCO) "
  ) %>%
  add_row(variable = "Assumed/Actual UpperC Flow", 
          description = paste("USGS Gage below Willow Creek (Gage 09021000) added to 'Actual/Assumed UpperC Above Willow Creek Flow'. 'Assumed/Actual Flow near Granby' 
                              was calculated using USGS gage near Granby (09019500) when available, then using legacy site for that gage (https://waterdata.usgs.gov/nwis/uv?site_no=09019500&legacy=1)
                              to get available instantaneous data and combined to get daily values (only Fall 2025 data available). 
                              If that data wasn't available, a correction factor of ", round(percentageChangeFall2025, 2), "percent was applied to USGS flow data right below Lake Granby (09019000) based on 
                              the mean percent difference in flow between 'USGS gage near Granby' and 'USGS Gage below Lake Granby' for periods when both datasets were available
                              (n = ", nrow(granbygagesOveralppingDates), "days). " 
                              ))

###WHEN RETURNING< THINK ABOUT PUMPING AND WHAT THAT DOES TO FRASER
allFiles <- list("Metadata" = metadata1,
                 "Assumed and Actual Flow Data" = allDataDaily3, 
                 "AllData" = allDataDaily2)
write_xlsx(allFiles, "Outputs/reconstructedWGFPDailyFlow.xlsx") 
#write_csv(allDataDaily2, "Outputs/reconstructedHydrology.csv")
#summary(allDataDaily2$difference)
