#reconstructing WGFP Hydrology
library(tidyverse) 
library(lubridate)
library(sf)
library(readxl)
library(dataRetrieval) #this is the usgs package for getting up to date data
library(writexl)
library(purrr)


###NEAR GRANBY USGS GAGE INSTANT/CONTINOUS FALL 2025
#can only get these fall dates, otherwise if you go back more than 120 days it will show up as "Ssn"
#only works for instant as well
#https://waterdata.usgs.gov/nwis/uv?site_no=09019500&legacy=1
fall2025nearGranbyGageNWISInstant <- read_csv("Inputs/fall2025nearGranbyGageNWISInstant.csv")

fall2025nearGranbyGageNWISInstant1 <- fall2025nearGranbyGageNWISInstant %>%
  mutate(datetime = mdy_hm(datetime), 
         Date = as.Date(datetime)) %>%
  group_by(Date) %>%
  summarise(flowNearGRanbyNWISOld = mean(as.numeric(`60`), na.rm = TRUE))

##WINDY GAP PUMPING RECORD
#assuming NA pumping values are not pumping
#only goers back to June 2021
#this came from https://dwr.state.co.us/Tools/Stations/WGPPMPCO
WindyGapPumpingRecord <- read_csv("Inputs/WindyGapPumpingRecord.csv")

WindyGapPumpingRecord1 <- WindyGapPumpingRecord %>%
  mutate(Date = mdy(meas_date), 
         windyGapPumpingFLow = replace_na(as.numeric(`Streamflow Value`), 0)) %>%
  select(Date, windyGapPumpingFLow)

#DEFINE FUNCTION TO RETRIEVE AND FORMAT UP-TO-DATE USGS DATA FROM WEB
#not all gages have temperature available so we have that as an optional argument
#"USGS-09034250", #code for windy gap
codeID = "09034250"
getDailyand15MinUSGSData <- function(codeID, startDate = "2020-08-06", endDate = Sys.Date(), waterTemp = TRUE) {
  ##windy gap/hitching post 
  #reading in USGS data with upt to date data
  USGSDataDaily <- read_waterdata_daily(monitoring_location_id = paste0("USGS-", codeID),
                                        parameter_code = c("00060", "00010"), #this is parameter codes for discharge and celsius water temp; more can be added if needed. https://help.waterdata.usgs.gov/codes-and-parameters/parameters
                                        time = c(startDate, endDate)
  )
  #removes geometry so no longer sf object
  #gets to desired wide format while applying "mean" function to the values
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
  #sometimes this can fail if USGS is having issues on their end
  ###since the new water data continupous only retunrs 3 year datasets, use this from purrr to combine it together
  #Set up total date range, make sure it's in denver timezone or else we'll lose data when getting start/end dates for date chunks
  ##same thing in windy gap app runscript
  start_date <- as.POSIXct("2020-08-06 00:00:00", tz = "America/Denver")
  end_date <- Sys.time()
  
  # Create the sequence of start dates (every 2 years)
  starts <- seq(from = start_date, to = end_date, by = "2 years")
  
  # Create the sequence of end dates
  # take all the start dates (skipping the first one), subtract 1 day, 
  # and then tack final end_date onto the very end.
  ends <- c(starts[-1] - 1, end_date)
  
  #  Loop through the intervals, pull the data, and bind the rows together
  #map2 = map over 2 vectors or lists in parrallel: starts and ends
  #_dfr = return resultting in df by rows, not lists
  continuousSiteData <- map2_dfr(
    starts, # Start dates for each chunk
    ends, # End dates for each chunk
    #~ means make a anonmymous function
    ~ read_waterdata_continuous(
      monitoring_location_id = paste0("USGS-", codeID),
      parameter_code = c("00060", "00010", "00065"),
      time = c(as.character(.x), as.character(.y))
    )
  )
  
  continuousSiteData_wide <- continuousSiteData %>%
    
    # Translate the numeric codes into human-readable names
    mutate(
      parameter_code = replace_values(
        parameter_code,
        "00060" ~ "USGSDischarge",
        "00010" ~ "USGSWatertemp",
        "00065" ~ "USGSGageHeightFt"
      )
    ) %>%
    # Pivot to wide format
    pivot_wider(
      id_cols = c(monitoring_location_id, time),
      names_from = parameter_code,
      values_from = value
    )
  USGSData <- continuousSiteData_wide %>%
    #mutate(USGSWatertemp = (`USGSWatertemp` * 9/5) + 32) %>%
    rename(dateTime = time)
  # USGSData <- readNWISuv(siteNumbers = codeID, #code for windy gap
  #                        parameterCd = c("00060", "00010"), #this is parameter code for discharge; more can be added if needed
  #                        startDate = startDate, #if you want to do times it is this format: "2014-10-10T00:00Z",
  #                        endDate = endDate,
  #                        tz = "America/Denver")
  
  #USGSData <- renameNWISColumns(USGSData) 
  
  if(waterTemp){
    USGSData <- USGSData %>%
      mutate(USGSWatertemp = (USGSWatertemp * 9/5) + 32) 
  }
  # USGSData <- USGSData %>%
  #   rename(USGSDischarge = Flow_Inst)
  #this is to make attaching these readings to detections later
  USGSData$dateTime <- lubridate::force_tz(USGSData$dateTime, tzone = "UTC") 
  
  return(list("USGSData" = USGSData, 
              "USGSDataDaily" = USGSDataDaily))
}

##windy gap/hitching post 

windayGpData <- getDailyand15MinUSGSData("09034250")
windyGap <- windayGpData$USGSData %>%
  rename(`CR Below WG Flow` = USGSDischarge)
windyGapDaily <- windayGpData$USGSDataDaily %>%
  select(Date, Flow) %>%
  rename(`CR Below WG Flow` = Flow)

belowGranbyData <- getDailyand15MinUSGSData("09019000", waterTemp = FALSE)
belowGranby <- belowGranbyData$USGSData %>%
  rename(`CR Below Lake Granby Flow` = USGSDischarge)
belowGranbyDaily <- belowGranbyData$USGSDataDaily %>%
  select(Date, Flow) %>%
  rename(`CR Below Lake Granby Flow` = Flow)

nearGranbyData <- getDailyand15MinUSGSData("09019500") 
nearGranby <- nearGranbyData$USGSData %>%
  rename(`CR Near Granby Flow` = USGSDischarge)
nearGranbyDaily <- nearGranbyData$USGSDataDaily %>%
  select(Date, Flow) %>%
  rename(`CR Near Granby Flow` = Flow)

#willow creek only has discharge, no temperature
willowCreekData <- getDailyand15MinUSGSData("09021000", waterTemp = FALSE)
willowCreek <- willowCreekData$USGSData %>%
  rename(`WC Below Res Flow` = USGSDischarge)
willowCreekDaily <- willowCreekData$USGSDataDaily %>%
  select(Date, Flow) %>%
  rename(`WC Below Res Flow` = Flow)

#bring in fraser data to join
#this sheet came from U drive, used to get fraser river only calcs
WGFP_Hydrology_Qdaily_2020_to_2022 <- read_excel("Inputs/WGFP_Hydrology_Qdaily_2020_to_2022.xlsx", 
                                                 sheet = "data_all")
#this sheet caem from https://dwr.state.co.us/Tools/Stations/FRAGRACO
#the U drive sheet was probably gleaned from this but the u drive sheety has more data for the beginning of the study and this has mroe towards the end of the study
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
  mutate(#granby difference in gages for all data
         differenceCFSGranbyGages = `CR Near Granby Flow` - `CR Below Lake Granby Flow`, 
         differencePercentGranbyGages = round((differenceCFSGranbyGages/`CR Below Lake Granby Flow`)*100, 2),
         
         #granby difference in gagews for fall 2025 instanteous data
         differenceCFSFall2025Granby = flowNearGRanbyNWISOld - `CR Below Lake Granby Flow`, 
         differenceCFSFall2025Granbypercent = round((differenceCFSFall2025Granby/`CR Below Lake Granby Flow`)*100, 2)
         
         # comments = ifelse(is.na(windyGapPumpingFLow), "No Windy Gap Pumping Data Avaialble for this time period", "")
         )

###all difference between granby gages
mean(allDataDaily1$differenceCFSGranbyGages, na.rm = T)
percentageChangeAllGranby <- mean(allDataDaily1$differencePercentGranbyGages, na.rm = T)

#for fall 2025, data is typically off only by .5 cfs (on average; big std though), 
# but that equates to an average of 8.8%
###So if we are going to extrapolate fall values, maybe it would make sense to add 8.8% more flow to the "below WG gage"
#IMPORTANT: Even when new data is ran, the old data can change because of revisions from USGS since each time the script is run it gets data from the whole study period. 
#so this means we can get a different corrective factor that is used for the assumed flow above willow creek when actual data from the gages aren't available
#change in the corrective factor comes from fall 2025 data changes to 'CR Below Lake Granby' gage 
mean(allDataDaily1$differenceCFSFall2025Granby, na.rm = T)
percentageChangeFall2025 <- mean(allDataDaily1$differenceCFSFall2025Granbypercent, na.rm = T)

granbygagesOveralppingDates <- allDataDaily1 %>%
  filter(!is.na(differencePercentGranbyGages))
allDataDaily2 <- allDataDaily1 %>%
  #if data is available for the near granby gagae, use that bc it's most treliable before adding into willow creek
  #if that's not available, use the isntanteous flow from the legacy site (only available fall 2025)
  #if that's not avaialble, use the correction based off below lake granby gage; see above for chagnign correction caveat
  ###NOT SURE IF WE WANT TO USE CORRECTION FOR ALL DATA (percentageChangeAllGranby, -1% ish) OR FOR JUST FALL 2025 (percentageChangeFall2025, 8.8%)
  #for now using percentageChangeFall2025
  mutate(`Actual/Assumed UpperC Above WillowCreek Flow` = case_when(!is.na(`CR Near Granby Flow`) ~ `CR Near Granby Flow`, 
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

##differences between assumed fraser and actual for the data we have
# mean(allDataDaily1$differenceFraserCFS, na.rm = T)
# percentageDifFraser <- mean(allDataDaily1$differenceFraserPercent, na.rm = T)
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
          assumed Fraser Flow was calculated by subtracting 'Assumed/Actual Upper C Flow' from 'CR Below WG Flow' + 'Windy gap pumping flow' (https://dwr.state.co.us/Tools/Stations/WGPPMPCO). 
          No correction factor was used due to inconsistent pumping flows out of Windy Gap. "
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
write_xlsx(allFiles, paste0("Outputs/reconstructedWGFPDailyFlow_", Sys.Date(), ".xlsx")) 
#write_csv(allDataDaily2, "Outputs/reconstructedHydrology.csv")
#summary(allDataDaily2$difference)
