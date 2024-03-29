

#' Pull diagnosis dates
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param dx_codes_9 icd-9 diagnosis codes to collect
#' @param dx_codes_10 icd-10 diagnosis codes to collect
#' @param db_path a string with the path to the databases folder
#' @param years years of data to collect for
#' @param medicaid_years years of medicaid data to collect for
#' @param cluster_size size of cluster to create for pulling data
#' @param num_to_collect number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
pull_dx_dates <- function(dx_codes_9, dx_codes_10, db_path, years, medicaid_years=NULL,
                          cluster_size = 21, num_to_collect = 10){

  # Setup cluster
  cl <- parallel::makeCluster(cluster_size)
  parallel::clusterEvalQ(cl, library(tidyverse))
  parallel::clusterEvalQ(cl, library(bit64))
  parallel::clusterEvalQ(cl, library(dbBuildr))
  parallel::clusterExport(cl,list("num_to_collect",
                                  "dx_codes_9",
                                  "dx_codes_10",
                                  "db_path"), envir = environment())

  # define medicaid years to collect

  # ADD THIS.....

  ## Get Inpatient Visits -----------------------------

  res_inpatient_c <-  parLapply(cl,years,
                                function(x) {get_dx_dates(setting = "inpatient",
                                                          source = "ccae",
                                                          year = x,
                                                          dx9_list = dx_codes_9,
                                                          dx10_list = dx_codes_10,
                                                          con = DBI::dbConnect(RSQLite::SQLite(),
                                                                               paste0(db_path,"truven_",x,".db")),
                                                          collect_n = num_to_collect)})
  
  res_inpatient_c <- do.call("rbind", res_inpatient_c) %>%
    mutate(source=1L)
  

  res_inpatient_m <-  parLapply(cl,years,
                                function(x) {get_dx_dates(setting = "inpatient",
                                                          source = "mdcr",
                                                          year = x,
                                                          dx9_list = dx_codes_9,
                                                          dx10_list = dx_codes_10,
                                                          con = DBI::dbConnect(RSQLite::SQLite(),
                                                                               paste0(db_path,"truven_",x,".db")),
                                                          collect_n = num_to_collect)})
  
  res_inpatient_m <- do.call("rbind", res_inpatient_m) %>%
    mutate(source=0L)
  
  
  if (!is.null(medicaid_years)){
    res_inpatient_medicaid <-  parLapply(cl,medicaid_years,
                                         function(x) {get_dx_dates(setting = "inpatient",
                                                                   source = "medicaid",
                                                                   year = x,
                                                                   dx9_list = dx_codes_9,
                                                                   dx10_list = dx_codes_10,
                                                                   con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                        paste0(db_path,"truven_medicaid_",x,".db")),
                                                                   collect_n = num_to_collect)}) 
    
    res_inpatient_medicaid <- do.call("rbind", res_inpatient_medicaid) %>%
      mutate(source=2L)
    
  } else {
    res_inpatient_medicaid <- tibble()
  }


  #### Combine Inpatient

  all_inpatient_visits <- rbind(res_inpatient_m,
                                res_inpatient_c,
                                res_inpatient_medicaid)

  rm(res_inpatient_c, res_inpatient_m, res_inpatient_medicaid)

  ## Get Outpatient ------------------------------------------------------------

  res_outpatient_c <-  parLapply(cl,years,
                                 function(x) {get_dx_dates(setting = "outpatient",
                                                           source = "ccae",
                                                           year = x,
                                                           dx9_list = dx_codes_9,
                                                           dx10_list = dx_codes_10,
                                                           con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                paste0(db_path,"truven_",x,".db")),
                                                           collect_n = num_to_collect)})
  res_outpatient_c <- do.call("rbind", res_outpatient_c) %>%
    mutate(source=1L)

  res_outpatient_m <-  parLapply(cl,years,
                                 function(x) {get_dx_dates(setting = "outpatient",
                                                           source = "mdcr",
                                                           year = x,
                                                           dx9_list = dx_codes_9,
                                                           dx10_list = dx_codes_10,
                                                           con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                paste0(db_path,"truven_",x,".db")),
                                                           collect_n = num_to_collect)})
  
  res_outpatient_m <- do.call("rbind", res_outpatient_m) %>%
    mutate(source=0L)
  
  if (!is.null(medicaid_years)){
    
    res_outpatient_medicaid <-  parLapply(cl,medicaid_years,
                                          function(x) {get_dx_dates(setting = "outpatient",
                                                                    source = "medicaid",
                                                                    year = x,
                                                                    dx9_list = dx_codes_9,
                                                                    dx10_list = dx_codes_10,
                                                                    con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                         paste0(db_path,"truven_medicaid_",x,".db")),
                                                                    collect_n = num_to_collect)})
    
    res_outpatient_medicaid <- do.call("rbind", res_outpatient_medicaid) %>%
      mutate(source=2L)
    
  } else {
    res_outpatient_medicaid <- tibble()
  }
  

  all_outpatient_visits <- rbind(res_outpatient_m,
                                 res_outpatient_c,
                                 res_outpatient_medicaid)


  rm(res_outpatient_c, res_outpatient_m, res_outpatient_medicaid)
  
  
  ## Get facility headers ---------------------------------------
  
  # function to add in dx_poa if missing
  fix_poa <- function(data){
    if (!("dx_poa" %in% names(data))) {
      dplyr::mutate(data,dx_poa=NA)
    } else {
      data
    }
  }
  
  # remove year 01 from years
  fac_years <- years[!(years=="01")]
  
  res_facility_c <-  parallel::parLapply(cl,fac_years,
                                function(x) {get_dx_dates(setting = "facility",
                                                          source = "ccae",
                                                          year = x,
                                                          dx9_list = dx_codes_9,
                                                          dx10_list = dx_codes_10,
                                                          con = DBI::dbConnect(RSQLite::SQLite(),
                                                                               paste0(db_path,"facilities_dbs/facilities_",x,".db")),
                                                          collect_n = num_to_collect)})
  
  res_facility_c <- map(res_facility_c,fix_poa) %>% 
    do.call("rbind",.) %>%
    mutate(source=1L)
  
  
  res_facility_m <-  parallel::parLapply(cl,fac_years,
                                         function(x) {get_dx_dates(setting = "facility",
                                                                   source = "mdcr",
                                                                   year = x,
                                                                   dx9_list = dx_codes_9,
                                                                   dx10_list = dx_codes_10,
                                                                   con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                        paste0(db_path,"facilities_dbs/facilities_",x,".db")),
                                                                   collect_n = num_to_collect)})
  
  res_facility_m <- map(res_facility_m,fix_poa) %>% 
    do.call("rbind",.) %>%
    mutate(source=0L)
  
  
  if (!is.null(medicaid_years)){
    res_facility_medicaid <-  parLapply(cl,medicaid_years,
                                         function(x) {get_dx_dates(setting = "facility",
                                                                   source = "medicaid",
                                                                   year = x,
                                                                   dx9_list = dx_codes_9,
                                                                   dx10_list = dx_codes_10,
                                                                   con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                        paste0(db_path,"facilities_dbs/facilities_medicaid_",x,".db")),
                                                                   collect_n = num_to_collect)}) 
    
    
    res_facility_medicaid <- map(res_facility_medicaid,fix_poa) %>% 
      do.call("rbind",.) %>%
      mutate(source=2L)
    
  } else {
    res_facility_medicaid <- tibble()
  }
  
  
  #### Combine Inpatient
  
  all_facility_visits <- rbind(res_facility_m,
                               res_facility_c,
                               res_facility_medicaid)
  
  rm(res_facility_c, res_facility_m, res_facility_medicaid)
  parallel::stopCluster(cl)

  return(list(all_inpatient_visits = all_inpatient_visits,
              all_outpatient_visits = all_outpatient_visits,
              all_facility_visits = all_facility_visits))

}


#' Pull tables across multiple years
#'
#' This function pulls columns from a specified table
#'
#' @param table_name name of table to pull
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param vars vector of variables to collect
#' @param num_to_collect the number of variables to collect
pull_tables <- function() {

  # Pull Medicare

  parLapply(cl,years,
            function(x) {get_table_vars(table_name = ,
                                        source = "mdcr",
                                        year = x,
                                        vars = vars,
                                        db_con = DBI::dbConnect(RSQLite::SQLite(),
                                                             paste0(db_path,"truven_",x,".db")),
                                        collect_n = num_to_collect)})

  # Pull Commercial Claims

  # Pull Medicaid



}

#' Pull rx encounters across multiple years
#'
#' This function pulls columns from a specified table
#'
#' @param years vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param medicaid_years years of medicaid data to collect for
#' @param ndc_codes ndc codes to pull
#' @param vars vector of variables to collect
#' @param cluster_size size of cluster to create for pulling data
#' @param num_to_collect the number of variables to collect
#' @param db_path path to Truven databases
#' 
#' @export
pull_rx_encounters <- function(years,medicaid_years,ndc_codes,vars = c("enrolid","ndcnum","svcdate"),num_to_collect = 10,
                               cluster_size = 20,db_path = "/Shared/Statepi_Marketscan/databases/Truven/") {
  
  cl <- parallel::makeCluster(cluster_size)
  parallel::clusterEvalQ(cl, library(tidyverse))
  parallel::clusterEvalQ(cl, library(bit64))
  parallel::clusterEvalQ(cl, library(dbBuildr))
  parallel::clusterExport(cl, "get_rx_encounters")
  parallel::clusterExport(cl, "params")
  parallel::clusterExport(cl, "num_to_collect")
  
  
  # Pull Medicare
  
  rx_mdcr <- parLapply(cl,years,
                       function(x) {get_rx_encounters(source = "mdcr",
                                                      year = x,
                                                      ndc_codes = ndc_codes,
                                                      vars = vars,
                                                      db_con = DBI::dbConnect(RSQLite::SQLite(),
                                                                              paste0(db_path,"truven_",x,".db")),
                                                      collect_n = num_to_collect)})
  
  rx_ccae <- parLapply(cl,years,
                       function(x) {get_rx_encounters(source = "ccae",
                                                      year = x,
                                                      ndc_codes = ndc_codes,
                                                      vars = vars,
                                                      db_con = DBI::dbConnect(RSQLite::SQLite(),
                                                                              paste0(db_path,"truven_",x,".db")),
                                                      collect_n = num_to_collect)})
  
  rx_medicaid <- parLapply(cl,medicaid_years,
                           function(x) {get_rx_encounters(source = "medicaid",
                                                          year = x,
                                                          ndc_codes = ndc_codes,
                                                          vars = vars,
                                                          db_con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                  paste0(db_path,"truven_medicaid_",x,".db")),
                                                          collect_n = num_to_collect)})
  
  return(list(rx_mdcr = rx_mdcr,
              rx_ccae = rx_ccae,
              rx_medicaid = rx_medicaid))
  
}


# Pull Procedure Encounters
