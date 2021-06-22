

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
#' @param cluster_size size of cluster to create for pulling data
#' @param num_to_collect number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
pull_dx_dates <- function(dx_codes_9, dx_codes_10, db_path, years,
                          cluster_size = 20, num_to_collect = 10){

  # Setup cluster
  cl <- parallel::makeCluster(cluster_size)
  parallel::clusterEvalQ(cl, library(tidyverse))
  parallel::clusterEvalQ(cl, library(bit64))
  parallel::clusterEvalQ(cl, library(dbBuildr))
  parallel::clusterExport(cl, "num_to_collect")
  parallel::clusterExport(cl, "dx_codes_9")
  parallel::clusterExport(cl, "dx_codes_10")
  parallel::clusterExport(cl, "db_path")

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

  res_inpatient_m <-  parLapply(cl,years,
                                function(x) {get_dx_dates(setting = "inpatient",
                                                          source = "mdcr",
                                                          year = x,
                                                          dx9_list = dx_codes_9,
                                                          dx10_list = dx_codes_10,
                                                          con = DBI::dbConnect(RSQLite::SQLite(),
                                                                               paste0(db_path,"truven_",x,".db")),
                                                          collect_n = num_to_collect)})

  res_inpatient_medicaid <-  parLapply(cl,medicaid_years,
                                       function(x) {get_dx_dates(setting = "inpatient",
                                                                 source = "medicaid",
                                                                 year = x,
                                                                 dx9_list = dx_codes_9,
                                                                 dx10_list = dx_codes_10,
                                                                 con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                      paste0(db_path,"truven_medicaid_",x,".db")),
                                                                 collect_n = num_to_collect)})


  #### Combine Inpatient

  all_inpatient_visits <- rbind(do.call("rbind", res_inpatient_m) %>%
                                  mutate(source=0L),
                                do.call("rbind", res_inpatient_c) %>%
                                  mutate(source=1L),
                                do.call("rbind", res_inpatient_medicaid) %>%
                                  mutate(source=2L))

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

  res_outpatient_m <-  parLapply(cl,years,
                                 function(x) {get_dx_dates(setting = "outpatient",
                                                           source = "mdcr",
                                                           year = x,
                                                           dx9_list = dx_codes_9,
                                                           dx10_list = dx_codes_10,
                                                           con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                paste0(db_path,"truven_",x,".db")),
                                                           collect_n = num_to_collect)})

  res_outpatient_medicaid <-  parLapply(cl,medicaid_years,
                                        function(x) {get_dx_dates(setting = "outpatient",
                                                                  source = "medicaid",
                                                                  year = x,
                                                                  dx9_list = dx_codes_9,
                                                                  dx10_list = dx_codes_10,
                                                                  con = DBI::dbConnect(RSQLite::SQLite(),
                                                                                       paste0(db_path,"truven_medicaid_",x,".db")),
                                                                  collect_n = num_to_collect)})

  all_outpatient_visits <- rbind(do.call("rbind", res_outpatient_m) %>%
                                   mutate(source=0L),
                                 do.call("rbind", res_outpatient_c) %>%
                                   mutate(source=1L),
                                 do.call("rbind", res_outpatient_medicaid) %>%
                                   mutate(source=2L))


  rm(res_outpatient_c, res_outpatient_m, res_outpatient_medicaid)

  return(list(all_inpatient_visits = all_inpatient_visits,
              all_outpatient_visits = all_outpatient_visits))

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
