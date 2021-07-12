
#' Add DB Directory
#'
#' Adds a directory for a small DB and creates an entry in the db build status
#' log
#'
#'
#' @param project_name a name for the project. This should also correspond to a
#'  disease group in the codeBuildr package
#' @param base_path a path of where to build the db
#' @param sources the sources to pull data from
#' @param years the years to pull data for
#' @param medicaid_years the years of medicaid data to pull from
#' @param param_path the path to the parameter folder
#'
#' @export
add_db_directory <- function(project_name, base_path = "/Shared/AML/small_dbs",
                             sources = c("ccae","mdcr","medicaid"), years = 1:20,
                             medicaid_years = 14:18,
                             param_path = "/Shared/AML/params"){

  if(file.exists(paste0(param_path,"/small_db_paths.RData"))){
    load(paste0(param_path,"/small_db_paths.RData"))
  } else {
    small_db_paths <- list()
  }

  small_db_paths[[project_name]]$sources <- sources
  small_db_paths[[project_name]]$years <- years
  small_db_paths[[project_name]]$medicaid_years <- medicaid_years
  small_db_paths[[project_name]]$base_path <- base_path
  small_db_paths[[project_name]]$last_build <- Sys.Date()
  small_db_paths[[project_name]]$build_status <- "Built directory"


  dir.create(path = paste0(base_path,"/",project_name))
  dir.create(path = paste0(base_path,"/",project_name,"/truven"))

  save(small_db_paths, file = paste0(param_path,"/small_db_paths.RData"))

  }

#' Update Build Status
#'
#' Updates the status of the small_db_paths file
#'
#' @param project_name a name for the project. This should also correspond to a
#'  disease group in the codeBuildr package
#' @param description a description to update the status to
#' @param param_path the path to the parameter folder
#'
#' @export
update_build_status <- function(project_name, description,
                                param_path = "/Shared/AML/params"){

  load(paste0(param_path,"/small_db_paths.RData"))

  small_db_paths[[project_name]]$build_status <- description
  small_db_paths[[project_name]]$last_build <- Sys.Date()

  save(small_db_paths, file = paste0(param_path,"/small_db_paths.RData"))

}



