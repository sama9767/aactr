#' Get AACT tables for given NCT ids
#'
#' Calls \code{connect_aact} to connect to AACT database and \code{query_aact} to query and download individual AACT tables. Dataframes saved to specified \code{dir} as .csv. If \code{tables} not specified, all tables (as prespecified in function and messaged to user) will be queried. Logs query date with \code{loggit}.
#'
#' AACT documentation available in the [database schema](https://aact.ctti-clinicaltrials.org/schema) and [data dictionary](https://aact.ctti-clinicaltrials.org/data_dictionary).

#' @param ids Character. Vector of NCT ids to query.
#' @param dir Character. Directory in which to save raw CSVs. Defaults to working directory with `here::here("raw")`.
#' @param user Character. AACT username.
#' @param password Character. Default is NULL. If not provided, password will be searched for in \code{keyring} under the \code{aact} service with the provided \code{username}. If no password found, user will be interactively asked and input will be stored in keyring.
#' @param tables Character. Vector of AACT tables to query. Default of `NULL` results in querying all tables ("studies", "designs", "interventions", "references", "ids", "centers", "officials", "responsible-parties", "sponsors", "facilities", "central-contacts", "facility-contacts", "result-contacts").
#' @param overwrite Logical. If all tables already downloaded, should they be overwritten? Defaults to `FALSE`. Note that if *any* table is missing, *all* tables will be queried.
#' @param query Character. Query `INFO` for \code{loggit}. Defaults to "AACT".
#'
#' @return NULL
#'
#' @export
#'
#' @examples
#' \dontrun{
#' download_aact(ids = c("NCT00868166", "NCT00869726"),
#'               dir = here::here("data", "raw"),
#'               user = "respmetrics")
#'}

download_aact <- function(ids,
                          dir = here::here("raw"),
                          user, password = NULL,
                          tables = NULL,
                          overwrite = FALSE,
                          query = "AACT"){

  # Queries prepared for certain tables
  valid_tables <- c("studies", "designs", "interventions", "references", "ids", "centers", "officials", "responsible-parties", "sponsors", "facilities", "central-contacts", "facility-contacts", "result-contacts")

  # If no user-specified tables, query all tables
  if (rlang::is_null(tables)) tables <- valid_tables

  tables_txt <-
    glue::glue_collapse(glue::glue("'{tables}'"), sep = ", ", last = ", and ")

  valid_tables_txt <-
    glue::glue_collapse(glue::glue("'{valid_tables}'"), sep = ", ", last = ", and ")

  # Check for valid tables
  if (any(!tables %in% valid_tables)){
    rlang::abort(glue::glue("All `tables` must be in {valid_tables_txt}"))
  }

  # Prepare log
  LOGFILE <- here::here("queries.log")
  loggit::set_logfile(LOGFILE)


  # Create output directory if doesn't exist
  fs::dir_create(dir)

  # If all tables already downloaded and not overwriting, then inform user and return
  downloaded_tables <-
    fs::dir_ls(dir) %>%
    fs::path_file() %>%
    fs::path_ext_remove()

  if (all(tables %in% downloaded_tables) & !overwrite){

    # Function to get latest query
    get_latest_query <- function(query, logfile) {

      # Read logs and catch error if no existing logs
      logs <- tryCatch(error = function(cnd) NULL, loggit::read_logs(logfile))

      if (!rlang::is_null(logs)){
        logs %>%
          dplyr::filter(.data$log_msg == query) %>%
          dplyr::arrange(dplyr::desc(.data$timestamp)) %>%
          dplyr::slice_head(n = 1) %>%
          dplyr::pull(.data$timestamp) %>%
          as.Date.character()
      } else {"No previous query"}
    }

    rlang::inform(glue::glue("Already downloaded on {get_latest_query(query)}: {tables_txt}"))
    return(NULL)
  }

  # Inform user about tables to be queried
  rlang::inform(glue::glue("Querying AACT for tables: {tables_txt}"))

  con <- connect_aact(user, password = password)

  # Query aact database and write output to files
  if ("studies" %in% tables){
    query_aact(
      "studies", ids,
      con, filepath = fs::path(dir, "studies", ext = "csv"),
      "last_update_submitted_date",
      "start_month_year", "completion_month_year", "primary_completion_month_year",
      "study_first_submitted_date", "results_first_submitted_date",
      "study_type", "phase", "enrollment", "overall_status",
      "official_title", "brief_title"
    )
  }

  if ("designs" %in% tables){
    query_aact(
      "designs", ids,
      con, filepath = fs::path(dir, "designs", ext = "csv"),
      "allocation", "masking"
    )
  }

  if ("interventions" %in% tables){
    query_aact(
      "interventions", ids,
      con, filepath = fs::path(dir, "interventions", ext = "csv"),
      "intervention_type"
    )
  }

  if ("references" %in% tables){
    query_aact(
      "study_references", ids,
      con, filepath = fs::path(dir, "references", ext = "csv"),
      "pmid", "reference_type", "citation"
    )
  }

  if ("ids" %in% tables){
    query_aact(
      "id_information", ids,
      con, filepath = fs::path(dir, "ids", ext = "csv"),
      # id_source, id_type_description, id_link missing from aact data schema
      "id_source", "id_value", "id_type", "id_type_description", "id_link"
    )
  }

  if ("centers" %in% tables){
    query_aact(
      "calculated_values", ids,
      con, filepath = fs::path(dir, "centers", ext = "csv"),
      "has_single_facility", "number_of_facilities"
    )
  }

  if ("officials" %in% tables){
    query_aact(
      "overall_officials", ids,
      con, filepath = fs::path(dir, "officials", ext = "csv"),
      "affiliation"
    )
  }

  if ("responsible-parties" %in% tables){
    query_aact(
      "responsible_parties", ids,
      con, filepath = fs::path(dir, "responsible-parties", ext = "csv"),
      "affiliation", "organization"
    )
  }

  if ("sponsors" %in% tables){
    query_aact(
      "sponsors", ids,
      con, filepath = fs::path(dir, "sponsors", ext = "csv"),
      "agency_class", # main_sponsor
      "lead_or_collaborator", "name"
    )
  }

  if ("facilities" %in% tables){
    query_aact(
      "facilities", ids,
      con, filepath = fs::path(dir, "facilities", ext = "csv"),
      "name", "city", "country"
    )
  }

  if ("central-contacts" %in% tables){
    query_aact(
      "central_contacts", ids,
      con, filepath = fs::path(dir, "central-contacts", ext = "csv"),
      "contact_type", "name", "email", "phone"
    )
  }

  if ("facility-contacts" %in% tables){
    query_aact(
      "facility_contacts", ids,
      con, filepath = fs::path(dir, "facility-contacts", ext = "csv"),
      "facility_id", "contact_type", "name", "email", "phone"
    )
  }

  if ("result-contacts" %in% tables){
    query_aact(
      "result_contacts", ids,
      con, filepath = fs::path(dir, "result-contacts", ext = "csv"),
      "organization", "name", "email", "phone"
    )
  }


  # Disconnect aact database
  RPostgreSQL::dbDisconnect(con)

  # Log query date
  loggit::loggit("INFO", query)
}
