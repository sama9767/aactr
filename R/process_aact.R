#' Process AACT tables in given directory, process into rds
#'
#' Processes and cleans AACT tables from \code{dir_raw}. Dataframes saved to specified \code{dir_out} as .rds. All tables (as prespecified in function and matching \code{download_aact}) will be processed unless *all* already processed and \code{overwrite} is `FALSE`.
#'
#' AACT documentation available in the [database schema](https://aact.ctti-clinicaltrials.org/schema) and [data dictionary](https://aact.ctti-clinicaltrials.org/data_dictionary).

#' @param dir_in Character. Directory from which to get raw .csv files. Defaults to working directory with `here::here("raw")`.
#' @param dir_out Character. Directory in which to save processed files. Defaults to working directory with `here::here("processed")`.
#' @param ext_out Character. Extension for processed files. "rds" (default) or "csv".
#' @param overwrite Logical. If all valid output tables already processed, should they be overwritten? Defaults to `FALSE`. Note that if *any* table is not processed, *all* tables will be processed. Since this checks that *all* valid output tables are processed, if *all* valid input tables aren't included in \code{dir_in}, *all* available raw tables will be re-processed each time.
#'
#' @return NULL
#'
#' @export
#'
#' @examples
#' \dontrun{
#' process_aact(dir_in = here::here("data", "raw"),
#'              dir_out = here::here("data", "processed"))
#'}



process_aact <- function(dir_in = here::here("raw"),
                         dir_out = here::here("processed"),
                         ext_out = "rds",
                         # tables = NULL, # could add param to chose tables
                         overwrite = FALSE){

  # Create output directory if doesn't exist
  fs::dir_create(dir_out)

  # Output extension must be "rds" or "csv
  stopifnot(ext_out %in% c("rds", "csv"))

  # Processing prepared for certain tables
  valid_in_tables <- c("studies", "designs", "interventions", "references", "ids", "centers", "officials", "responsible-parties", "sponsors", "facilities", "central-contacts", "facility-contacts", "result-contacts")
  valid_out_tables <- c("ctgov-lead-affiliations", "ctgov-facility-affiliations", "ctgov-studies", "ctgov-ids", "ctgov-crossreg",  "ctgov-references", "ctgov-contacts")

  # If all tables already processed and not overwriting, then inform user and return
  raw_tables <-
    fs::dir_ls(dir_in) %>%
    fs::path_file() %>%
    fs::path_ext_remove() %>%

    # Limit to valid tables in case of additional files
    intersect(valid_in_tables)

  processed_tables <-
    fs::dir_ls(dir_out) %>%
    fs::path_file() %>%
    fs::path_ext_remove()

  if (all(valid_out_tables %in% processed_tables) & !overwrite){

    rlang::inform(glue::glue("Already processed all valid output tables: ", glue::glue_collapse(valid_out_tables, sep = ", ", last = ", and ")))
    return(NULL)

  }

  # If not all tables processed, reprocess *all* tables (alternatively could process unprocessed only)

  # Process lead and facility affiliations ----------------------------------

  # Since we don't distinguish between sponsors/official/responsible parties, combine affiliations
  # Note: For now, using intovalue umc affiliations, so simply save output

  # Check that all three lead affiliation tables available
  if (all(c("responsible-parties", "officials", "sponsors") %in% raw_tables)){
    resp_parties <-
      readr::read_csv(fs::path(dir_in, "responsible-parties", ext = "csv")) %>%
      dplyr::filter(!is.na(affiliation) | !is.na(organization)) %>%

      # Check that each study has only affiliation OR organization, and then merge
      assertr::assert_rows(assertr::num_row_NAs, assertr::in_set(1), c(affiliation, organization)) %>%

      dplyr::mutate(affiliation = dplyr::coalesce(affiliation, organization), .keep = "unused") %>%
      dplyr::distinct() %>%
      dplyr::mutate(affiliation_type = "Responsible Party")

    officials <-
      readr::read_csv(fs::path(dir_in, "officials", ext = "csv")) %>%
      dplyr::distinct() %>%
      dplyr::filter(!is.na(affiliation)) %>%
      dplyr::mutate(affiliation_type = "Study Official")

    sponsors <-
      readr::read_csv(fs::path(dir_in, "sponsors", ext = "csv")) %>%
      dplyr::filter(lead_or_collaborator == "lead") %>%
      dplyr::select(-lead_or_collaborator) %>%

      dplyr::rename(
        main_sponsor = agency_class,
        affiliation = name
      ) %>%
      assertr::assert(assertr::is_uniq, nct_id) %>%
      dplyr::mutate(affiliation_type = "Sponsor")

    affiliations <-
      dplyr::bind_rows(sponsors, resp_parties, officials) %>%
      dplyr::distinct() %>%
      dplyr::select(nct_id, affiliation_type, lead_affiliation = affiliation) %>%
      dplyr::arrange(nct_id)

    if (ext_out == "csv"){

    } else {}
    write_rds_csv(affiliations, fs::path(dir_out, "ctgov-lead-affiliations", ext = ext_out))

  }

  if ("facilities" %in% raw_tables){
    facilities <-
      readr::read_csv(fs::path(dir_in, "facilities", ext = "csv")) %>%
      dplyr::rename(facility_affiliation = name)

    write_rds_csv(facilities, fs::path(dir_out, "ctgov-facility-affiliations", ext = ext_out))

  }

  # Process studies ---------------------------------------------------------

  if ("studies" %in% raw_tables){
    studies <-
      readr::read_csv(fs::path(dir_in, "studies", ext = "csv")) %>%

      dplyr::rename(
        registration_date = study_first_submitted_date,
        summary_results_date = results_first_submitted_date,
        recruitment_status = overall_status
      ) %>%

      # Parse dates, which are either Month Year, or Month Day Year
      # If no day, default to 1st, like intovalue
      dplyr::mutate(
        start_date = lubridate::parse_date_time(start_month_year, c("my", "mdY")),
        completion_date = lubridate::parse_date_time(completion_month_year, c("my", "mdY")),
        primary_completion_date = lubridate::parse_date_time(primary_completion_month_year, c("my", "mdY")),
        .keep = "unused"
      ) %>%

      dplyr::mutate(
        has_summary_results = dplyr::if_else(!is.na(summary_results_date), TRUE, FALSE)
      ) %>%
      #
      # mutate(
      #   days_reg_to_start = duration_days(registration_date, start_date),
      #   days_reg_to_comp = duration_days(registration_date, completion_date),
      #   days_comp_to_summary = duration_days(completion_date, summary_results_date)
      # ) %>%

      dplyr::mutate(
        phase = dplyr::na_if(phase, "N/A"),
        study_type =
          dplyr::if_else(
            stringr::str_detect(study_type, "Observational"),
            "Observational", study_type
          )
      ) %>%

      dplyr::rename(title = brief_title) %>%

      dplyr::select(-official_title)
  }

  if (all(c("designs", "centers", "sponsors") %in% raw_tables)){

    designs <- readr::read_csv(fs::path(dir_in, "designs", ext = "csv"))
    centers <- readr::read_csv(fs::path(dir_in, "centers", ext = "csv"))

    studies <-

      studies %>%

      # Check that designs & centers have no duplicates per study, and add to studies
      dplyr::left_join(assertr::assert(designs, assertr::is_uniq, nct_id), by = "nct_id") %>%
      dplyr::left_join(assertr::assert(centers, assertr::is_uniq, nct_id), by = "nct_id") %>%
      dplyr::left_join(dplyr::select(sponsors, nct_id, main_sponsor), by = "nct_id") %>%

      dplyr::mutate(
        allocation =  dplyr::na_if(allocation, "N/A"),
        is_multicentric = !has_single_facility
      ) %>%

      dplyr::select(-has_single_facility, -number_of_facilities)

  }

  write_rds_csv(studies, fs::path(dir_out, "ctgov-studies", ext = ext_out))

  # Process interventions ---------------------------------------------------

  # Note: Unclear how IntoValue selects single intervention_type, so for now disregard

  # if ("interventions" %in% raw_tables){
  #
  #   interventions <- readr::read_csv(fs::path(dir_in, "interventions", ext = "csv"))
  #
  #   interventions %>%
  #     dplyr::distinct() %>%
  #     janitor::get_dupes(nct_id) %>%
  #     dplyr::count(dupe_count)
  #
  #   interventions %>%
  #     dplyr::distinct() %>%
  #     dplyr::group_by(nct_id) %>%
  #     dplyr::mutate(intervention_types = paste(intervention_type, collapse="; ")) %>%
  #     dplyr::distinct(nct_id, intervention_types)
  # }

  # Process ids -------------------------------------------------------------
  if ("ids" %in% raw_tables){
    ids <-
      readr::read_csv(fs::path(dir_in, "ids", ext = "csv")) %>%

      ctregistries::mutate_trn_registry(id_value) %>%

      # Clean trns and collapse EudraCT entries
      dplyr::mutate(
        raw_trn = trn,
        trn = purrr::map_chr(raw_trn, ctregistries::clean_trn)
      )

    write_rds_csv(ids, fs::path(dir_out, "ctgov-ids", ext = ext_out))

    crossreg <-
      ids %>%
      dplyr::filter(!is.na(trn)) %>%
      dplyr::select(nct_id, crossreg_registry = registry, crossreg_trn = trn)

    write_rds_csv(crossreg, fs::path(dir_out, "ctgov-crossreg", ext = ext_out))
  }

  # Process references ------------------------------------------------------

  if ("references" %in% raw_tables){
    references <-
      readr::read_csv(fs::path(dir_in, "references", ext = "csv")) %>%

      # Extract identifiers
      dplyr::mutate(
        pmid = as.numeric(pmid),
        doi = stringr::str_extract(citation, "10\\.\\d{4,9}/[-.;()/:\\w\\d]+"),
        doi = stringr::str_remove(doi, "\\.$"), # remove trailing period
        pmcid = stringr::str_extract(citation, "PMC[0-9]{7}")
      ) %>%

      # Some references are automatically derived in ct.gov
      dplyr::mutate(reference_derived = dplyr::if_else(reference_type == "derived", TRUE, FALSE))

    write_rds_csv(references, fs::path(dir_out, "ctgov-references", ext = ext_out))
  }

  # Process contacts --------------------------------------------------------

  if (all(c("central-contacts", "facility-contacts", "result-contacts") %in% raw_tables)){
    central_contacts <-
      readr::read_csv(fs::path(dir_in, "central-contacts", ext = "csv")) %>%
      dplyr::distinct() %>%
      dplyr::mutate(contact_type = stringr::str_c("central_", contact_type))

    facility_contacts <-
      readr::read_csv(fs::path(dir_in, "facility-contacts", ext = "csv")) %>%
      dplyr::distinct() %>%
      dplyr::mutate(contact_type = stringr::str_c("facility_", contact_type))

    result_contacts <-
      readr::read_csv(fs::path(dir_in, "result-contacts", ext = "csv")) %>%
      dplyr::distinct() %>%
      dplyr::mutate(contact_type = "result")

    contacts <-
      dplyr::bind_rows(central_contacts, facility_contacts, result_contacts) %>%
      dplyr::distinct() %>%
      dplyr::arrange(nct_id)

    write_rds_csv(contacts, fs::path(dir_out, "ctgov-contacts", ext = ext_out))
  }
}
