#' Get trials "led" by specified organization
#'
#' Trials "led" by organization if organization appears in: overall officials, sponsors (lead only), responsible parties ("principal_investigator", "study_director", "unspecified_official", "sub_investigator", "study_chair")
#' Based on IntoValue (https://github.com/quest-bih/IntoValue2/blob/master/code/1_sample_generation/Create_CTgov_sample.R#L87)
#'
#' @param org Character. Regular expression for organization.
#' @param user Character. AACT username.
#' @param password Character. Default is NULL. If not provided, password will be searched for in \code{keyring} under the \code{aact} service with the provided \code{username}. If no password found, user will be interactively asked and input will be stored in keyring.
  #' @param overwrite Logical. If \code{org} already queried, should it be requeried? Defaults to `FALSE`.
#' @param query Character. Query `INFO` for \code{loggit}. Defaults to "Organization: {org}".
#' @param ignore_case Boolean. Whether to ignore case for \code{org}. Defaults to TRUE.
#'
#' @return Dataframe with trials with organization as lead and "leaderhip" columns
#'
#' @export


# TODO: currently, org doesn't work
# TODO: currently, not writing to disk and not using overwrite, but could add

get_org_trials <- function(org,
                           user, password = NULL,
                           overwrite = FALSE,
                           query = glue::glue("Organization: {org}"),
                           ignore_case = TRUE){
  # Prepare log
  LOGFILE <- here::here("queries.log")
  loggit::set_logfile(LOGFILE)

  # Inform user about tables to be queried
  rlang::inform(glue::glue("Querying AACT for organization: {org}"))

  con <- connect_aact(user, password = password)

  sponsors <-
    query_org_in_tbl(org, "sponsors", columns = c("lead_or_collaborator", "name"),
                     con = con,
                     ignore_case = ignore_case) %>%

    # Trials are limited to lead sponsor
    dplyr::rename(lead_sponsor = "name") %>%
    dplyr::select("nct_id", "lead_sponsor")

  officials <-
    query_org_in_tbl(org, "overall_officials", columns = c("affiliation", "role"),
                     con = con,
                     ignore_case = ignore_case) %>%

    # Some trials have multiple officials of same type and affiliation, so remove duplicates
    dplyr::distinct() %>%

    # Some trials have multiple officials of same type and different affiliation, so collapse and distinct again
    dplyr::group_by(.data$nct_id, .data$role) %>%
    dplyr::mutate(affiliation = stringr::str_c(.data$affiliation, collapse = "; ")) %>%
    dplyr::ungroup() %>%
    dplyr::distinct() %>%

    # Some trials have unspecified officials
    dplyr::mutate(role = tidyr::replace_na(.data$role, "unspecified_official")) %>%

    tidyr::pivot_wider(id_cols = .data$nct_id, names_from = .data$role, values_from = .data$affiliation) %>%

    janitor::clean_names()

  responsible_parties <-
    query_org_in_tbl(org, "responsible_parties", columns = c("affiliation", "organization"),
                     con = con,
                     ignore_case = ignore_case) %>%

    # Check that each study has only affiliation OR organization, and then merge and rename to responsible party
    assertr::assert_rows(assertr::num_row_NAs, assertr::in_set(1), c("affiliation", "organization")) %>%
    dplyr::mutate(affiliation = dplyr::coalesce(.data$affiliation, .data$organization), .keep = "unused") %>%
    dplyr::rename(responsible_party = "affiliation") %>%
    dplyr::select(-"table")
  # tidyr::pivot_wider(id_cols = nct_id, names_from = table, values_from = affiliation)

  # Disconnect aact database
  RPostgreSQL::dbDisconnect(con)

  # Log query date
  loggit::loggit("INFO", query)

  org_trials <-
    dplyr::full_join(sponsors, officials, by = "nct_id") %>%
    dplyr::full_join(responsible_parties, by = "nct_id")

  org_trials
}

# # Additional tables could be checked for organization, e.g., "results_contacts" and "central_contacts"
#
# # Some trials have organization email but not affiliation
# result_contacts_email_only <-
#   query_org_in_tbl("result_contacts", columns = c("organization", "email")) %>%
#   filter(stringr::str_detect(organization, "(?i)stanford", negate = TRUE))
#
# # Limit results contacts based on organization (not email)
# result_contacts <-
#   query_org_in_tbl("result_contacts", columns = "organization", con) %>%
#   rename(results_contact = organization) %>%
#   select(-table)
#
# # There are also central contacts, but individual not organization
# central_contacts <-
#   query_org_in_tbl("central_contacts", columns = c("contact_type", "email", "name"))
