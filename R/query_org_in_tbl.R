#' Get trials which have organization in specified columns/table
#'
#' AACT documentation available in the [database schema](https://aact.ctti-clinicaltrials.org/schema) and [data dictionary](https://aact.ctti-clinicaltrials.org/data_dictionary)
#'
#' @param org Character. Regular expression for organization. By default, case agnostic, controlled by \code{ignore_case}.
#' @param table Character. AACT table.
#' @param columns Character. Vector of column names from AACT table. `nct_id` is always returned and should not be included in this. Organization will be searched in these columns
#' @param con Connection to AACT database as returned by \code{dbConnect}.
#' @param ignore_case Boolean. Whether to ignore case for \code{org}. Defaults to TRUE.
#' @return Dataframe with organization in specified columns from \code{table}

query_org_in_tbl <- function(org, table, columns, con, ignore_case = TRUE){

  # Ignore case, if specified and not already ignored
  if (ignore_case == TRUE & stringr::str_detect(org, stringr::coll("^(?i)"), negate = TRUE)){
    org <- paste0("(?i)", org)
  }

  # Placeholder value to avoid CMD check errors
  . <- NULL

  query <-
    dplyr::tbl(con, table) %>%
    dplyr::select("nct_id", dplyr::all_of(columns)) %>%

    # Limit "sponsors" to "lead"
    {if (table == "sponsors") dplyr::filter(., .data$lead_or_collaborator == "lead") else .} %>%

    # Thank you @bgcarlisle for help with curly-curly!
    dplyr::filter(dplyr::if_any(dplyr::everything(), ~stringr::str_detect(., {{org}})))

  # dplyr::show_query(query)

  out <- dplyr::collect(query)

  out %>%
    dplyr::mutate(table = table)
}
