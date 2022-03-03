#' Get trials which have organization in specified columns/table
#'
#' AACT documentation available in the [database schema](https://aact.ctti-clinicaltrials.org/schema) and [data dictionary](https://aact.ctti-clinicaltrials.org/data_dictionary)
#'
#' @param org Character. Regular expression for organization.
#' @param table Character. AACT table.
#' @param columns Character. Vector of column names from AACT table. `nct_id` is always returned and should not be included in this. Organization will be searched in these columns
#' @param con Connection to AACT database as returned by \code{dbConnect}.
#' @param ignore_case Boolean. Whether to ignore case for \code{org}. Defaults to TRUE.
#' @return Dataframe with organization in specified columns from \code{table}

# TODO: organization currently hardcoded and should be passed in as variable but not working. issue open on so: https://stackoverflow.com/questions/71208648/use-variable-with-regex-in-stringstr-detect-in-dbplyr-sql-query

query_org_in_tbl <- function(org, table, columns, con, ignore_case = TRUE){
  query <-
    dplyr::tbl(con, table) %>%
    dplyr::select(nct_id, dplyr::all_of(columns)) %>%

    # Limit "sponsors" to "lead"
    {if (table == "sponsors") dplyr::filter(., lead_or_collaborator == "lead") else .} %>%

    dplyr::filter(dplyr::if_any(dplyr::everything(), ~stringr::str_detect(., "(?i)ucsf|University of California San Francisco|University of California, San Francisco"))) #%>%  #this works, but need to specify miami, i.e., can't use variable
  # dplyr::filter(dplyr::if_any(dplyr::everything(), ~stringr::str_detect(., org))) %>% # TODO

  # dplyr::show_query(query)

  out <- dplyr::collect(query)

  out %>%
    dplyr::mutate(table = table)
}
