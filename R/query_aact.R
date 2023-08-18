#' Query AACT table
#'
#' AACT documentation available in the [database schema](https://aact.ctti-clinicaltrials.org/schema) and [data dictionary](https://aact.ctti-clinicaltrials.org/data_dictionary)
#'
#' @param table Character. AACT table.
#' @param ids Character. Vector of NCT ids to query.
#' @param con Connection to AACT database as returned by \code{dbConnect}.
#' @param filepath Character. Filepath including directory portion and ending in ".csv". If no filepath provided, output will be only returned and not written to file.
#' @param ... One or more unquoted expressions separated by commas. Column names from AACT table. `nct_id` is always returned and should not be included in this.
#'
#' @return Dataframe with specified columns from \code{table}
#'

query_aact <- function(table, ids, con, filepath = NULL, ...){

  query <-
    dplyr::tbl(con, table) %>%
    dplyr::filter(.data$nct_id %in% ids) %>%
    dplyr::select(
      "nct_id",
      ...
    )

  # if (!quiet) dplyr::show_query(query)

  out <- dplyr::collect(query)

  if (!rlang::is_null(filepath)){
    readr::write_csv(out, filepath)
  }

  out
}
