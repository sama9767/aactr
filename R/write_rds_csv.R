#' Write a data frame to RDS or CSV
#'
#' @param x A data frame or tibble to write to disk.
#' @param filepath File to write to, with extension.
#'
#' @return NULL
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Write test.rds within working directory
#' write_rds_csv(mtcars, fs::path("test", ext = "rds"))
#'
#' # Write test.csv within test directory (which must already exist) in working directory
#' write_rds_csv(mtcars, fs::path("test", "test", ext = "csv"))
#' }

write_rds_csv <- function(x,
                          file){

  ext <- fs::path_ext(file)

  # Validate extension
  stopifnot(ext %in% c("rds", "csv"))

  if (ext == "csv") {
    readr::write_csv(x, file)
  } else {readr::write_rds(x, file)}
}


