# Get clinicaltrials.gov data via https://aact.ctti-clinicaltrials.org/
# AACT is a relational database of clinicaltrials.gov

# You must first create an account with AACT: https://aact.ctti-clinicaltrials.org/users/sign_up

#' Connect to AACT database
#'
#' Based on https://aact.ctti-clinicaltrials.org/r.
#' Use account credentials created via https://aact.ctti-clinicaltrials.org/users/sign_up
#'
#' @param user Character. AACT username.
#' @param password Character. Default is NULL. If not provided, password will be searched for in \code{keyring} under the \code{aact} service with the provided \code{username}. If no password found, user will be interactively asked and input will be stored in keyring.
#'
#' @return \<PostgreSQLConnection\> object from \code{dbConnect}
#'

connect_aact <- function(user, password = NULL) {
  password <-
    ifelse(
      rlang::is_true(stringr::str_detect(keyring::key_list("aact")$username, user)),
      keyring::key_get("aact", user),
      keyring::key_set("aact", user)
    )

  # drv <- DBI::dbDriver('PostgreSQL')
  drv <- RPostgreSQL::PostgreSQL()
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = "aact",
                                host = "aact-db.ctti-clinicaltrials.org",
                                port = 5432,
                                user = user,
                                password = password
  )

  con
}
