############################################
# TAPE PIPELINE WRAPPER
# Purpose: Run test.r with error handling
############################################

library(httr)

send_email_alert <- function(msg) {
  api_key <- Sys.getenv("MAILGUN_API_KEY")
  domain <- Sys.getenv("MAILGUN_DOMAIN")
  from_addr <- Sys.getenv("MAILGUN_FROM")
  to_addr <- Sys.getenv("MAILGUN_TO")

  if (any(nchar(c(api_key, domain, from_addr, to_addr)) == 0)) {
    warning("Mailgun ENV variables not set — skipping email alert")
    return(invisible(NULL))
  }

  url <- paste0("https://api.mailgun.net/v3/", domain, "/messages")

  response <- httr::POST(
    url,
    httr::authenticate("api", api_key),
    encode = "form",
    body = list(
      from = from_addr,
      to = to_addr,
      subject = "TAPE Pipeline Failure",
      text = msg
    )
  )

  if (httr::http_error(response)) {
    warning("Email alert failed: ", httr::http_status(response)$message)
  }
}

handle_error <- function(e) {
  message("❌ Pipeline failed: ", conditionMessage(e))

  tm <- as.POSIXlt(Sys.time(), "UTC")
  strftime(tm , "%Y-%m-%dT%H:%M:%S%z")

  email_message <- paste(
    "Warning the Test TAPE R Pipeline failed with the following notification:",
    conditionMessage(e),
    paste0("Occured at time/date: ", strftime(tm , "%Y-%m-%dT%H:%M:%S%z")),
    sep = "\n\n"
  )

  send_email_alert(email_message)
}

tryCatch(
  expr = {
    source("test.r")
    message("✅ Pipeline completed successfully")
  },
  error = handle_error
  )
