% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/query_helper_funcs.R
\name{get_rx_encounters}
\alias{get_rx_encounters}
\title{Get rx encounters}
\usage{
get_rx_encounters(source, year, ndc_codes, vars = NULL, db_con, collect_n = 10)
}
\arguments{
\item{source}{the source (ccae or mdcr)}

\item{year}{the year to collect from}

\item{ndc_codes}{ndc codes to filter on}

\item{vars}{the variables to collect (if NULL all will be collect)}

\item{db_con}{a database connection}

\item{collect_n}{the number of variables to collect}
}
\description{
Get rx encounters
}
