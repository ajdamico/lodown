#' locally download, import, prepare publicly-available microdata for analysis
#'
#' subheader
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param catalog a \code{data.frame} containing folders and files to load
#' @param ... passed to \code{get_catalog} and \code{lodown_}
#'
#' @details stores microdata in the current working directory
#'
#' @return TRUE if it worked
#'
#' @author Anthony Damico
#'
#' @examples
#'
#' \dontrun{
#'
#' setwd( "C:/My Directory/ATUS" )
#'
#' lodown( "atus" )
#'
#' }
#'
#' @export
#'
lodown <-
  function( data_name , catalog = NULL , ... ){

    if( is.null( catalog ) ){

      catalog <- get_catalog( data_name , ... )

    }

    load_fun <- getFromNamespace( paste0( "lodown_" , data_name ) , "lodown" )

    load_fun( catalog , ... )

  }
