#' retrieve a listing of all available extracts for a microdata set
#'
#' subheader
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param output_dir home directory on the local computer to save the microdata
#' @param ... passed to \code{get_catalog}
#'
#' @return \code{data.frame} with files and folders available for download
#'
#' @author Anthony Damico
#'
#' @examples
#'
#' \dontrun{
#'
#' # download the entire american time use survey catalog
#' my_catalog <- get_catalog( "atus" )
#'
#' # review files from data year 2012
#' subset( my_catalog , directory == 2012 )
#'
#' setwd( "C:/My Directory/ATUS" )
#'
#' # download data year 2012
#' lodown( "atus" , subset( my_catalog , directory == 2012 ) )
#'
#' list.files()
#'
#' }
#'
#' @export
#'
get_catalog <-
  function( data_name , output_dir = getwd() , ... ){

    cat_fun <- getFromNamespace( paste0( "get_catalog_" , data_name ) , "lodown" )

    cat_fun( output_dir = output_dir , ... )

  }
