#' locally download, import, prepare publicly-available microdata
#'
#' get_catalog retrieves a listing of all available extracts for a microdata set
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param catalog \code{data.frame} detailing available microdata extracts
#' @param output_dir directory on your local computer to save the microdata
#' @param ... passed to \code{get_catalog} and \code{lodown_}
#'
#' @return a freshly-prepared microdata extract on your local computer
#'
#' @author Anthony Damico
#'
#' @examples
#'
#' \dontrun{
#'
#' # examples to download everything
#' lodown( "ahrf" , output_dir = "C:/My Directory/AHRF" )
#' lodown( "atus" , output_dir = "C:/My Directory/ATUS" )
#' lodown( "ess" , output_dir = "C:/My Directory/ESS" , your_email = "email@address.com" )
#' lodown( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "yrbss" , output_dir = "C:/My Directory/YRBSS" )
#'
#' # examples to download only the first two records in the catalog
#' ahrf_cat <- get_catalog( "ahrf" , output_dir = "C:/My Directory/AHRF" )
#' lodown( "ahrf" , ahrf_cat[ 1:2 , ] )
#' atus_cat <- get_catalog( "atus" , output_dir = "C:/My Directory/ATUS" )
#' lodown( "atus" , atus_cat[ 1:2 , ] )
#' ess_cat <- get_catalog( "ess" , output_dir = "C:/My Directory/ESS" )
#' lodown( "ess" , ess_cat[ 1:2 , ] , your_email = "email@address.com" )
#' nis_cat <- get_catalog( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nis" , nis_cat[ 1:2 , ] )
#' nsch_cat <- get_catalog( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "nsch" , nsch_cat[ 1:2 , ] )
#' yrbss_cat <- get_catalog( "yrbss" , output_dir = "C:/My Directory/YRBSS" )
#' lodown( "yrbss" , yrbss_cat[ 1:2 , ] )
#' #'
#' }
#'
#' @export
#'
lodown <-
  function( data_name , catalog = NULL , ... ){

    if( is.null( catalog ) ){

      catalog <- get_catalog( data_name , ... )

    }

	unique_directories <- unique( dirname( catalog[ , 'output_filename' ] ) )

	for ( this_dir in unique_directories ) if( !file.exists( this_dir ) ) dir.create( this_dir , recursive = TRUE )

    load_fun <- getFromNamespace( paste0( "lodown_" , data_name ) , "lodown" )

    load_fun( catalog , ...)

    cat( paste0( data_name , " local download completed\r\n\n" ) )

	invisible( TRUE )

  }

#' @rdname lodown
#' @export
#'
get_catalog <-
  function( data_name , output_dir = getwd() , ... ){

    cat_fun <- getFromNamespace( paste0( "get_catalog_" , data_name ) , "lodown" )

    cat_fun( output_dir = output_dir , ... )

  }

  
read_SAScii <-
	function( dat_path , sas_path , ... ){

      sasc <- SAScii::parse.SAScii( sas_path )

      sasc$varname[ is.na( sasc$varname ) ] <- paste0( "toss" , seq( sum( is.na( sasc$varname ) ) ) )

      # read in the fixed-width file..
      x <-
        readr::read_fwf(
          # using the ftp filepath
          dat_path ,
          # using the parsed sas widths
          readr::fwf_widths( abs( sasc$width ) , col_names = sasc[ , 'varname' ] ) ,
          # using the parsed sas column types
          col_types = paste0( ifelse( grepl( "^toss" , sasc$varname ) , "_" , ifelse( sasc$char , "c" , "d" ) ) , collapse = "" )
		  
        )

      data.frame( x )
	  
	}