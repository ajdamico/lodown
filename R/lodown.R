#' @importFrom utils download.file read.csv unzip getFromNamespace
NULL

#' locally download, import, prepare publicly-available microdata
#'
#' get_catalog retrieves a listing of all available extracts for a microdata set
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param catalog \code{data.frame} detailing available microdata extracts
#' @param output_dir directory on your local computer to save the microdata
#' @param ... passed to \code{get_catalog} and \code{lodown_}
#'
#' @return TRUE, and also the microdata in either the folder you specified or your working directory
#'
#' @examples
#'
#' \dontrun{
#'
#' # examples to download everything
#' lodown( "addhealth" , output_dir = "C:/My Directory/AddHealth" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "ahrf" , output_dir = "C:/My Directory/AHRF" )
#' lodown( "atus" , output_dir = "C:/My Directory/ATUS" )
#' lodown( "ces" , output_dir = "C:/My Directory/CES" )
#' lodown( "ess" , output_dir = "C:/My Directory/ESS" , your_email = "email@address.com" )
#' lodown( "nhis" , output_dir = "C:/My Directory/NHIS" )
#' lodown( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "pme" , output_dir = "C:/My Directory/PME" )
#' lodown( "scf" , output_dir = "C:/My Directory/SCF" )
#' lodown( "yrbss" , output_dir = "C:/My Directory/YRBSS" )
#'
#' # examples to download only the first two records in the catalog
#' addhealth_cat <- get_catalog( "addhealth" , output_dir = "C:/My Directory/AddHealth" )
#' lodown( "addhealth" , addhealth_cat , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' ahrf_cat <- get_catalog( "ahrf" , output_dir = "C:/My Directory/AHRF" )
#' lodown( "ahrf" , ahrf_cat[ 1:2 , ] )
#' atus_cat <- get_catalog( "atus" , output_dir = "C:/My Directory/ATUS" )
#' lodown( "atus" , atus_cat[ 1:2 , ] )
#' ces_cat <- get_catalog( "ces" , output_dir = "C:/My Directory/CES" )
#' lodown( "ces" , ces_cat[ 1:2 , ] )
#' ess_cat <- get_catalog( "ess" , output_dir = "C:/My Directory/ESS" )
#' lodown( "ess" , ess_cat[ 1:2 , ] , your_email = "email@address.com" )
#' nhis_cat <- get_catalog( "nhis" , output_dir = "C:/My Directory/NHIS" )
#' lodown( "nhis" , nhis_cat[ 1:2 , ] )
#' nis_cat <- get_catalog( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nis" , nis_cat[ 1:2 , ] )
#' nsch_cat <- get_catalog( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "nsch" , nsch_cat[ 1:2 , ] )
#' pme_cat <- get_catalog( "pme" , output_dir = "C:/My Directory/PME" )
#' lodown( "pme" , pme_cat[ 1:2 , ] )
#' scf_cat <- get_catalog( "scf" , output_dir = "C:/My Directory/SCF" )
#' lodown( "scf" , scf_cat[ 1:2 , ] )
#' yrbss_cat <- get_catalog( "yrbss" , output_dir = "C:/My Directory/YRBSS" )
#' lodown( "yrbss" , yrbss_cat[ 1:2 , ] )
#' 
#' }
#'
#' @export
lodown <-
	function( data_name , catalog = NULL , ... ){

		if( is.null( catalog ) ){

			cat( paste0( "building catalog for " , data_name , "\r\n\n" ) )

			catalog <- get_catalog( data_name , ... )

		}

		unique_directories <- unique( dirname( catalog[ , 'output_filename' ] ) )

		for ( this_dir in unique_directories ) if( !file.exists( this_dir ) ) dir.create( this_dir , recursive = TRUE )

		load_fun <- getFromNamespace( paste0( "lodown_" , data_name ) , "lodown" )

		cat( paste0( "beginning local download of " , data_name , "\r\n\n" ) )

		load_fun( catalog , ...)

		cat( paste0( data_name , " local download completed\r\n\n" ) )

		invisible( TRUE )

	}

#' @rdname lodown
#' @export
get_catalog <-
	function( data_name , output_dir = getwd() , ... ){

		cat_fun <- getFromNamespace( paste0( "get_catalog_" , data_name ) , "lodown" )

		cat_fun( output_dir = output_dir , ... )

	}


read_SAScii <-
	function( dat_path , sas_path , beginline = 1 , lrecl = NULL , ... ){

		sasc <- SAScii::parse.SAScii( sas_path , beginline = beginline , lrecl = lrecl )

		sasc$varname[ is.na( sasc$varname ) ] <- paste0( "toss" , seq( sum( is.na( sasc$varname ) ) ) )

		# read in the fixed-width file..
		x <-
			readr::read_fwf(
				# using the ftp filepath
				dat_path ,
				# using the parsed sas widths
				readr::fwf_widths( abs( sasc$width ) , col_names = sasc[ , 'varname' ] ) ,
				# using the parsed sas column types
				col_types = paste0( ifelse( grepl( "^toss" , sasc$varname ) , "_" , ifelse( sasc$char , "c" , "d" ) ) , collapse = "" ) ,
				# passed in from read_SAScii
				...
			)

		data.frame( x )

	}
	
