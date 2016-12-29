#' @importFrom utils download.file read.csv unzip getFromNamespace write.csv
#' @importFrom stats as.formula
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
#' lodown( "anes" , output_dir = "C:/My Directory/ANES" , your_email = "email@address.com" )
#' lodown( "atus" , output_dir = "C:/My Directory/ATUS" )
#' lodown( "brfss" , output_dir = "C:/My Directory/BRFSS" )
#' lodown( "ces" , output_dir = "C:/My Directory/CES" )
#' lodown( "ess" , output_dir = "C:/My Directory/ESS" , your_email = "email@address.com" )
#' lodown( "ncvs" , output_dir = "C:/My Directory/NCVS" ,
#' 		your_email = "email@address.com" )
#' lodown( "nhis" , output_dir = "C:/My Directory/NHIS" )
#' lodown( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nibrs" , output_dir = "C:/My Directory/NIBRS" ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "nppes" , output_dir = "C:/My Directory/NPPES" )
#' lodown( "nsduh" , output_dir = "C:/My Directory/NSDUH" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "pme" , output_dir = "C:/My Directory/PME" )
#' lodown( "pnadc" , output_dir = "C:/My Directory/PNADC" )
#' lodown( "scf" , output_dir = "C:/My Directory/SCF" )
#' lodown( "yrbss" , output_dir = "C:/My Directory/YRBSS" )
#'
#' # examples to download only the first two records in the catalog
#' addhealth_cat <- get_catalog( "addhealth" , output_dir = "C:/My Directory/AddHealth" )
#' lodown( "addhealth" , addhealth_cat , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' ahrf_cat <- get_catalog( "ahrf" , output_dir = "C:/My Directory/AHRF" )
#' lodown( "ahrf" , ahrf_cat[ 1:2 , ] )
#' anes_cat <- get_catalog( "anes" , output_dir = "C:/My Directory/ANES" , 
#' 		your_email = "email@address.com" )
#' lodown( "anes" , anes_cat , your_email = "email@address.com" )
#' atus_cat <- get_catalog( "atus" , output_dir = "C:/My Directory/ATUS" )
#' lodown( "atus" , atus_cat[ 1:2 , ] )
#' brfss_cat <- get_catalog( "brfss" , output_dir = "C:/My Directory/BRFSS" )
#' lodown( "brfss" , brfss_cat[ 1:2 , ] )
#' ces_cat <- get_catalog( "ces" , output_dir = "C:/My Directory/CES" )
#' lodown( "ces" , ces_cat[ 1:2 , ] )
#' ess_cat <- get_catalog( "ess" , output_dir = "C:/My Directory/ESS" )
#' lodown( "ess" , ess_cat[ 1:2 , ] , your_email = "email@address.com" )
#' ncvs_cat <- get_catalog( "ncvs" , output_dir = "C:/My Directory/NCVS" )
#' lodown( "ncvs" , ncvs_cat[ 1:2 , ] ,
#' 		your_email = "email@address.com" )
#' nhis_cat <- get_catalog( "nhis" , output_dir = "C:/My Directory/NHIS" )
#' lodown( "nhis" , nhis_cat[ 1:2 , ] )
#' nibrs_cat <- get_catalog( "nibrs" , output_dir = "C:/My Directory/NIBRS" )
#' lodown( "nibrs" , nibrs_cat[ 1:2 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' nis_cat <- get_catalog( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nis" , nis_cat[ 1:2 , ] )
#' nppes_cat <- get_catalog( "nppes" , output_dir = "C:/My Directory/NPPES" )
#' lodown( "nppes" , nppes_cat )
#' nsch_cat <- get_catalog( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "nsch" , nsch_cat[ 1:2 , ] )
#' nsduh_cat <- get_catalog( "nsduh" , output_dir = "C:/My Directory/NSDUH" )
#' lodown( "nsduh" , nsduh_cat[ 1:2 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' pme_cat <- get_catalog( "pme" , output_dir = "C:/My Directory/PME" )
#' lodown( "pme" , pme_cat[ 1:2 , ] )
#' pnadc_cat <- get_catalog( "pnadc" , output_dir = "C:/My Directory/PNADC" )
#' lodown( "pnadc" , pnadc_cat[ 1:2 , ] )
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

		unique_directories <- unique( c( catalog$unzip_folder , if( 'output_filename' %in% names( catalog ) ) dirname( catalog$output_filename ) , catalog$dbfolder ) )

		for ( this_dir in unique_directories ) if( !file.exists( this_dir ) ) dir.create( this_dir , recursive = TRUE )

		load_fun <- getFromNamespace( paste0( "lodown_" , data_name ) , "lodown" )

		cat( paste0( "locally downloading " , data_name , "\r\n\n" ) )

		load_fun( data_name = data_name , catalog , ...)

		cat( paste0( data_name , " local download completed\r\n\n" ) )

		invisible( TRUE )

	}

#' @rdname lodown
#' @export
get_catalog <-
	function( data_name , output_dir = getwd() , ... ){

		cat_fun <- getFromNamespace( paste0( "get_catalog_" , data_name ) , "lodown" )

		cat_fun( data_name = data_name , output_dir = output_dir , ... )

	}
	
no.na <- function( x , value = FALSE ){ x[ is.na( x ) ] <- value ; x }
