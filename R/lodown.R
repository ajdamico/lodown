#' @importFrom utils download.file read.csv unzip getFromNamespace write.csv read.table
#' @importFrom stats as.formula vcov coef pf update
#' @importFrom graphics plot rasterImage
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
#' lodown( "acs" , output_dir = "C:/My Directory/ACS" )
#' lodown( "addhealth" , output_dir = "C:/My Directory/AddHealth" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "ahrf" , output_dir = "C:/My Directory/AHRF" )
#' lodown( "anes" , output_dir = "C:/My Directory/ANES" , your_email = "email@address.com" )
#' lodown( "atus" , output_dir = "C:/My Directory/ATUS" )
#' lodown( "brfss" , output_dir = "C:/My Directory/BRFSS" )
#' lodown( "bsapuf" , output_dir = "C:/My Directory/BSAPUF" )
#' lodown( "censo" , output_dir = "C:/My Directory/CENSO" )
#' lodown( "censo_escolar" , output_dir = "C:/My Directory/CENSO_ESCOLAR" )
#' lodown( "ces" , output_dir = "C:/My Directory/CES" )
#' lodown( "ess" , output_dir = "C:/My Directory/ESS" , your_email = "email@address.com" )
#' lodown( "faers" , output_dir = "C:/My Directory/FAERS" )
#' lodown( "hmda" , output_dir = "C:/My Directory/HMDA" )
#' lodown( "hrs" , output_dir = "C:/My Directory/HRS" , 
#' 		your_username = "username" , your_password = "password" )
#' lodown( "meps" , output_dir = "C:/My Directory/MEPS" )
#' lodown( "mics" , output_dir = "C:/My Directory/MICS" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "mlces" , output_dir = "C:/My Directory/MLCES" )
#' lodown( "nbs" , output_dir = "C:/My Directory/NBS" )
#' lodown( "ncvs" , output_dir = "C:/My Directory/NCVS" ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "nhis" , output_dir = "C:/My Directory/NHIS" )
#' lodown( "nhts" , output_dir = "C:/My Directory/NHTS" )
#' lodown( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nibrs" , output_dir = "C:/My Directory/NIBRS" ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "nsfg" , output_dir = "C:/My Directory/NSFG" )
#' lodown( "nppes" , output_dir = "C:/My Directory/NPPES" )
#' lodown( "nps" , output_dir = "C:/My Directory/NPS" )
#' lodown( "nsduh" , output_dir = "C:/My Directory/NSDUH" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "piaac" , output_dir = "C:/My Directory/PIAAC" )
#' lodown( "pisa" , output_dir = "C:/My Directory/PISA" )
#' lodown( "pls" , output_dir = "C:/My Directory/PLS" )
#' lodown( "pme" , output_dir = "C:/My Directory/PME" )
#' lodown( "pns" , output_dir = "C:/My Directory/PNS" )
#' lodown( "pnadc" , output_dir = "C:/My Directory/PNADC" )
#' lodown( "pof" , output_dir = "C:/My Directory/POF" )
#' lodown( "psid" , output_dir = "C:/My Directory/PSID" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "sbo" , output_dir = "C:/My Directory/SBO" )
#' lodown( "scf" , output_dir = "C:/My Directory/SCF" )
#' lodown( "seer" , output_dir = "C:/My Directory/SEER" ,
#'		your_username = "username" , your_password = "password" )
#' lodown( "sipp" , output_dir = "C:/My Directory/SIPP" )
#' lodown( "uspums" , output_dir = "C:/My Directory/USPUMS" )
#' lodown( "yrbss" , output_dir = "C:/My Directory/YRBSS" )
#'
#' # examples to download only the first two records in the catalog
#' acs <- get_catalog( "acs" , output_dir = "C:/My Directory/ACS" )
#' lodown( "acs" , acs[ 1:2 , ] )
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
#' bsapuf_cat <- get_catalog( "bsapuf" , output_dir = "C:/My Directory/BSAPUF" )
#' lodown( "bsapuf" , bsapuf_cat[ 1:2 , ] )
#' censo_cat <- get_catalog( "censo" , output_dir = "C:/My Directory/CENSO" )
#' lodown( "censo" , censo_cat[ 1:2 , ] )
#' censo_escolar_cat <- get_catalog( "censo_escolar" , output_dir = "C:/My Directory/CENSO_ESCOLAR" )
#' lodown( "censo_escolar" , censo_escolar_cat[ 1:2 , ] )
#' ces_cat <- get_catalog( "ces" , output_dir = "C:/My Directory/CES" )
#' lodown( "ces" , ces_cat[ 1:2 , ] )
#' ess_cat <- get_catalog( "ess" , output_dir = "C:/My Directory/ESS" )
#' lodown( "ess" , ess_cat[ 1:2 , ] , your_email = "email@address.com" )
#' faers_cat <- get_catalog( "faers" , output_dir = "C:/My Directory/FAERS" )
#' lodown( "faers" , faers_cat[ 1:2 , ] )
#' hmda_cat <- get_catalog( "hmda" , output_dir = "C:/My Directory/HMDA" )
#' lodown( "hmda" , hmda_cat[ 1:2 , ] )
#' hrs_cat <- get_catalog( "hrs" , output_dir = "C:/My Directory/HRS" , 
#' 		your_username = "username" , your_password = "password" )
#' lodown( "hrs" , hrs_cat[ 1:2 , ] , 
#' 		your_username = "username" , your_password = "password" )
#' meps_cat <- get_catalog( "meps" , output_dir = "C:/My Directory/MEPS" )
#' lodown( "meps" , meps_cat[ 1:2 , ] )
#' mics_cat <- get_catalog( "mics" , output_dir = "C:/My Directory/MICS" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "mics" , mics_cat[ 1:2 , ] , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' mlces_cat <- get_catalog( "mlces" , output_dir = "C:/My Directory/MLCES" )
#' lodown( "mlces" , mlces_cat[ 1:2 , ] )
#' nbs_cat <- get_catalog( "nbs" , output_dir = "C:/My Directory/NBS" )
#' lodown( "nbs" , nbs_cat[ 1:2 , ] )
#' ncvs_cat <- get_catalog( "ncvs" , output_dir = "C:/My Directory/NCVS" )
#' lodown( "ncvs" , ncvs_cat[ 1:2 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' nhis_cat <- get_catalog( "nhis" , output_dir = "C:/My Directory/NHIS" )
#' lodown( "nhis" , nhis_cat[ 1:2 , ] )
#' nhts_cat <- get_catalog( "nhts" , output_dir = "C:/My Directory/NHTS" )
#' lodown( "nhts" , nhts_cat[ 1:2 , ] )
#' nibrs_cat <- get_catalog( "nibrs" , output_dir = "C:/My Directory/NIBRS" )
#' lodown( "nibrs" , nibrs_cat[ 1:2 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' nis_cat <- get_catalog( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nis" , nis_cat[ 1:2 , ] )
#' nppes_cat <- get_catalog( "nppes" , output_dir = "C:/My Directory/NPPES" )
#' lodown( "nppes" , nppes_cat )
#' nps_cat <- get_catalog( "nps" , output_dir = "C:/My Directory/NPS" )
#' lodown( "nps" , nps_cat )
#' nsch_cat <- get_catalog( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "nsch" , nsch_cat[ 1:2 , ] )
#' nsfg_cat <- get_catalog( "nsfg" , output_dir = "C:/My Directory/NSFG" )
#' lodown( "nsfg" , nsfg_cat[ 1:2 , ] )
#' nsduh_cat <- get_catalog( "nsduh" , output_dir = "C:/My Directory/NSDUH" )
#' lodown( "nsduh" , nsduh_cat[ 1:2 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' piaac_cat <- get_catalog( "piaac" , output_dir = "C:/My Directory/PIAAC" )
#' lodown( "piaac" , piaac_cat[ 1:2 , ] )
#' pisa_cat <- get_catalog( "pisa" , output_dir = "C:/My Directory/PISA" )
#' lodown( "pisa" , pisa_cat[ 1:2 , ] )
#' pls_cat <- get_catalog( "pls" , output_dir = "C:/My Directory/PLS" )
#' lodown( "pls" , pls_cat[ 1:2 , ] )
#' pme_cat <- get_catalog( "pme" , output_dir = "C:/My Directory/PME" )
#' lodown( "pme" , pme_cat[ 1:2 , ] )
#' pns_cat <- get_catalog( "pns" , output_dir = "C:/My Directory/PNS" )
#' lodown( "pns" , pns_cat[ 1:2 , ] )
#' pnadc_cat <- get_catalog( "pnadc" , output_dir = "C:/My Directory/PNADC" )
#' lodown( "pnadc" , pnadc_cat[ 1:2 , ] )
#' pof_cat <- get_catalog( "pof" , output_dir = "C:/My Directory/POF" )
#' lodown( "pof" , pof_cat[ 1:2 , ] )
#' psid_cat <- get_catalog( "psid" , output_dir = "C:/My Directory/PSID" )
#' lodown( "psid" , psid_cat[ 1:2 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' sbo_cat <- get_catalog( "sbo" , output_dir = "C:/My Directory/SBO" )
#' lodown( "sbo" , sbo_cat[ 1:2 , ] )
#' scf_cat <- get_catalog( "scf" , output_dir = "C:/My Directory/SCF" )
#' lodown( "scf" , scf_cat[ 1:2 , ] )
#' seer_cat <- get_catalog( "seer" , output_dir = "C:/My Directory/SEER" )
#' lodown( "seer" , seer_cat[ 1:2 , ] ,
#'		your_username = "username" , your_password = "password" )
#' sipp_cat <- get_catalog( "sipp" , output_dir = "C:/My Directory/SIPP" )
#' lodown( "sipp" , sipp_cat[ 1:2 , ] )
#' uspums_cat <- get_catalog( "uspums" , output_dir = "C:/My Directory/USPUMS" )
#' lodown( "uspums" , uspums_cat[ 1:2 , ] )
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

		unique_directories <- unique( c( catalog$unzip_folder , if( 'output_filename' %in% names( catalog ) ) dirname( catalog$output_filename ) , catalog$dbfolder , catalog$output_folder ) )

		for ( this_dir in unique_directories ) if( !file.exists( this_dir ) ) dir.create( this_dir , recursive = TRUE , showWarnings = FALSE )

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
