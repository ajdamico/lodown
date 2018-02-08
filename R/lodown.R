#' @importFrom utils download.file read.csv unzip getFromNamespace write.csv read.table read.fwf
#' @importFrom stats as.formula vcov coef pf update confint qbeta qt var
#' @importFrom graphics plot rasterImage
NULL

#' locally download and prepare publicly-available microdata
#'
#' \code{lodown} actively downloads the extracts specified by the catalog.  \code{get_catalog} retrieves a listing of all available extracts for a microdata set.  skipping \code{get_catalog} will attempt to download all available extracts
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param catalog \code{data.frame} detailing available microdata extracts and metadata
#' @param output_dir directory on your local computer to save the microdata
#' @param ... passed to \code{get_catalog_*} and \code{lodown_*}
#'
#' @return catalog \code{data.frame} detailing available microdata extracts and metadata, along with local file paths and possibly additional metadata acquired during the download
#'
#' @seealso \url{http://www.asdfree.com} for usage examples
#'
#' @rdname lodown
#' @export
get_catalog <-
	function( data_name , output_dir = getwd() , ... ){

		cat_fun <- getFromNamespace( paste0( "get_catalog_" , data_name ) , "lodown" )

		cat( paste0( "building catalog for " , data_name , "\r\n\n" ) )

		cat_fun( data_name = data_name , output_dir = output_dir , ... )

	}

#' @rdname lodown
#' @export
lodown <-
	function( data_name , catalog = NULL , ... ){

		if( is.null( catalog ) ) catalog <- get_catalog( data_name , ... )

		unique_directories <- unique( c( catalog$unzip_folder , if( 'output_filename' %in% names( catalog ) ) np_dirname( catalog$output_filename ) , if( 'dbfile' %in% names( catalog ) ) np_dirname( catalog$dbfile ) , catalog$output_folder ) )

		for ( this_dir in unique_directories ){
			if( !dir.exists( this_dir ) ){
				tryCatch( { 
					dir.create( this_dir , recursive = TRUE , showWarnings = TRUE ) 
					} , 
					warning = function( w ) stop( "while creating directory " , this_dir , "\n" , conditionMessage( w ) ) 
				)
			}
		}

		catalog$case_count <- NA
		
		load_fun <- getFromNamespace( paste0( "lodown_" , data_name ) , "lodown" )

		cat( paste0( "locally downloading " , data_name , "\r\n\n" ) )

		memory_note <- "\r\n\nlodown is now exiting due to a memory error.\nwindows users: your computing performance would suffer due to disk paging,\nbut you can increase your memory limits with beyond your available hardware with the `?memory.limit` function.\nfor example, you can set the memory ceiling of an R session to 256 GB by typing `memory.limit(256000)`.\r\n\n"
		
		installation_note <- "\r\n\nlodown is now exiting due to an installation error.\r\n\n"
		
		parameter_note <- "\r\n\nlodown is now exiting due to a parameter omission.\r\n\n"
		
		unknown_error_note <- "\r\n\nlodown is now exiting unexpectedly.\nwebsites that host publicly-downloadable microdata change often and sometimes those changes cause this software to break.\nif the error call stack below appears to be a hiccup in your internet connection, then please verify your connectivity and retry the download.\notherwise, please open a new issue at `https://github.com/ajdamico/asdfree/issues` with the contents of this error call stack and also the output of your `sessionInfo()`.\r\n\n"
		
		withCallingHandlers(
			catalog <- load_fun( data_name = data_name , catalog , ... ) , 
			error = 
				function( e ){ 
			
					print( sessionInfo() )
			
					if( grepl( 'cannot allocate vector of size' , e ) ) message( memory_note ) else 
					if( grepl( 'parameter must be specified' , e ) ) message( parameter_note ) else
					if( grepl( 'to install' , e ) ) message( installation_note ) else {
					
						message( unknown_error_note )
					
						print( sys.calls() )
						
					}
					
				}
		)

		cat( paste0( data_name , " local download completed\r\n\n" ) )

		invisible( catalog )

	}
	
no.na <- function( x , value = FALSE ){ x[ is.na( x ) ] <- value ; x }

unzip_warn_fail <- function( ... ) tryCatch( { unzip( ... ) } , warning = function( w ) stop( conditionMessage( w ) ) )

unarchive_nicely <- 
	function( file_to_unzip , unzip_directory = tempdir() ) {
		file.remove( list.files( file.path( unzip_directory , "archive_unzip" ) , recursive = TRUE , full.names = TRUE ) )
		archive::archive_extract( file_to_unzip , dir = file.path( unzip_directory , "archive_unzip" ) )
		list.files( file.path( unzip_directory , "archive_unzip" ) , recursive = TRUE , full.names = TRUE )
	}

np_dirname <- function( ... ) normalizePath( dirname( ... ) , mustWork = FALSE )
