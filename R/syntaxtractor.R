
#' rmarkdown syntax extractor for sisyphean perpetual testing
#'
#' parses asdfree textbook for runnable code.  probably not useful for anything else.
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param repo github repository containing textbook
#' @param ref github branch containing textbook
#' @param replacements list containing text to swap out and in, using regular expressions
#' @param setup_test either "setup" for dataname-setup.R or "test" for dataname-test.R or NULL for everything
#'
#' @return filepath with runnable code
#'
#' @examples
#'
#' \dontrun{
#'
#' replacements_list <- list( c( "C:/My Directory" , tempdir() ) )
#' runnable_code <- syntaxtractor( "yrbss" , replacements = replacements_list )
#' source( runnable_code , echo = TRUE )
#'
#' 
#' # usage examples
#' source( syntaxtractor( "prerequisites" ) , echo = TRUE )
#' source( syntaxtractor( "ahrf" , replacements = NULL ) , echo = TRUE )
#' source( syntaxtractor( "nppes" , replacements = NULL ) , echo = TRUE )
#' source( syntaxtractor( "pisa" , replacements = NULL ) , echo = TRUE )
#' source( syntaxtractor( "pnad" , replacements = NULL ) , echo = TRUE )
#' source( syntaxtractor( "scf" , replacements = NULL ) , echo = TRUE )
#' source( syntaxtractor( "yrbss" , replacements = NULL ) , echo = TRUE )
#'
#' some_info <- list( "email@address\\.com" , "ajdamico@gmail.com" )
#' source( syntaxtractor( "lavaan" , replacements = some_info ) , echo = TRUE )
#'
#' }
#'
#' @export
syntaxtractor <-
	function( data_name , repo = "ajdamico/asdfreebook" , ref = "master" , replacements = NULL , setup_test = NULL ){

		repo_homepage <- readLines_retry( paste0( "https://github.com/" , repo , "/tree/" , ref , "/" ) )
		
		rmd_links <- gsub( "(.*)>(.*)\\.Rmd</a>(.*)" , "\\2" , grep( "Rmd" , repo_homepage , value = TRUE ) )
		
		this_rmd <- grep( data_name , rmd_links , value = TRUE )
	
		rmd_page <- readLines_retry( paste0( "https://raw.githubusercontent.com/" , repo , "/" , ref , "/" , this_rmd , ".Rmd" ) )

		v <- grep( "```" , rmd_page )
		
		lines_to_eval <- unlist( mapply( `:` , v[ seq( 1 , length( v ) - 1 , 2 ) ] + 1 , v[ seq( 2 , length( v ) + 1 , 2 ) ] - 1 ) )

		for ( this_replacement in replacements ) rmd_page[ lines_to_eval ] <- gsub( this_replacement[ 1 ] , this_replacement[ 2 ] , rmd_page[ lines_to_eval ] , fixed = TRUE )
		
		rmd_page <- rmd_page[ lines_to_eval ]
		
		if( !is.null( setup_test ) ){
		
			# find the second `library(lodown)` line
			second_library_lodown_line <- grep( "^library\\(lodown\\)$" , rmd_page )[ 2 ]
			
			# if that line does not exist, simply use the first two lines of code
			if( is.na( second_library_lodown_line ) ){
				second_library_lodown_line <- 3
			}
			
			if( setup_test == "setup" ){
				
				rmd_page <- rmd_page[ seq_along( rmd_page ) < second_library_lodown_line ]
			
			} else if( setup_test == "test" ) {
			
				rmd_page <- rmd_page[ seq_along( rmd_page ) >= second_library_lodown_line ]
				
				lodown_command_line <- grep( "^lodown\\(" , rmd_page )
				
				rmd_page[ lodown_command_line ] <- paste0( "stopifnot( nrow( " , data_name , "_cat ) > 0 )" )
			
			} else stop( "setup_test= must be 'setup' or 'test'" )
		
		}
		
		temp_script <- tempfile()

		writeLines( rmd_page , temp_script )

		temp_script
	}


readLines_retry <-
	function( ... , attempts = 10 , sleep_length = 60 * sample( 1:5 , 1 ) ){
	
		for( i in seq( attempts ) ){
		
			this_warning <- tryCatch( result <- readLines( ... ) , warning = print ) )
			
			if( grepl( "404" , as.character( this_warning ) ) ) stop( as.character( this_warning ) ) 
			
			if( length( result ) > 0 ) return( result ) else Sys.sleep( sleep_length )
		
		}
		
		stop( this_warning )
		
	}
	