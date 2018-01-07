
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
	function( data_name , repo = "ajdamico/asdfree" , ref = "master" , replacements = NULL , setup_test = NULL , local_comp = FALSE ){

		this_rmd <- grep( paste0( "-" , ifelse( data_name %in% c( 'acs2' , 'acs3' , 'acs4' ) , 'acs' , data_name ) , "\\.Rmd$" ) , list.files( "C:/Users/anthonyd/Documents/GitHub/asdfree/" , full.names = TRUE ) , value = TRUE )
		
		rmd_page <- readLines( this_rmd )
	
		v <- grep( "```" , rmd_page )
		
		lines_to_eval <- unlist( mapply( `:` , v[ seq( 1 , length( v ) - 1 , 2 ) ] + 1 , v[ seq( 2 , length( v ) + 1 , 2 ) ] - 1 ) )
		
		rmd_page <- rmd_page[ lines_to_eval ]
		
		# if there's a `dbdir` line, use it for corruption sniffing
		dbdir_line <- grep( "dbdir" , rmd_page , value = TRUE )
		
		if( !is.null( setup_test ) ){
		
			# find the second `library(lodown)` line
			second_library_lodown_line <- grep( "^library\\(lodown\\)$" , rmd_page )[ 2 ]
			
			# if that line does not exist, simply use the first two lines of code
			if( is.na( second_library_lodown_line ) ){
				second_library_lodown_line <- 3
			}
			
			if( setup_test == "setup" ){
				
				if( data_name == 'acs' ){
				
					rmd_page <-
						"library(lodown)\nacs_cat <- get_catalog( \"acs\" , , output_dir = file.path( path.expand( \"~\" ) , \"ACS\" ) )\nlodown( \"acs\" , subset( acs_cat , ( time_period == '1-Year' & year == 2011 ) | year <= 2008 ) )"
					
				} else if( data_name == 'acs2' ){
				
					rmd_page <-
						"library(lodown)\nacs_cat <- get_catalog( \"acs\" , , output_dir = file.path( path.expand( \"~\" ) , \"ACS\" ) )\nlodown( \"acs\" , subset( acs_cat , year >= 2009 & year <= 2011 ) )"
					
				} else if( data_name == 'acs3' ){
				
					rmd_page <-
						"library(lodown)\nacs_cat <- get_catalog( \"acs\" , , output_dir = file.path( path.expand( \"~\" ) , \"ACS\" ) )\nlodown( \"acs\" , subset( acs_cat , ( time_period == '1-Year' & year == 2011 ) | ( year >= 2012 & year <= 2014 ) ) )"
					
				} else if( data_name == 'acs4' ){
				
					rmd_page <-
						"library(lodown)\nacs_cat <- get_catalog( \"acs\" , , output_dir = file.path( path.expand( \"~\" ) , \"ACS\" ) )\nlodown( \"acs\" , subset( acs_cat , ( time_period == '1-Year' & year == 2011 ) | ( year >= 2015 ) ) )"
					
				} else {
					
					rmd_page <- rmd_page[ seq_along( rmd_page ) < second_library_lodown_line ]
					
				}
			
			} else if( setup_test == "test" ) {
			
				rmd_page <- rmd_page[ seq_along( rmd_page ) >= second_library_lodown_line ]
				
				lodown_command_line <- grep( "^lodown\\(" , rmd_page )
				
				if( length( lodown_command_line ) > 0 ){
				
					rmd_page[ lodown_command_line ] <- paste0( "stopifnot( nrow( " , ifelse( data_name %in% c( 'acs2' , 'acs3' , 'acs4' ) , 'acs' , data_name ) , "_cat ) > 0 )" )
					
					# following two lines might include usernames/passwords
					if( grepl( "your_" , rmd_page[ lodown_command_line + 1 ] ) ) rmd_page[ lodown_command_line + 1 ] <- ""
					if( grepl( "your_" , rmd_page[ lodown_command_line + 2 ] ) ) rmd_page[ lodown_command_line + 2 ] <- ""
					if( grepl( "your_" , rmd_page[ lodown_command_line + 3 ] ) ) rmd_page[ lodown_command_line + 3 ] <- ""
					
				}
			
			} else stop( "setup_test= must be 'setup' or 'test'" )
		
		}
		
		if( length( dbdir_line ) > 0 ){
				
			cs_query <- "select tables.name, columns.name, location from tables inner join columns on tables.id=columns.table_id left join storage on tables.name=storage.table and columns.name=storage.column where location is null and tables.name not in ('tables', 'columns', 'users', 'querylog_catalog', 'querylog_calls', 'querylog_history', 'tracelog', 'sessions', 'optimizers', 'environment', 'queue', 'rejects', 'storage', 'storagemodel', 'tablestoragemodel')"
			
			cs_lines <-
				paste(
					dbdir_line[1] ,
					'\nwarnings()\nlibrary(DBI)\ndb <- dbConnect( MonetDBLite::MonetDBLite() , dbdir )\ncs <- dbGetQuery( db , "' , cs_query , '" )\nprint(cs)\nstopifnot(nrow(cs) == 0)\ndbDisconnect( db , shutdown = TRUE )'
				)
				
			rmd_page <- c( rmd_page , cs_lines )
				
		
		}
		
		temp_script <- tempfile()

		for ( this_replacement in replacements ) rmd_page <- gsub( this_replacement[ 1 ] , this_replacement[ 2 ] , rmd_page , fixed = TRUE )

		writeLines( rmd_page , temp_script )

		temp_script
	}


readLines_retry <-
	function( ... , attempts = 10 , sleep_length = 60 * sample( 1:5 , 1 ) ){
	
		for( i in seq( attempts ) ){
		
			this_warning <- tryCatch( { result <- readLines( ... ) ; return( result ) } , warning = print )
			
			if( grepl( "404" , paste( as.character( this_warning ) , collapse = " " ) ) ) stop( as.character( this_warning ) ) 
			
			Sys.sleep( sleep_length )
		
		}
		
		stop( this_warning )
		
	}
	
