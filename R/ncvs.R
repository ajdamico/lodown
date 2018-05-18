get_catalog_ncvs <-
	function( data_name = "ncvs" , output_dir , ... ){

	catalog <- get_catalog_icpsr( "95" , archive = "NACJD" , bundle_preference = c( "rdata" , "stata" , "ascsas" ) )
	
	catalog$unzip_folder <- paste0( output_dir , "/" , gsub( "\\[|\\]" , "" , gsub( "[^0-9A-z ]" , "" , gsub( "-" , " " , catalog$name ) ) , "/" , catalog$dataset_name , "/" ) )

	catalog$db_tablename <- tolower( gsub( " |-" , "_" , paste0( gsub( "([^0-9A-z ])" , "" , paste( catalog$name , catalog$dataset_name ) ) ) ) )
	catalog$db_tablename <- gsub( "national_crime_victimization_survey_" , "x" , catalog$db_tablename )
	catalog$db_tablename <- gsub( "\\[|\\]" , "" , catalog$db_tablename )
	catalog$db_tablename <- gsub( "concatenated" , "concat" , catalog$db_tablename )
	catalog$db_tablename <- gsub( "recordtype_file" , "lvl" , catalog$db_tablename )
	catalog$db_tablename <- gsub( "segment" , "seg" , catalog$db_tablename )
	
	catalog$unzip_folder <- gsub( "National Crime Victimization Survey( )?" , "" , catalog$unzip_folder )

	catalog$dbfile <- paste0( output_dir , "/SQLite.db" )

	catalog

}


lodown_ncvs <-
	function( data_name = "ncvs" , catalog , ... ){

		on.exit( print( catalog ) )

		lodown_icpsr( data_name = data_name , catalog , ... )
		
		for( i in seq_len( nrow( catalog ) ) ){
		
			if( grepl( "bundle=stata" , catalog[ i , 'full_url' ] ) ){
		
				# find stata file within unzipped path
				stata_files <- grep( "\\.dta$" , list.files( catalog[ i , 'unzip_folder' ] , full.names = TRUE ) , value = TRUE )
				
				stopifnot( length( stata_files ) == 1 )
				
				x <- icpsr_stata( x , catalog_entry = catalog[ i , ] )

			}
			
			if( grepl( "bundle=ascsas" , catalog[ i , 'full_url' ] ) ){
		
				# find stata file within unzipped path
				txt <- grep( "\\.txt$" , list.files( catalog[ i , 'unzip_folder' ] , full.names = TRUE ) , value = TRUE )
				
				sas <- grep( "\\.sas$" , list.files( catalog[ i , 'unzip_folder' ] , full.names = TRUE ) , value = TRUE )
				
				stopifnot( length( stata_files ) == 1 )
				
				x <- read_SAScii( txt , sas )

			}
			
			if( !grepl( "bundle=rdata" , catalog[ i , 'full_url' ] ) ){
				
				names( x ) <- tolower( names( x ) )
				
				saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )
				
				catalog[ i , 'case_count' ] <- nrow( x )
				
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

			}
			
		}

		on.exit()
		
		catalog

	}

