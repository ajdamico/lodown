get_catalog_nsduh <-
	function( data_name = "nsduh" , output_dir , ... ){

	catalog <- get_catalog_icpsr( "00064" , bundle_preference = "stata" )
	
	catalog$output_filename <- paste0( output_dir , "/" , catalog$temporalCoverage , " " , ifelse( catalog$dataset_name %in% c( "Part A" , "Part B" ) , tolower( catalog$dataset_name ) , "main" ) , ".rda" )

	catalog

}


lodown_nsduh <-
	function( data_name = "nsduh" , catalog , ... ){
	
		lodown_icpsr( data_name = data_name , catalog , ... )

		for( i in seq_len( nrow( catalog ) ) ){
		
			# find stata file within unzipped path
			stata_files <- grep( "\\.dta$" , list.files( dirname( catalog[ i , 'output_filename' ] ) , full.names = TRUE ) , value = TRUE )
			
			for( this_stata in stata_files ){
			
				x <- data.frame( haven::read_dta( this_stata ) )
				
				names( x ) <- tolower( names( x ) )
				
				save( x , file = catalog[ i , 'output_filename' ] )
				
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )
		
			}
		
		}
		
		invisible( TRUE )

	}

