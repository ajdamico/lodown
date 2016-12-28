get_catalog_addhealth <-
	function( data_name = "addhealth" , output_dir , ... ){

	catalog <- get_catalog_icpsr( study_numbers = "21600" )
	
	catalog$wave <- stringr::str_trim( gsub( "[[:punct:]]" , "" , sapply( strsplit( catalog$dataset_name , ":" ) , "[[" , 1 ) ) )
	
	catalog$data_title <- stringr::str_trim( gsub( "[[:punct:]]" , "" , sapply( strsplit( catalog$dataset_name , ":" ) , "[[" , 2 ) ) )
	
	catalog$output_filename <- paste0( output_dir , "/" , catalog$wave , "/" , catalog$data_title , "/main.rda" )

	catalog

}


lodown_addhealth <-
	function( catalog , data_name = "addhealth" , ... ){
	
		lodown_icpsr( catalog , data_name = data_name , ... )

		invisible( TRUE )

	}

