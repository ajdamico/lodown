get_catalog_ncvs <-
	function( data_name = "ncvs" , output_dir , ... ){

	catalog <- get_catalog_icpsr( "95" )
	
	catalog$unzip_folder <- paste0( output_dir , "/" , gsub( "\\[|\\]" , "" , gsub( "[^0-9A-z ]" , "" , catalog$name ) , "/" , catalog$dataset_name ) )

	catalog$db_tablename <- tolower( gsub( " |-" , "_" , paste0( gsub( "([^0-9A-z ])" , "" , paste( catalog$name , catalog$dataset_name ) ) ) ) )
	catalog$db_tablename <- gsub( "national_crime_victimization_survey_" , "x" , catalog$db_tablename )
	catalog$db_tablename <- gsub( "\\[|\\]" , "" , catalog$db_tablename )
	catalog$db_tablename <- gsub( "concatenated" , "concat" , catalog$db_tablename )
	catalog$db_tablename <- gsub( "recordtype_file" , "lvl" , catalog$db_tablename )
	catalog$db_tablename <- gsub( "segment" , "seg" , catalog$db_tablename )
	
	catalog$unzip_folder <- gsub( "National Crime Victimization Survey( )?" , "" , catalog$unzip_folder )

	catalog$dbfolder <- paste0( output_dir , "/MonetDB" )

	catalog

}


lodown_ncvs <-
	function( data_name = "ncvs" , catalog , ... ){

		lodown_icpsr( data_name = data_name , catalog , ... )

		invisible( TRUE )

	}

