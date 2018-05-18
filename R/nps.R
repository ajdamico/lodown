get_catalog_nps <-
	function( data_name = "nps" , output_dir , ... ){

		tf <- tempfile()
	
		# set the agidnet page containing all of the available microdata files
		main.file.page <- "https://agid.acl.gov/DataFiles/NPS/"

		# download the contents of that page to an object
		z <- httr::GET( main.file.page )

		# store the contents of that object into a temporary file
		writeBin( z$content , tf )

		# read that temporary file's contents back into RAM
		html <- readLines( tf )
		# kinda circuitous, eh?

		# limit that `html` character vector
		# to only lines containing the text `serviceid`
		serviceid.lines <- html[ grep( 'serviceid' , html ) ]

		# get rid of all text in each line in front of `href="`
		after.href <- sapply( strsplit( serviceid.lines , 'href=\"' ) , '[[' , 2 )

		# and also get rid of all text in each line after `">`
		before.bracket <- sapply( strsplit( after.href , "\">" ) , '[[' , 1 )

		# suddenly, you've got yourself a character vector containing
		# the paths to all of the available microdata files.  yippie!
		file_pages <- paste0( main.file.page , before.bracket )

		service_ids <- as.numeric( gsub( "(.*)(serviceid=)(.)" , "\\3" , file_pages ) )
		
		if ( any( !( service_ids %in% 1:9 ) ) ) stop( 'unexpected serviceid' )
		
		years <- as.numeric( gsub( "(.*)(year=)([0-9]+)(&amp)(.*)" , "\\3" , file_pages ) )
		
		service_names <- c( 'caregiver' , 'collected caregiver' , "family caregiver" , 'home delivered meals' , 'congregate meals' , 'homemaker' , "info and assistance" , 'transportation' , 'case management' )
		
		folder_prefix <- c( "Caregiver" , "Collected_Caregiver" , "Family_Caregiver" , "HomeDeliveredMeals" , "CongregateMeals" , "Homemaker" , "InfoAssistance" , "Transportation" , "CaseManagement" ) 
		
		file_prefix <- c( "Caregiver_" , "Collected_Caregiver_" , "Family_Caregiver_" , "Home_Meals_" , "Cong_Meals_" , "Homemaker_" , "InfoAssistance_" , "Transportation_" , "Case_Management_" )
		
		full_url = paste0( "https://agid.acl.gov/DataFiles/Documents/NPS/" , folder_prefix[ service_ids ] , years , "/" , file_prefix[ service_ids ] , years , "_csv.zip" )
		
		catalog <-
			data.frame(
				year = years ,
				full_url = full_url ,
				output_filename = paste0( output_dir , "/" , years , " " , service_names[ service_ids ] , ".rds" ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_nps <-
	function( data_name = "nps" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			# and if the zipped file contains more than one file, this program needs to be updated ;)
			if ( length( unzipped_files ) > 1 ) stop( 'multi-file zipped' )
			
			# load the unzipped csv file into an R data.frame object
			suppressMessages( x <- data.frame( readr::read_csv( unzipped_files , locale = readr::locale( decimal_mark = "." , grouping_mark = "," ) ) ) )
			
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )
			
			# add a column of all ones
			x$one <- 1
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			catalog[ i , 'case_count' ] <- nrow( x )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

