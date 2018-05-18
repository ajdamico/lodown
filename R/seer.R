get_catalog_seer <-
	function( data_name = "seer" , output_dir , ... ){
	
		# find the seerstat page containing the link to the latest zipped file
		ssp <- readLines( "https://seer.cancer.gov/data/options.html" , warn = FALSE )

		# find the latest filepath
		fp <- gsub( '(.*)\"https://(.*)\\.(zip|ZIP)\"(.*)' , "\\2.\\3" , grep( "\\.(zip|ZIP)" , ssp , value = TRUE ) )

		# there can be only one
		stopifnot( length( fp ) == 1 )

		catalog <-
			data.frame(
				output_folder = paste0( output_dir , "/" ) ,
				at_url = fp ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_seer <-
	function( data_name = "seer" , catalog , ... ){

		on.exit( print( catalog ) )

		if( nrow( catalog ) != 1 ) stop( "seer catalog must be exactly one record" )
	
		tf <- tempfile()
		
		if( !( 'your_username' %in% names(list(...)) ) ) stop( "`your_username` parameter must be specified.  create an account at https://seer.cancer.gov/seertrack/data/request/" )
		
		your_username <- list(...)[["your_username"]]

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at https://seer.cancer.gov/seertrack/data/request/" )
		
		your_password <- list(...)[["your_password"]]

		cachaca( paste0( "https://" , your_username , ":" , your_password , "@" , catalog$at_url ) , tf , mode = 'wb' )

		unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )



		# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
		# identify all files to import into r data files (.rds) #

		# create a character vector matching the different cancer file name identifiers
		words.to.match <- c( "BREAST" , "COLRECT" , "DIGOTHR" , "FEMGEN" , "LYMYLEUK" , "MALEGEN" , "RESPIR" , "URINARY" , "OTHER" )

		# subset the `unzipped_files` character vector to only retain files containing *any* of the words in the `words.to.match` vector
		( ind_file_matches <- unzipped_files[ grep( paste0( words.to.match , collapse = "|" ) , unzipped_files ) ] )
		# by encasing the above statement in parentheses, the `ind_file_matches` object will also be printed to the screen

		# subset the `unzipped_files` character vector to only retain files containing *either* the string '19agegroups' or the string 'singleages'
		( pop_file_matches <- unzipped_files[ grep( "19agegroups|singleages" , unzipped_files ) ] )
		# by encasing the above statement in parentheses, the `pop_file_matches` object will also be printed to the screen


		# end of file identification  #
		# # # # # # # # # # # # # # # #


		# # # # # # # # # # # # # # # # # # #
		# import all individual-level files #

		# create a temporary file on the local disk
		edited.sas.instructions <- tempfile()

		# read the sas importation script into memory
		z <- readLines( grep( "\\.sas$" , unzipped_files , value = TRUE ) )

		# get rid of the first through fourth lines (the -1:-4 part)
		# and at the same time get rid of the word `char` (the gsub part)
		z <- gsub( "char" , "" , z[-1:-4] )
		# since SAScii cannot handle char# formats

		# remove the leading space in front of the at signs,
		z <- gsub( "@ " , "@" , z , fixed = TRUE )
		# since SAScii does not expect that either

		# write the result back to a temporary file on the local disk
		writeLines( z , edited.sas.instructions )


		# loop through each of the individual-level files matched above
		for ( fp in ind_file_matches ){
			
			# use the revised sas importation instructions
			# to read the current ascii file directly into an r data.frame
			x <- read_SAScii( fp , edited.sas.instructions )
			# this simply reads the text file into the object `x`
			
			
			# calculate the save-file-location
			# by removing the downloaded zipped file's folderpath
			# and substituting `txt` with `rds`
			# and converting the file location to lowercase
			sfl <- gsub( "(.*)_TEXTDATA" , normalizePath( catalog$output_folder , winslash = '/' ) , gsub( "\\.txt$" , ".rds" , fp , ignore.case = TRUE ) )
			
			# convert all column names to lowercase
			# in the current data.frame object `x`
			names( x ) <- tolower( names( x ) )
			
			
			# (if it doesn't already exist)
			# create the directory of the save-file-location
			dir.create( np_dirname( sfl ) , showWarnings = FALSE ,	recursive = TRUE )

			catalog$case_count <- max( catalog$case_count , nrow( x ) , na.rm = TRUE )
			
			# save the data.frame to the save-file-location
			saveRDS( x , file = sfl ) ; rm( x ) ; gc()

			cat( paste0( data_name , " individual file " , which( fp == ind_file_matches ) , " of " , length( ind_file_matches ) , " stored at '" , sfl , "'\r\n\n" ) )

		}

		# end of individual-level file importation  #
		# # # # # # # # # # # # # # # # # # # # # # #


		# loop through each of the population-level files matched above
		for ( fp in pop_file_matches ){

			# use that `seer_pop_read_in` function defined above
			# to create a data.frame object `x` that read in the current file
			x <- seer_pop_read_in( fp )
				
			# calculate the save-file-location
			# by removing the downloaded zipped file's folderpath
			# and substituting `txt` with `rds`
			# and converting the file location to lowercase
			sfl <- gsub( "(.*)_TEXTDATA" , normalizePath( catalog$output_folder , winslash = '/' ) , gsub( "\\.txt$" , ".rds" , fp , ignore.case = TRUE ) )
				
			# convert all column names to lowercase
			# in the current data.frame object `x`
			names( x ) <- tolower( names( x ) )
			
			
			# (if it doesn't already exist)
			# create the directory of the save-file-location
			dir.create( np_dirname( sfl ) , showWarnings = FALSE ,	recursive = TRUE )

			catalog$case_count <- max( catalog$case_count , nrow( x ) , na.rm = TRUE )
			
			# save the data.frame to the save-file-location
			saveRDS( x , file = sfl ) ; rm( x ) ; gc()

			cat( paste0( data_name , " population file " , which( fp == pop_file_matches ) , " of " , length( pop_file_matches ) , " stored at '" , sfl , "'\r\n\n" ) )

		}

		# end of population-level file importation  #
		# # # # # # # # # # # # # # # # # # # # # # #


		# delete the temporary files
		suppressWarnings( file.remove( tf , unzipped_files ) )

		on.exit()

		catalog

	}




# # # # # # # # # # # # # # # # # # #
# import all population-level files #

# use the population file's data dictionary, available at
# http://seer.cancer.gov/manuals/Text.Data.popdic.html
# to construct a function that will define the names and widths
# for a readr::read_fwf call.  (for more detail about read_fwf, type ?readr::read_fwf)

# initiate a function #
seer_pop_read_in <-
	# that only requires the filepath
	function( fp ){

		# define the population file's widths
		pop.widths <- c( 4 , 2 , 2 , 3 , 2 , 1 , 1 , 1 , 2 , 10 )

		pop_types <- "ncnnnnnnnn"
		
		# define the population file's column (variable) names
		pop.names <- c( 'year' , 'stateab' , 'statefips' , 'countyfips' , 'registry' , 'race' , 'origin' , 'sex' , 'age' , 'population' )

		# actually read the text data into working memory
		pop <- 
			data.frame( 
				readr::read_fwf( 
					
					fp , 
					
					readr::fwf_widths( pop.widths , col_names = pop.names ) , 
				
					col_types = pop_types , 
					
					na = c( "NA" , "" , "." ) ,
					
					locale = readr::locale( decimal_mark = "." , grouping_mark = "," ) 
				)
			)

		# divide the population column by ten, as specified by the data dictionary
		pop$population <- pop$population / 10
	
		# since this is the last line of the function
		# return the population data.frame
		pop
	}
# end of function initiation
