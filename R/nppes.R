get_catalog_nppes <-
	function( data_name = "nppes" , output_dir , ... ){
		
	# read in the whole NPI files page
	npi.datapage <- suppressWarnings( readLines( "http://download.cms.gov/nppes/NPI_Files.html" ) )

	# find the first line containing the data dissemination link
	npi.dataline <- npi.datapage[ grep( "NPPES_Data_Dissemination_" , npi.datapage ) ][1]

	# pull out the zipped file's name from that line
	fn <- paste0( "http://download.cms.gov/nppes/" , gsub( "(.*)(NPPES_Data_Dissemination_.*\\.zip)(.*)$" , "\\2" , npi.dataline ) )
	
	catalog <-
		data.frame(
			full_url = fn ,
			db_tablename = "npi" ,
			dbfile = paste0( output_dir , "/SQLite.db" ) ,
			stringsAsFactors = FALSE
		)

	catalog

}


lodown_nppes <-
	function( data_name = "nppes" , catalog , path_to_7za = '7za' , ... ){

		on.exit( print( catalog ) )
	
		if( nrow( catalog ) != 1 ) stop( "nppes catalog must be exactly one record" )
		
		if( ( .Platform$OS.type != 'windows' ) && ( system( paste0('"', path_to_7za , '" -h' ) ) != 0 ) ) stop( "you need to install 7-zip.  if you already have it, include a path_to_7za='/directory/7za' parameter" )
 		
		tf <- tempfile()
		
		cachaca( catalog$full_url , tf , mode = 'wb', filesize_fun = 'unzip_verify' )
		
		# extract the file, platform-specific
		if ( .Platform$OS.type == 'windows' ){

			unzipped_files <- unzip_warn_fail( tf , exdir = tempdir() )

		} else {

			# build the string to send to the terminal on non-windows systems
			dos.command <- paste0( '"' , path_to_7za , '" x ' , tf , ' -o"' , tempdir() , '"' )
			system( dos.command )
			unzipped_files <- list.files( tempdir() , full.names = TRUE )

		}

		# ..and identify the appropriate 
		# comma separated value (csv) file
		# within the `.zip` file
		csv.file <- unzipped_files[ grepl( 'csv' , unzipped_files ) & !grepl( 'FileHeader' , unzipped_files ) ]

		# open the connection to the monetdblite database
		db <- DBI::dbConnect( RSQLite::SQLite() , catalog$dbfile )
		# from now on, the 'db' object will be used for r to connect with the monetdb server

		# read the first thousand records
		# of the csv.file into R
		col.check <- read.csv( csv.file , nrow = 1000 )

		# determine the field names
		fields <- names( col.check )

		# convert the field names to lowercase
		fields <- tolower( fields )

		# remove all `.` characters from field names
		fields <- gsub( "." , "_" , fields , fixed = TRUE )

		# fields containing the word `code`
		# and none of country, state, gender, taxonomy, or postal
		# should be numeric types.
		# all others should be character types.
		colTypes <- 
			ifelse( 
				grepl( "code" , fields ) & !grepl( "country|state|gender|taxonomy|postal" , fields ) , 
				'DOUBLE PRECISION' , 
				'STRING' 
			)
			
		cc <- ifelse( colTypes == 'STRING' , 'character' , 'numeric' )

		# build a sql string..
		colDecl <- paste( fields , colTypes )

		# ..to initiate this table in the monet database
		sql.create <- sprintf( paste( "CREATE TABLE" , catalog$db_tablename , "(%s)" ) , paste( colDecl , collapse = ", " ) )

		# run the actual table creation command
		DBI::dbSendQuery( db , sql.create )


		# create a read-only input connection..
		incon <- file( csv.file , "r" )

		# read in the header_line to skip it in the file connection
		header_line <- readLines( incon , n = 1 )
		
		# loop through every line in the input connection,
		# 50,000 lines at a time
		while( length( z <- read.csv( incon , nrow = 50000 , comment.char = "" , header = FALSE , stringsAsFactors = FALSE , col.names = fields ) ) > 0 ){
		
			# initiate the current table
			DBI::dbWriteTable( 
				db , 
				catalog$db_tablename , 
				z , 
				header = FALSE ,
				append = TRUE
			)

		}

		# shut down the file connection
		close( incon )
		
		
		catalog$case_count <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , catalog$db_tablename ) )
		
		stopifnot( R.utils::countLines( csv.file ) == ( catalog$case_count + 1 ) )
		
		# # # # # # # # #
		# end of import #
		# # # # # # # # #

		file.remove( unzipped_files , tf )
		
		on.exit()
		
		catalog

	}

