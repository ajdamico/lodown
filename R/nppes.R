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
			output_filename = paste0( output_dir , "/nppes.csv" ) ,
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

		csv.file <- unzipped_files[ grepl( 'csv' , unzipped_files ) & !grepl( 'FileHeader' , unzipped_files ) ]

		file.copy( csv.file , catalog$output_filename )
		
		catalog$case_count <- R.utils::countLines( csv.file ) - 1

		file.remove( unzipped_files , tf )
		
		on.exit()
		
		catalog

	}

