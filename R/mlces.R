get_catalog_mlces <-
	function( data_name = "mlces" , output_dir , ... ){

		catalog <-
			data.frame(
				full_url = paste0( "https://www.soa.org/Files/Research/" , 1997:1999 , ".zip" ) ,
				output_filename = paste0( output_dir , "/mlces" , 1997:1999 , ".rds" ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_mlces <-
	function( data_name = "mlces" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			# read the current file into RAM
			x <- read.csv( unzipped_files )

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			catalog[ i , 'case_count' ] <- nrow( x )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

