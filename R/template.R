# get_catalog_template <-
	# function( data_name = "template" , output_dir , ... ){

		# catalog <-
			# data.frame(
				# directory = ifelse( grepl( "_SN_" , path_to_files ) , "state" , "county" ) ,
				# tech_doc = grepl( "_USER_TECH_" , path_to_files ) ,
				# year = gsub( "(.*)([0-9][0-9][0-9][0-9])-([0-9][0-9][0-9][0-9])(.*)" , "\\2" , basename( path_to_files ) ) ,
				# full_url = path_to_files ,
				# stringsAsFactors = FALSE
			# )

		# catalog

	# }


# lodown_template <-
	# function( data_name = "template" , catalog , ... ){

		# on.exit( print( catalog ) )

		# if ( !requireNamespace( "template" , quietly = TRUE ) ) stop( "template needed for this function to work. to install it, type `install.packages( 'template' )`" , call. = FALSE )

		# tf <- tempfile()


		# for ( i in seq_len( nrow( catalog ) ) ){

			# # download the file
			# cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			# unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )






			# # convert all column names to lowercase
			# names( x ) <- tolower( names( x ) )

			# saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			# if there are multiple files per catalog entry, use `max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )` instead
			# catalog[ i , 'case_count' ] <- nrow( x )
			
			# # delete the temporary files
			# suppressWarnings( file.remove( tf , unzipped_files ) )

			# cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		# }

		# on.exit()
		
		# catalog

	# }

