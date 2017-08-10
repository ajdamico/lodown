get_catalog_enade <-
	function( data_name = "enade" , output_dir , ... ){

		dat_dir <- "ftp://ftp.inep.gov.br/microdados/Enade_Microdados/"

		dat_ftp <- readLines( textConnection( RCurl::getURL( dat_dir ) ) )

		all_files <- gsub( "(.*) (.*)" , "\\2" , dat_ftp )
		
		these_links <- file.path( dat_dir , all_files[ grep( "enade(.*)zip$|enade(.*)rar$" , basename( all_files ) , ignore.case = TRUE ) ] )

		enade_years <- substr( gsub( "[^0-9]" , "" , these_links ) , 1 , 4 )

		catalog <-
			data.frame(
				year = enade_years ,
				full_url = these_links ,
				output_filename = paste0( output_dir , "/" , enade_years , " main.rds" ) ,
				output_folder = paste0( output_dir , "/" , enade_years , "/" ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_enade <-
	function( data_name = "enade" , catalog , ... ){

		tf <- tempfile()
		
		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			archive::archive_extract( tf , dir = normalizePath( catalog[ i , "output_folder" ] ) )

			z <- unique( list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) )

			zf <- grep( "\\.zip|\\.ZIP" , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) , value = TRUE )

			if( length( zf ) > 0 ){

				archive::archive_extract( zf , dir = normalizePath( catalog[ i , "output_folder" ] ) )

				z <- unique( c( z , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) ) )

			}

			rfi <- grep( "\\.rar|\\.RAR" , z , value = TRUE )

			if( length( rfi ) > 0 ) {

				archive::archive_extract( rfi , dir = normalizePath( catalog[ i , "output_folder" ] ) )

				z <- unique( c( z , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) ) )

			}

			csvfile <- grep( "\\.csv|\\.CSV" , z , value = TRUE )

			# remove duplicated basenames
			csvfile <- csvfile[ !duplicated( basename( csvfile ) ) ]
			
			stopifnot( length( csvfile ) == 1 )

			tablename <- tolower( gsub( "\\.(.*)" , "" , basename( csvfile ) ) )

			# are decimals , or . ?
			dots_in_row <- grepl( "\\." , readLines( csvfile , n = 2 )[2] )

			x <- data.frame( readr::read_delim( csvfile , ';' , locale = readr::locale( decimal_mark = if( dots_in_row ) "." else "," ) ) )

			names( x ) <- tolower( names( x ) )
			
			stopifnot( R.utils::countLines( csvfile ) == nrow( x ) + 1 )

			saveRDS( x , catalog[ i , 'output_filename' ] )
			
			catalog[ i , 'case_count' ] <- nrow( x )
		
			# delete the temporary files?  or move some docs to a save folder?
			suppressWarnings( file.remove( tf ) )
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		catalog

	}

