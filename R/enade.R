get_catalog_enade <-
	function( data_name = "enade" , output_dir , ... ){

		if ( !requireNamespace( "archive" , quietly = TRUE ) ) stop( "archive needed for this function to work. to install it, type `devtools::install_github( 'jimhester/archive' )`" , call. = FALSE )

		inep_portal <- "http://portal.inep.gov.br/microdados"

		w <- rvest::html_attr( rvest::html_nodes( xml2::read_html( inep_portal ) , "a" ) , "href" )
		
		these_links <- w[ grep( "enade(.*)zip$|enade(.*)rar$" , basename( w ) , ignore.case = TRUE ) ]

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

		on.exit( print( catalog ) )

		tf <- tempfile()
		
		for ( i in seq_len( nrow( catalog ) ) ){

			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , attempts = 10 )

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
			
			if( length( csvfile ) == 0 ) csvfile <- grep( "microdados_enade_" , z , value = TRUE , ignore.case = TRUE )
			
			stopifnot( length( csvfile ) == 1 )

			tablename <- tolower( gsub( "\\.(.*)" , "" , basename( csvfile ) ) )

			# are decimals , or . ?
			dots_in_row <- grepl( "\\." , readLines( csvfile , n = 2 )[2] )

			x <- data.frame( readr::read_delim( csvfile , ';' , locale = readr::locale( decimal_mark = if( dots_in_row ) "." else "," ) ) )

			names( x ) <- tolower( names( x ) )
			
			stopifnot( R.utils::countLines( csvfile ) == nrow( x ) + 1 )

			saveRDS( x , catalog[ i , 'output_filename' ] , compress = FALSE )
			
			catalog[ i , 'case_count' ] <- nrow( x )
		
			# delete the temporary files?  or move some docs to a save folder?
			suppressWarnings( file.remove( tf ) )
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

