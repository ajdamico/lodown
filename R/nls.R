get_catalog_nls <-
	function( data_name = "nls" , output_dir , ... ){

		if ( !requireNamespace( "archive" , quietly = TRUE ) ) stop( "archive needed for this function to work. to install it, type `devtools::install_github( 'jimhester/archive' )`" , call. = FALSE )

		catalog <- NULL
		
		data_page <- "https://www.nlsinfo.org/accessing-data-cohorts"
	
		link_page <- rvest::html_nodes( xml2::read_html( data_page ) , "a" )
		
		link_text <- rvest::html_text( link_page )
		
		link_refs <- rvest::html_attr( link_page , "href" )
		
		microdata_text <- stringr::str_trim( link_text[ grep( "\\.zip$" , link_refs ) ] )
		
		microdata_refs <- stringr::str_trim( link_refs[ grep( "\\.zip$" , link_refs ) ] )
		
		catalog <-
			data.frame(
					study_name = microdata_text ,
					full_url = microdata_refs ,
					output_folder = paste0( output_dir , "/" , microdata_text , "/" ) ,
					stringsAsFactors = FALSE
				)
				
		catalog

	}


lodown_nls <-
	function( data_name = "nls" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the strata and psu for studies where they're available
			if( catalog[ i , 'study_name' ] == "NLS Youth 1997 (NLSY97)" ){
			
				# download the nlsy 1997 cohort's sampling information
				cachaca( "https://www.nlsinfo.org/sites/nlsinfo.org/files/attachments/140618/nlsy97stratumpsu.zip" , tf , mode = 'wb' )
				
				# unzip to the local disk
				unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , '/unzips' ) )

				strpsu <- read.csv( unzipped_files[ grep( '\\.csv' , unzipped_files ) ] )
				
				# store the complex sample variables on the local disk
				saveRDS( strpsu , file = paste0( catalog[ i , 'output_folder' ] , "/strpsu.rds" ) , compress = FALSE )
				
				# delete the temporary files
				suppressWarnings( file.remove( tf , unzipped_files ) )
				
			}

			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )
			
			unzip( tf , exdir = file.path( catalog[ i , 'output_folder' ] , 'unzips' ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

