get_catalog_share <-
	function( data_name = "share" , output_dir , ... ){

		if( !( 'your_username' %in% names(list(...)) ) ) stop( "`your_username` parameter must be specified.  create an account at http://www.share-project.org/t3/share/fileadmin/pdf_documentation/SHARE_Data_Statement.pdf" )
		
		your_username <- list(...)[["your_username"]]

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at http://www.share-project.org/t3/share/fileadmin/pdf_documentation/SHARE_Data_Statement.pdf" )
		
		your_password <- list(...)[["your_password"]]

		share_authenticate( your_username , your_password )

		page_list <- httr::GET( "https://share-project.centerdata.nl/sharedatadissemination/" )

		dl_page <-  xml2::read_html( page_list )

		full_table <- rvest::html_table( dl_page , fill = TRUE )[[1]]

		ra_link_refs <- rvest::html_attr( rvest::html_nodes( dl_page , "a" ) , "href" )
			
		ra_link_text <- rvest::html_text( rvest::html_nodes( dl_page , "a" ) )

		stata_link_refs <- ra_link_refs[ ra_link_text == "Download  data for Stata" ]

		stata_names <- gsub( "\t(.*)" , "" , full_table[ , 1 ] )

		catalog <-
			data.frame(
				output_folder = paste0( output_dir , "/" , gsub( ":" , "" , stata_names ) ) ,
				full_url = paste0( "https://share-project.centerdata.nl" , stata_link_refs ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_share <-
	function( data_name = "share" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		if( !( 'your_username' %in% names(list(...)) ) ) stop( "`your_username` parameter must be specified.  create an account at http://www.share-project.org/t3/share/fileadmin/pdf_documentation/SHARE_Data_Statement.pdf" )
		
		your_username <- list(...)[["your_username"]]

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at http://www.share-project.org/t3/share/fileadmin/pdf_documentation/SHARE_Data_Statement.pdf" )
		
		your_password <- list(...)[["your_password"]]

		share_authenticate( your_username , your_password )

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			this_file <- cachaca( catalog[ i , "full_url" ] , FUN = httr::GET )

			writeBin( httr::content( this_file , "raw" ) , tf )
			
			unzipped_files <- unzip_warn_fail( tf , exdir = catalog[ i , 'output_folder' ] )

			for( this_stata in grep( "\\.dta$" , unzipped_files , ignore.case = TRUE , value = TRUE ) ){

				x <- data.frame( haven::read_dta( this_stata ) )

				names( x ) <- tolower( names( x ) )
				
				catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )

				saveRDS( x , file = gsub( "\\.dta$" , ".rds" , this_stata ) , compress = FALSE )
			
			}
				
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()

		catalog

	}


share_authenticate <-
	function( your_username , your_password ){
		
		values <- list( "data[User][id]" = your_username , "data[User][password]" = your_password )
		
		httr::GET( "https://share-project.centerdata.nl/sharedatadissemination/users/login" , query = values )
		httr::POST( "https://share-project.centerdata.nl/sharedatadissemination/users/login" , body = values )
	
		page_list <- httr::GET( "https://share-project.centerdata.nl/sharedatadissemination/" )

		dl_page <-  xml2::read_html( page_list )

		ra_link_refs <- rvest::html_attr( rvest::html_nodes( dl_page , "a" ) , "href" )

		if( !any( grepl( "logout" , ra_link_refs ) ) ) stop( "you are not logged in" ) else invisible( TRUE )
	
	}
	